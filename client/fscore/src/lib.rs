use bytes::BytesMut;
use common::dfs::data_node_service_client::DataNodeServiceClient;
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::read_block_response::ResponseData;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    CreateFileRequest, CreateFileResponse, DeleteFileRequest, GetFileRequest, GetFileResponse,
    ListDirectoryRequest, ListDirectoryResponse, LoginUserRequest, ReadBlockRequest,
    RegisterUserRequest, RenameFileRequest, SetFileStatusRequest, WriteBlockRequest,
    WriteBlockResponse,
};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Request;
use tracing::{debug, error, info, instrument};

// ─── Config ──────────────────────────────────────────────────────────────────

/// Shared settings consumed by all DAFS client binaries (CLI, fuse_daemon, …).
/// Serializable so that downstream crates can build it with figment/toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DafsSettings {
    pub mpsc_upload_channel_size: u8,
    pub mpsc_download_channel_size: u8,
    pub master_endpoints: Vec<String>,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
    pub log_level: String,
    pub upload_chunk_size: usize,
    pub default_user_id: String,
    pub upload_ready_status: String,
    pub default_list_path: String,
    pub default_rename_old_path: String,
    pub default_rename_new_path: String,
    pub access_token_path: String,
}

impl Default for DafsSettings {
    fn default() -> Self {
        DafsSettings {
            mpsc_upload_channel_size: 5,
            mpsc_download_channel_size: 5,
            master_endpoints: vec!["http://[::1]:9090".to_string()],
            upload_concurrency: 2,
            download_concurrency: 10,
            log_level: "info".to_string(),
            upload_chunk_size: 64 * 1024,
            default_user_id: "1".to_string(),
            upload_ready_status: "READY".to_string(),
            default_list_path: "/".to_string(),
            default_rename_old_path: "/".to_string(),
            default_rename_new_path: "/".to_string(),
            access_token_path: "client/.token".to_string(),
        }
    }
}

// ─── Token helpers ────────────────────────────────────────────────────────────

pub fn read_access_token(path: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let token = fs::read_to_string(path).map_err(|_| {
        format!(
            "Cannot read access token from '{path}'. \
             Please log in first using the 'login' command or register with 'register'."
        )
    })?;
    let trimmed = token.trim().to_string();
    if trimmed.is_empty() {
        return Err("Stored access token is empty".into());
    }
    Ok(trimmed)
}

pub fn save_access_token(path: &str, token: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, token)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

/// Register a new user and immediately log in, saving the token to disk.
#[instrument(skip(settings, password))]
pub async fn register_user(
    settings: Arc<DafsSettings>,
    username: String,
    password: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    client
        .register_user(Request::new(RegisterUserRequest {
            username: username.clone(),
            password: password.clone(),
        }))
        .await?;
    login_user(settings, username, password).await?;
    Ok(())
}

/// Authenticate with the master and persist the JWT to `settings.access_token_path`.
#[instrument(skip(settings, password))]
pub async fn login_user(
    settings: Arc<DafsSettings>,
    username: String,
    password: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    let response = client
        .login_user(Request::new(LoginUserRequest { username, password }))
        .await?
        .into_inner();
    save_access_token(&settings.access_token_path, &response.access_token)?;
    info!(
        expires_at = response.expires_at,
        token_path = settings.access_token_path,
        "Access token saved"
    );
    Ok(())
}

// ─── Metadata operations ──────────────────────────────────────────────────────

/// Retrieve metadata + block map for a remote file.
#[instrument(skip(settings, access_token))]
pub async fn get_file_metadata(
    settings: Arc<DafsSettings>,
    remote_path: String,
    access_token: &str,
) -> Result<GetFileResponse, Box<dyn Error + Send + Sync>> {
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    let mut request = Request::new(GetFileRequest {
        path: remote_path,
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;
    Ok(client.get_file_metadata(request).await?.into_inner())
}

/// List the contents of a remote directory.
#[instrument(skip(settings, access_token))]
pub async fn list_directory(
    settings: Arc<DafsSettings>,
    directory: String,
    access_token: &str,
) -> Result<ListDirectoryResponse, Box<dyn Error + Send + Sync>> {
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    let mut request = Request::new(ListDirectoryRequest { path: directory });
    common::attach_bearer_token(&mut request, access_token)?;
    Ok(client.list_directory(request).await?.into_inner())
}

/// Rename or move a remote file.
#[instrument(skip(settings, access_token))]
pub async fn rename_file(
    settings: Arc<DafsSettings>,
    old_path: String,
    new_path: String,
    access_token: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!(old_path, new_path, "Renaming file");
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    let mut request = Request::new(RenameFileRequest {
        old_path: old_path.clone(),
        new_path: new_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;
    client.rename_file(request).await?;
    info!(old_path, new_path, "File rename completed");
    Ok(())
}

/// Delete a remote file.
#[instrument(skip(settings, access_token))]
pub async fn delete_file(
    settings: Arc<DafsSettings>,
    remote_path: String,
    access_token: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!(remote_path, "Deleting file");
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    let mut request = Request::new(DeleteFileRequest {
        path: remote_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;
    client.delete_file(request).await?;
    info!(remote_path, "File deleted");
    Ok(())
}

// ─── Block-level I/O (internal) ───────────────────────────────────────────────

#[instrument(skip(file_handle, data_node_client, settings, access_token))]
async fn upload_block(
    block_id: String,
    data_node_client: DataNodeServiceClient<tonic::transport::Channel>,
    offset: u64,
    block_size: i64,
    file_handle: Arc<std::fs::File>,
    settings: Arc<DafsSettings>,
    access_token: Arc<String>,
) -> Result<WriteBlockResponse, String> {
    debug!(block_id, "Starting block upload");
    let mut hasher = Hasher::new();
    let (tx, rx) = mpsc::channel(settings.mpsc_upload_channel_size as usize);
    let mut client = data_node_client;
    let mut write_request = Request::new(ReceiverStream::new(rx));
    common::attach_bearer_token(&mut write_request, access_token.as_str())
        .map_err(|e| format!("Failed to attach access token: {e}"))?;
    let response_fut = client.write_block(write_request);
    tx.send(WriteBlockRequest {
        request_data: Some(RequestData::Metadata(common::dfs::BlockMetadata {
            block_id: block_id.clone(),
            total_size: block_size,
        })),
    })
    .await
    .map_err(|e| format!("Failed to send block metadata: {e}"))?;

    let mut bytes_remaining = block_size as usize;
    let mut internal_block_offset = offset;
    while bytes_remaining > 0 {
        let bytes_to_read = std::cmp::min(bytes_remaining, settings.upload_chunk_size.max(1));
        let mut buffer = BytesMut::with_capacity(bytes_to_read);
        buffer.resize(bytes_to_read, 0);
        let file = Arc::clone(&file_handle);
        let (bytes_read, buffer) = tokio::task::spawn_blocking(move || {
            file.read_at(&mut buffer[..], internal_block_offset)
                .map(|n| (n, buffer))
        })
        .await
        .map_err(|e| format!("Block read task failed: {e}"))?
        .map_err(|e| format!("Failed to read file at offset {internal_block_offset}: {e}"))?;
        if bytes_read == 0 {
            break;
        }
        let mut final_data = buffer.freeze();
        if bytes_read < bytes_to_read {
            final_data = final_data.slice(0..bytes_read);
        }
        hasher.update(&final_data);
        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::ChunkData(final_data.to_vec())),
        })
        .await
        .map_err(|e| format!("Failed to send chunk data: {e}"))?;
        bytes_remaining -= bytes_read;
        internal_block_offset += bytes_read as u64;
    }

    let checksum = hasher.finalize();
    tx.send(WriteBlockRequest {
        request_data: Some(RequestData::Checksum(checksum.to_string())),
    })
    .await
    .map_err(|e| format!("Failed to send checksum: {e}"))?;
    drop(tx);

    let response = response_fut.await.map_err(|e| {
        error!("Failed to get write block response: {}", e);
        format!("Failed to get write block response: {e}")
    })?;
    let inner = response.into_inner();
    debug!(
        block_id = &block_id,
        bytes_written = inner.bytes_written,
        "Block upload completed"
    );
    Ok(inner)
}

#[instrument(skip(file_handle, data_node_client, settings, access_token))]
async fn download_block(
    block_id: String,
    data_node_client: DataNodeServiceClient<tonic::transport::Channel>,
    offset: u64,
    file_handle: Arc<std::fs::File>,
    settings: Arc<DafsSettings>,
    access_token: Arc<String>,
) -> Result<(), String> {
    debug!(block_id, "Starting block download");
    let mut hasher = Hasher::new();
    let mut client = data_node_client;
    let mut current_offset = offset;

    let mut stream = client
        .read_block({
            let mut req = Request::new(ReadBlockRequest {
                block_id: block_id.clone(),
            });
            common::attach_bearer_token(&mut req, access_token.as_str())
                .map_err(|e| format!("Could not attach access token: {e}"))?;
            req
        })
        .await
        .map_err(|e| format!("Could not read block: {e}"))?
        .into_inner();

    let mut writing_tasks = JoinSet::new();
    while let Some(msg) = stream.next().await {
        let data = msg.map_err(|e| format!("Failed to receive block chunk: {e}"))?;
        if let Some(ResponseData::Checksum(checksum)) = data.response_data {
            let calculated_checksum = hasher.finalize();
            if checksum != calculated_checksum.to_string() {
                error!(
                    block_id,
                    "Checksum mismatch: expected {}, got {}", checksum, calculated_checksum
                );
                return Err(format!(
                    "Checksum mismatch for block {block_id}: expected {checksum}, got {calculated_checksum}"
                ));
            }
            debug!(block_id, "Block download completed with valid checksum");
            break;
        }
        let chunk_data = match data.response_data {
            Some(ResponseData::ChunkData(data)) => data,
            _ => {
                return Err(format!(
                    "Unexpected response data type for block {block_id}"
                ))
            }
        };
        hasher.update(&chunk_data);
        let data_len = chunk_data.len() as u64;
        let write_offset = current_offset;
        let file = Arc::clone(&file_handle);
        writing_tasks.spawn_blocking(move || file.write_at(&chunk_data, write_offset));
        current_offset += data_len;
        if writing_tasks.len() > settings.download_concurrency {
            writing_tasks.join_next().await;
        }
    }

    while let Some(res) = writing_tasks.join_next().await {
        res.map_err(|err| format!("Block write task failed: {err}"))?
            .map_err(|err| format!("Failed to write block data: {err}"))?;
    }
    debug!(block_id, "Block downloaded");
    Ok(())
}

// ─── File-level I/O ───────────────────────────────────────────────────────────

/// Upload a local file to the DFS at the given remote path.
///
/// `remote_path` is the path in the DFS namespace (e.g. `/photos/cat.jpg`).
/// `access_token` is the JWT obtained via [`login_user`].
#[instrument(skip(settings, access_token))]
pub async fn upload_file(
    settings: Arc<DafsSettings>,
    local_path: String,
    remote_path: String,
    access_token: &str,
) -> Result<CreateFileResponse, Box<dyn Error + Send + Sync>> {
    info!(local_path, remote_path, "Starting file upload");
    let access_token = Arc::new(access_token.to_owned());
    let mut meta_client =
        MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;

    let file_handle = std::fs::File::open(&local_path)?;
    let mut create_file_request = Request::new(CreateFileRequest {
        path: remote_path.clone(),
        size: file_handle.metadata()?.size() as i64, // TODO: files > 4 GB
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut create_file_request, access_token.as_str())?;
    let create_file_response = meta_client
        .create_file(create_file_request)
        .await?
        .into_inner();

    let shared_file_handle = Arc::new(file_handle);
    let semaphore = Arc::new(Semaphore::new(settings.upload_concurrency));
    let mut set = JoinSet::new();
    let mut block_size = -1i64;

    let nodes_to_connect: HashSet<_> = create_file_response
        .blocks
        .iter()
        .map(|b| b.node_address.clone())
        .collect();
    let mut clients = HashMap::new();
    for addr in nodes_to_connect {
        let client = DataNodeServiceClient::connect(addr.clone()).await?;
        clients.insert(addr, client);
    }

    for block_assignment in create_file_response.blocks.clone() {
        if block_size == -1 {
            block_size = block_assignment.block_size;
        }
        let block_offset = block_assignment.order * block_size;
        let block_id = block_assignment.block_id.clone();
        let f_handle = Arc::clone(&shared_file_handle);
        let settings_clone = Arc::clone(&settings);
        let token_clone = Arc::clone(&access_token);
        let sem = Arc::clone(&semaphore);
        let client = clients[&block_assignment.node_address].clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.map_err(|e| e.to_string())?;
            upload_block(
                block_id,
                client,
                block_offset as u64,
                block_assignment.block_size,
                f_handle,
                settings_clone,
                token_clone,
            )
            .await
        });
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| {
            error!(local_path, "Block upload task panicked: {}", e);
            format!("Block upload task panicked: {e}")
        })??;
    }
    info!(
        local_path,
        blocks = create_file_response.blocks.len(),
        "File upload completed"
    );

    let mut set_status_request = Request::new(SetFileStatusRequest {
        path: remote_path,
        status: settings.upload_ready_status.clone(),
    });
    common::attach_bearer_token(&mut set_status_request, access_token.as_str())?;
    meta_client.set_file_status(set_status_request).await?;
    Ok(create_file_response)
}

/// Download a remote DFS file to a local path.
///
/// `access_token` is the JWT obtained via [`login_user`].
#[instrument(skip(settings, access_token))]
pub async fn download_file(
    settings: Arc<DafsSettings>,
    remote_path: String,
    local_path: String,
    access_token: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!(remote_path, local_path, "Starting file download");
    let access_token = Arc::new(access_token.to_owned());

    let get_file_response =
        get_file_metadata(Arc::clone(&settings), remote_path.clone(), &access_token).await?;

    let file_handle = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&local_path)?;
    file_handle.set_len(get_file_response.size as u64)?;
    let shared_file_handle = Arc::new(file_handle);
    let semaphore = Arc::new(Semaphore::new(settings.download_concurrency));
    let mut set = JoinSet::new();
    let mut block_size = -1i64;

    let nodes_to_connect: HashSet<_> = get_file_response
        .blocks
        .iter()
        .map(|b| b.node_address.clone())
        .collect();
    let mut clients = HashMap::new();
    for addr in nodes_to_connect {
        let client = DataNodeServiceClient::connect(addr.clone()).await?;
        clients.insert(addr, client);
    }

    for block_assignment in get_file_response.blocks.clone() {
        if block_size == -1 {
            block_size = block_assignment.block_size;
        }
        let permit = semaphore.clone().acquire_owned().await?;
        let block_offset = block_assignment.order * block_size;
        let block_id = block_assignment.block_id.clone();
        let f_handle = Arc::clone(&shared_file_handle);
        let settings_clone = Arc::clone(&settings);
        let token_clone = Arc::clone(&access_token);
        let client = clients[&block_assignment.node_address].clone();
        set.spawn(async move {
            let result = download_block(
                block_id,
                client,
                block_offset as u64,
                f_handle,
                settings_clone,
                token_clone,
            )
            .await;
            drop(permit);
            result
        });
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| {
            error!(remote_path, "Block download task panicked: {}", e);
            format!("Block download task panicked: {e}")
        })??;
    }
    info!(remote_path, local_path, "File download completed");
    Ok(())
}
