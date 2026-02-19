use bytes::BytesMut;
use clap::{Parser, Subcommand};
use common::dfs::data_node_service_client::DataNodeServiceClient;
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::read_block_response::ResponseData;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    CreateFileRequest, CreateFileResponse, GetFileRequest, ListDirectoryRequest, LoginUserRequest,
    ReadBlockRequest, RegisterUserRequest, RenameFileRequest, SetFileStatusRequest,
    WriteBlockRequest, WriteBlockResponse,
};
use crc32fast::Hasher;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
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

fn format_file_size(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1024 * KB;
    const GB: i64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn format_timestamp(millis: i64) -> String {
    let secs = millis / 1000;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn read_access_token(path: &str) -> Result<String, Box<dyn Error>> {
    let token = fs::read_to_string(path).map_err(|_| format!("Cannot read access token from '{path}'. Please log in first using the 'login' command or register with 'register' command"))?;
    let trimmed = token.trim().to_string();
    if trimmed.is_empty() {
        return Err("Stored access token is empty".into());
    }
    Ok(trimmed)
}

fn save_access_token(path: &str, token: &str) -> Result<(), Box<dyn Error>> {
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

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct InternalSettings {
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

pub fn load_config() -> InternalSettings {
    let cli = Cli::parse();
    info!("Loading CLI configuration from '{}'", cli.config_path);

    let has_master_endpoints = !cli.master_endpoints.is_empty();
    let master_endpoints = cli.master_endpoints.clone();

    let mut figment = Figment::new()
        .merge(Serialized::defaults(InternalSettings {
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
            access_token_path: "client/cli/.token".to_string(),
        }))
        .merge(Toml::file(&cli.config_path))
        .merge(Serialized::globals(cli));

    if has_master_endpoints {
        figment = figment.merge(("master_endpoints", master_endpoints));
    }

    figment.extract().expect("Error loading configuration")
}

/// DAFS CLI - Distributed Asynchronous File System client
#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(name = "dafs-cli")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Master node endpoint
    #[arg(short, long, global = true)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    master_endpoints: Vec<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,

    /// Default user id sent to metadata service
    #[arg(long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    default_user_id: Option<String>,

    /// Upload block read chunk size in bytes
    #[arg(long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    upload_chunk_size: Option<usize>,

    /// Path to persisted access token
    #[arg(long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    access_token_path: Option<String>,

    #[command(subcommand)]
    #[serde(skip)]
    command: Option<Commands>,

    #[arg(short, long, default_value = "config.toml", global = true)]
    #[serde(skip)]
    pub config_path: String,
}

#[derive(Subcommand, Debug, Serialize, Deserialize)]
enum Commands {
    /// Register a new user account
    Register {
        #[arg(long)]
        username: String,
        #[arg(long)]
        password: String,
    },
    /// Login and store access token locally
    Login {
        #[arg(long)]
        username: String,
        #[arg(long)]
        password: String,
    },
    /// Upload a file to the distributed file system
    Upload {
        /// Path to the local file to upload
        #[arg(short, long)]
        file: String,

        /// Remote path in the DFS (defaults to the filename)
        #[arg(short, long)]
        remote_path: Option<String>,
    },
    /// Download a file from the distributed file system
    Download {
        /// Remote path in the DFS
        #[arg(short, long)]
        remote_path: String,

        /// Local output file path
        #[arg(short, long)]
        output: String,
    },
    /// List directory contents
    List {
        /// Directory path to list
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Rename a file or move it from old_path to new_path
    Rename {
        #[arg(short, long)]
        old_path: Option<String>,

        #[arg(short, long)]
        new_path: Option<String>,
    },
}

#[instrument(skip(file_handle, data_node_client, settings))]
async fn upload_block(
    block_id: String,
    // data_node_endpoint: String,
    data_node_client: DataNodeServiceClient<tonic::transport::Channel>,
    offset: u64,
    block_size: i64,
    file_handle: Arc<std::fs::File>,
    settings: Arc<InternalSettings>,
    access_token: Arc<String>,
) -> Result<WriteBlockResponse, String> {
    debug!(block_id, "Starting block upload");
    let mut hasher = Hasher::new();
    let (tx, rx) = mpsc::channel(settings.mpsc_upload_channel_size as usize);
    let mut client = data_node_client;
    debug!(block_id, "Connected to datanode for upload");
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
        "Block upload completed successfully"
    );
    Ok(inner)
}

#[instrument(skip(settings))]
async fn upload_file(
    settings: Arc<InternalSettings>,
    file_path: String,
) -> Result<CreateFileResponse, Box<dyn Error>> {
    info!(file_path, "Starting file upload");
    let access_token = Arc::new(read_access_token(&settings.access_token_path)?);
    let mut meta_client =
        MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;

    let file_handle = std::fs::File::open(&file_path)?;
    let mut create_file_request = Request::new(CreateFileRequest {
        path: file_path.clone(),
        size: file_handle.metadata()?.size() as i64, //TODO: file can be larger than 4GB
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
    let mut block_size = -1;

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
        let sem = Arc::clone(&semaphore); // Clonamos el Arc fuera
        let client = clients.get(&block_assignment.node_address).unwrap().clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.map_err(|e| e.to_string())?;

            let result = upload_block(
                block_id,
                client,
                block_offset as u64,
                block_assignment.block_size,
                f_handle,
                settings_clone,
                token_clone,
            )
            .await;

            result
        });
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| {
            error!(file_path, "Block upload task panicked: {}", e);
            format!("Block upload task panicked: {e}")
        })??;
    }
    info!(
        file_path,
        blocks = create_file_response.blocks.len(),
        "File upload completed successfully"
    );
    let mut set_status_request = Request::new(SetFileStatusRequest {
        path: file_path,
        status: settings.upload_ready_status.clone(),
    });
    common::attach_bearer_token(&mut set_status_request, access_token.as_str())?;
    meta_client.set_file_status(set_status_request).await?;
    Ok(create_file_response)
}

#[instrument(skip(file_handle, data_node_client, settings))]
async fn download_block(
    block_id: String,
    data_node_client: DataNodeServiceClient<tonic::transport::Channel>,
    offset: u64,
    file_handle: Arc<std::fs::File>,
    settings: Arc<InternalSettings>,
    access_token: Arc<String>,
) -> Result<bool, String> {
    debug!(block_id, "Starting block download");
    let hasher = Hasher::new();
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
        .map_err(|e| format!("Couldnt read block {e}"))?
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
                    "Checksum mismatch for block {block_id}: expected {checksum}, got {calculated_checksum}"));
            }
            debug!(
                block_id,
                "Block download completed successfully with valid checksum"
            );
            break;
        }
        let chunk_data = match data.response_data {
            Some(ResponseData::ChunkData(data)) => data,
            _ => {
                error!(block_id, "Received unexpected response data type");
                return Err(format!(
                    "Received unexpected response data type for block {block_id}"
                ));
            }
        };
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
        res.map_err(|err| {
            error!(block_id, "Block write task failed: {}", err);
            format!("Block write task failed: {err}")
        })?
        .map_err(|err| {
            error!(block_id, "Failed to write block data to file: {}", err);
            format!("Failed to write block data to file: {err}")
        })?;
    }
    debug!(block_id, "Block downloaded successfully");
    Ok(true)
}

#[instrument(skip(settings))]
async fn download_file(
    settings: Arc<InternalSettings>,
    file_path: String,
    out_file_path: String,
) -> Result<bool, Box<dyn Error>> {
    info!(file_path, "Starting file download");
    let access_token = Arc::new(read_access_token(&settings.access_token_path)?);
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;

    let mut get_file_request = Request::new(GetFileRequest {
        path: file_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut get_file_request, access_token.as_str())?;
    let get_file_response = client
        .get_file_metadata(get_file_request)
        .await?
        .into_inner();
    let file_handle = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&out_file_path)?;
    file_handle.set_len(get_file_response.size as u64)?;
    let shared_file_handle = Arc::new(file_handle);
    let semaphore = Arc::new(Semaphore::new(settings.download_concurrency));
    let mut set = JoinSet::new();

    let mut block_size = -1;
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
        let client = clients.get(&block_assignment.node_address).unwrap().clone();
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
            let _ptr = permit;
            result
        });
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| {
            error!(file_path, "Block download task panicked: {}", e);
            format!("Block download task panicked: {e}")
        })??;
    }
    info!(
        file_path,
        out_file_path, "File download completed successfully"
    );
    Ok(true)
}

#[instrument(skip(settings))]
async fn list_directory(
    settings: Arc<InternalSettings>,
    directory: String,
) -> Result<bool, Box<dyn Error>> {
    info!(directory, "Starting listing directory");
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    let access_token = read_access_token(&settings.access_token_path)?;

    let mut list_request = Request::new(ListDirectoryRequest {
        path: directory.clone(),
    });
    common::attach_bearer_token(&mut list_request, &access_token)?;
    let get_file_response = client.list_directory(list_request).await?.into_inner();
    let file_count = get_file_response.files.len();
    println!(
        "{:<30} {:<12} {:<15} {:<15} {:<20} {:<20} {:<12}",
        "Filename", "Type", "Size", "Owner", "Created", "Modified", "Status"
    );
    println!("{}", "-".repeat(124));
    for file in get_file_response.files {
        let file_type = if file.is_directory {
            "Directory"
        } else {
            "File"
        };
        let size_str = if file.is_directory {
            "-".to_string()
        } else {
            format_file_size(file.size)
        };
        let created_time = format_timestamp(file.created_at);
        let modified_time = format_timestamp(file.last_modified_at);
        println!(
            "{:<30} {:<12} {:<15} {:<15} {:<20} {:<20} {:<12}",
            file.filename,
            file_type,
            size_str,
            file.owner,
            created_time,
            modified_time,
            file.file_status
        );
    }
    info!(directory, file_count, "Directory listing completed");
    Ok(true)
}

#[instrument(skip(settings))]
async fn rename_file(
    settings: Arc<InternalSettings>,
    old_path: String,
    new_path: String,
) -> Result<bool, Box<dyn Error>> {
    info!(old_path, new_path, "Starting renaming file");
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    let access_token = read_access_token(&settings.access_token_path)?;

    let mut request = Request::new(RenameFileRequest {
        old_path: old_path.clone(),
        new_path: new_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, &access_token)?;
    client.rename_file(request).await?;
    info!(old_path, new_path, "File rename completed successfully");
    Ok(true)
}

#[instrument(skip(settings))]
async fn register_user(
    settings: Arc<InternalSettings>,
    username: String,
    password: String,
) -> Result<bool, Box<dyn Error>> {
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;
    client
        .register_user(Request::new(RegisterUserRequest {
            username: username.clone(),
            password: password.clone(),
        }))
        .await?;
    login_user(settings, username, password).await?;
    Ok(true)
}

#[instrument(skip(settings))]
async fn login_user(
    settings: Arc<InternalSettings>,
    username: String,
    password: String,
) -> Result<bool, Box<dyn Error>> {
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
    Ok(true)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let settings = Arc::new(load_config());

    common::init_tracing("cli", &settings.log_level);
    debug!("CLI configuration loaded: {:?}", settings);
    info!("DAFS CLI starting");
    info!("Master node: {}", settings.master_endpoints.join(", "));

    let result = match cli.command {
        Some(Commands::Register { username, password }) => {
            register_user(Arc::clone(&settings), username, password)
                .await
                .map(|_| info!("User registered successfully"))
        }
        Some(Commands::Login { username, password }) => {
            login_user(Arc::clone(&settings), username, password)
                .await
                .map(|_| info!("Login successful"))
        }
        Some(Commands::Upload { file, remote_path }) => {
            let remote = remote_path.unwrap_or_else(|| {
                std::path::Path::new(&file)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(&file)
                    .to_string()
            });
            info!("Uploading {} to {}", file, remote);
            upload_file(Arc::clone(&settings), file).await.map(|resp| {
                info!("File uploaded successfully");
                info!("Blocks created: {}", resp.blocks.len());
            })
        }
        Some(Commands::Download {
            remote_path,
            output,
        }) => {
            info!("Downloading {} to {}", remote_path, output);
            download_file(Arc::clone(&settings), remote_path, output)
                .await
                .map(|_| info!("File downloaded successfully"))
        }
        Some(Commands::List { path }) => list_directory(
            Arc::clone(&settings),
            path.unwrap_or_else(|| settings.default_list_path.clone()),
        )
        .await
        .map(|_| ()),
        Some(Commands::Rename { old_path, new_path }) => {
            let resolved_old = old_path.unwrap_or_else(|| settings.default_rename_old_path.clone());
            let resolved_new = new_path.unwrap_or_else(|| settings.default_rename_new_path.clone());
            info!("Renaming/Moving from {} to {}", resolved_old, resolved_new);
            rename_file(Arc::clone(&settings), resolved_old, resolved_new)
                .await
                .map(|_| info!("File renamed/moved successfully"))
        }
        None => {
            error!("No command provided. Use --help for usage information.");
            std::process::exit(1);
        }
    };

    if let Err(e) = result {
        error!("Error: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests;
