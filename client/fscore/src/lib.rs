use bytes::BytesMut;
use common::dfs::data_node_service_client::DataNodeServiceClient;
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::read_block_response::ResponseData;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    AllocateBlocksRequest, AllocateBlocksResponse, CreateFileRequest, CreateFileResponse, CreateDirectoryRequest, DeleteFileRequest, DeleteDirectoryRequest, GetFileRequest, GetFileResponse, ListDirectoryRequest, ListDirectoryResponse, LoginUserRequest, ReadBlockRequest, RegisterUserRequest, RenameFileRequest, SetFilePermissionsRequest, SetFileStatusRequest, GetSystemStatsRequest, WriteBlockRequest, WriteBlockResponse
};
use common::raft::raft_service_client::RaftServiceClient;
use common::raft::GetLeaderRequest;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::OnceLock;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
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
    pub block_size: u64,
    pub realign_flush_delay_ms: u64,
    pub metadata_cache_ttl_ms: u64,
    pub read_block_cache_entries: usize,
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
            log_level: "warn".to_string(),
            upload_chunk_size: 1024 * 1024,
            block_size: 1_048_576,
            realign_flush_delay_ms: 10,
            metadata_cache_ttl_ms: 500,
            read_block_cache_entries: 512,
            default_user_id: "1".to_string(),
            upload_ready_status: "READY".to_string(),
            default_list_path: "/".to_string(),
            default_rename_old_path: "/".to_string(),
            default_rename_new_path: "/".to_string(),
            access_token_path: "client/.token".to_string(),
        }
    }
}

type DatanodeClient = DataNodeServiceClient<tonic::transport::Channel>;
type MetadataClient = MetadataServiceClient<Channel>;
static DATANODE_CLIENT_CACHE: OnceLock<RwLock<HashMap<String, DatanodeClient>>> = OnceLock::new();
static METADATA_CLIENT_CACHE: OnceLock<RwLock<HashMap<String, MetadataClient>>> = OnceLock::new();
static LEADER_CACHE: OnceLock<RwLock<Option<(Instant, String)>>> = OnceLock::new();
static METADATA_CACHE: OnceLock<RwLock<HashMap<String, (Instant, GetFileResponse)>>> = OnceLock::new();
static BLOCK_READ_CACHE: OnceLock<RwLock<BlockReadCache>> = OnceLock::new();

/// LRU cache of downloaded block payloads. Each entry tracks a logical access
/// tick; eviction drops the least-recently-used entry. Eviction is O(n) over
/// the (small, bounded) entry set, only on insert-when-full.
#[derive(Default)]
struct BlockReadCache {
    map: HashMap<String, (Arc<Vec<u8>>, u64)>,
    clock: u64,
}

/// How long a discovered Raft leader stays trusted before we re-discover it.
const LEADER_CACHE_TTL_MS: u64 = 30_000;

fn datanode_client_cache() -> &'static RwLock<HashMap<String, DatanodeClient>> {
    DATANODE_CLIENT_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn metadata_client_cache() -> &'static RwLock<HashMap<String, MetadataClient>> {
    METADATA_CLIENT_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn leader_cache() -> &'static RwLock<Option<(Instant, String)>> {
    LEADER_CACHE.get_or_init(|| RwLock::new(None))
}

fn metadata_cache() -> &'static RwLock<HashMap<String, (Instant, GetFileResponse)>> {
    METADATA_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn block_read_cache() -> &'static RwLock<BlockReadCache> {
    BLOCK_READ_CACHE.get_or_init(|| RwLock::new(BlockReadCache::default()))
}

async fn get_cached_metadata(path: &str, ttl_ms: u64) -> Option<GetFileResponse> {
    let cache = metadata_cache().read().await;
    let (cached_at, data) = cache.get(path)?;
    if cached_at.elapsed().as_millis() <= ttl_ms as u128 {
        Some(data.clone())
    } else {
        None
    }
}

async fn put_cached_metadata(path: String, metadata: GetFileResponse) {
    metadata_cache()
        .write()
        .await
        .insert(path, (Instant::now(), metadata));
}

async fn invalidate_metadata_cache(path: &str) {
    metadata_cache().write().await.remove(path);
}

async fn get_cached_block(block_id: &str) -> Option<Arc<Vec<u8>>> {
    // Write lock because a hit bumps the entry's recency. The critical section
    // is just a hashmap lookup + Arc clone (microseconds), so this does not
    // meaningfully serialize the concurrent, network-bound block fetches.
    let mut cache = block_read_cache().write().await;
    cache.clock += 1;
    let now = cache.clock;
    let entry = cache.map.get_mut(block_id)?;
    entry.1 = now;
    Some(Arc::clone(&entry.0))
}

async fn put_cached_block(block_id: String, data: Vec<u8>, max_entries: usize) -> Arc<Vec<u8>> {
    let entry = Arc::new(data);
    let mut cache = block_read_cache().write().await;
    cache.clock += 1;
    let now = cache.clock;

    let cap = max_entries.max(1);
    if !cache.map.contains_key(&block_id) && cache.map.len() >= cap {
        // Evict the least-recently-used entry (smallest access tick).
        if let Some(lru_key) = cache
            .map
            .iter()
            .min_by_key(|(_, (_, tick))| *tick)
            .map(|(key, _)| key.clone())
        {
            cache.map.remove(&lru_key);
        }
    }
    cache.map.insert(block_id, (Arc::clone(&entry), now));
    entry
}

async fn invalidate_block_cache(block_id: &str) {
    block_read_cache().write().await.map.remove(block_id);
}

async fn get_or_connect_datanode_client(
    addr: &str,
) -> Result<DatanodeClient, Box<dyn Error + Send + Sync>> {
    if let Some(client) = datanode_client_cache().read().await.get(addr).cloned() {
        return Ok(client);
    }

    let connected = DataNodeServiceClient::connect(addr.to_string()).await?;
    let mut cache = datanode_client_cache().write().await;
    let client = cache
        .entry(addr.to_string())
        .or_insert_with(|| connected.clone())
        .clone();
    Ok(client)
}

async fn build_datanode_clients(
    node_addresses: HashSet<String>,
) -> Result<HashMap<String, DatanodeClient>, Box<dyn Error + Send + Sync>> {
    let mut clients = HashMap::new();
    for addr in node_addresses {
        clients.insert(addr.clone(), get_or_connect_datanode_client(&addr).await?);
    }
    Ok(clients)
}

fn parse_leader_addr_from_message(message: &str) -> Option<String> {
    let marker = "leader_addr=";
    let start = message.find(marker)? + marker.len();
    let rest = &message[start..];
    let addr = rest.split(';').next().unwrap_or(rest).trim();
    if addr.is_empty() || addr == "unknown" {
        None
    } else {
        Some(addr.to_string())
    }
}

fn prioritized_endpoints(settings: &DafsSettings, preferred: Option<String>) -> Vec<String> {
    let mut endpoints = Vec::new();
    if let Some(ep) = preferred {
        endpoints.push(ep);
    }
    for ep in &settings.master_endpoints {
        if !endpoints.iter().any(|existing| existing == ep) {
            endpoints.push(ep.clone());
        }
    }
    endpoints
}

/// Actually probe every master via the Raft service to find the current leader.
/// This opens fresh connections, so callers should prefer the cached wrapper.
async fn discover_leader_endpoint_uncached(settings: &DafsSettings) -> Option<String> {
    for endpoint in &settings.master_endpoints {
        let mut client = match RaftServiceClient::connect(endpoint.clone()).await {
            Ok(client) => client,
            Err(_) => continue,
        };

        let response = match client.get_leader(Request::new(GetLeaderRequest {})).await {
            Ok(resp) => resp.into_inner(),
            Err(_) => continue,
        };

        if response.known && !response.leader_address.is_empty() {
            return Some(response.leader_address);
        }
    }
    None
}

/// Cached leader lookup. Avoids re-probing all masters (fresh TCP + RPC) on
/// every metadata operation; the leader is stable except during failover.
async fn discover_leader_endpoint(settings: &DafsSettings) -> Option<String> {
    {
        let guard = leader_cache().read().await;
        if let Some((cached_at, addr)) = guard.as_ref() {
            if cached_at.elapsed().as_millis() <= LEADER_CACHE_TTL_MS as u128 {
                return Some(addr.clone());
            }
        }
    }

    let discovered = discover_leader_endpoint_uncached(settings).await;
    if let Some(addr) = discovered.as_ref() {
        *leader_cache().write().await = Some((Instant::now(), addr.clone()));
    }
    discovered
}

/// Connection-pooled metadata client. HTTP/2 channels multiplex requests, so a
/// single cached channel per endpoint serves all concurrent calls.
async fn get_or_connect_metadata_client(
    endpoint: &str,
) -> Result<MetadataClient, Box<dyn Error + Send + Sync>> {
    if let Some(client) = metadata_client_cache().read().await.get(endpoint).cloned() {
        return Ok(client);
    }

    let connected = MetadataServiceClient::connect(endpoint.to_string()).await?;
    let mut cache = metadata_client_cache().write().await;
    let client = cache
        .entry(endpoint.to_string())
        .or_insert_with(|| connected.clone())
        .clone();
    Ok(client)
}

async fn connect_metadata_client(
    settings: &DafsSettings,
    preferred: Option<String>,
) -> Result<MetadataServiceClient<Channel>, Box<dyn Error + Send + Sync>> {
    // Note: on failover the cached leader self-heals when its TTL expires; the
    // FailedPrecondition retry path still reaches the correct leader meanwhile.
    let mut last_error: Option<Box<dyn Error + Send + Sync>> = None;
    for endpoint in prioritized_endpoints(settings, preferred) {
        match get_or_connect_metadata_client(&endpoint).await {
            Ok(client) => return Ok(client),
            Err(err) => last_error = Some(err),
        }
    }

    Err(last_error.unwrap_or_else(|| "No reachable master endpoint".into()))
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
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let req = Request::new(RegisterUserRequest {
        username: username.clone(),
        password: password.clone(),
    });

    match client.register_user(req).await {
        Ok(_) => {}
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            retry_client
                .register_user(Request::new(RegisterUserRequest {
                    username: username.clone(),
                    password: password.clone(),
                }))
                .await?;
        }
        Err(status) => return Err(Box::new(status)),
    }

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
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let response = match client
        .login_user(Request::new(LoginUserRequest {
            username: username.clone(),
            password: password.clone(),
        }))
        .await
    {
        Ok(resp) => resp.into_inner(),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            retry_client
                .login_user(Request::new(LoginUserRequest { username, password }))
                .await?
                .into_inner()
        }
        Err(status) => return Err(Box::new(status)),
    };

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
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut request = Request::new(GetFileRequest {
        path: remote_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;

    match client.get_file_metadata(request).await {
        Ok(resp) => Ok(resp.into_inner()),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(GetFileRequest {
                path: remote_path,
                user_id: settings.default_user_id.clone(),
            });
            common::attach_bearer_token(&mut retry_request, access_token)?;
            Ok(retry_client
                .get_file_metadata(retry_request)
                .await?
                .into_inner())
        }
        Err(status) => Err(Box::new(status)),
    }
}

/// List the contents of a remote directory.
#[instrument(skip(settings, access_token))]
pub async fn list_directory(
    settings: Arc<DafsSettings>,
    directory: String,
    access_token: &str,
) -> Result<ListDirectoryResponse, Box<dyn Error + Send + Sync>> {
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut request = Request::new(ListDirectoryRequest {
        path: directory.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;

    match client.list_directory(request).await {
        Ok(resp) => Ok(resp.into_inner()),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(ListDirectoryRequest { path: directory });
            common::attach_bearer_token(&mut retry_request, access_token)?;
            Ok(retry_client
                .list_directory(retry_request)
                .await?
                .into_inner())
        }
        Err(status) => Err(Box::new(status)),
    }
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
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut request = Request::new(RenameFileRequest {
        old_path: old_path.clone(),
        new_path: new_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;

    match client.rename_file(request).await {
        Ok(_) => {}
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(RenameFileRequest {
                old_path: old_path.clone(),
                new_path: new_path.clone(),
                user_id: settings.default_user_id.clone(),
            });
            common::attach_bearer_token(&mut retry_request, access_token)?;
            retry_client.rename_file(retry_request).await?;
        }
        Err(status) => return Err(Box::new(status)),
    }

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
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut request = Request::new(DeleteFileRequest {
        path: remote_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;

    match client.delete_file(request).await {
        Ok(_) => {}
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(DeleteFileRequest {
                path: remote_path.clone(),
                user_id: settings.default_user_id.clone(),
            });
            common::attach_bearer_token(&mut retry_request, access_token)?;
            retry_client.delete_file(retry_request).await?;
        }
        Err(status) => return Err(Box::new(status)),
    }

    info!(remote_path, "File deleted");
    Ok(())
}

/// Create a directory in the remote filesystem.
#[instrument(skip(settings, access_token))]
pub async fn create_directory(
    settings: Arc<DafsSettings>,
    directory_path: String,
    access_token: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!(directory_path, "Creating directory");
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut request = Request::new(CreateDirectoryRequest {
        path: directory_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;

    match client.create_directory(request).await {
        Ok(_) => {
            info!(directory_path, "Directory created successfully");
            Ok(())
        }
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(CreateDirectoryRequest {
                path: directory_path.clone(),
                user_id: settings.default_user_id.clone(),
            });
            common::attach_bearer_token(&mut retry_request, access_token)?;
            retry_client.create_directory(retry_request).await?;
            info!(directory_path, "Directory created successfully (after retry)");
            Ok(())
        }
        Err(status) => Err(Box::new(status)),
    }
}

/// Delete a directory from the remote filesystem (must be empty).
#[instrument(skip(settings, access_token))]
pub async fn delete_directory(
    settings: Arc<DafsSettings>,
    directory_path: String,
    access_token: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!(directory_path, "Deleting directory");
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut request = Request::new(DeleteDirectoryRequest {
        path: directory_path.clone(),
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut request, access_token)?;

    match client.delete_directory(request).await {
        Ok(_) => {
            info!(directory_path, "Directory deleted successfully");
            Ok(())
        }
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(DeleteDirectoryRequest {
                path: directory_path.clone(),
                user_id: settings.default_user_id.clone(),
            });
            common::attach_bearer_token(&mut retry_request, access_token)?;
            retry_client.delete_directory(retry_request).await?;
            info!(directory_path, "Directory deleted successfully (after retry)");
            Ok(())
        }
        Err(status) => Err(Box::new(status)),
    }
}

/// Get filesystem statistics (total/free blocks and inodes).
#[instrument(skip(settings, access_token))]
pub async fn get_system_stats(
    settings: Arc<DafsSettings>,
    access_token: &str,
) -> Result<common::dfs::GetSystemStatsResponse, Box<dyn Error + Send + Sync>> {
    info!("Querying system statistics");
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut request = Request::new(GetSystemStatsRequest {});
    common::attach_bearer_token(&mut request, access_token)?;

    match client.get_system_stats(request).await {
        Ok(resp) => {
            let stats = resp.into_inner();
            info!(
                total_blocks = stats.total_blocks,
                free_blocks = stats.free_blocks,
                total_inodes = stats.total_inodes,
                free_inodes = stats.free_inodes,
                "System statistics retrieved"
            );
            Ok(stats)
        }
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(GetSystemStatsRequest {});
            common::attach_bearer_token(&mut retry_request, access_token)?;
            let stats = retry_client
                .get_system_stats(retry_request)
                .await?
                .into_inner();
            Ok(stats)
        }
        Err(status) => Err(Box::new(status)),
    }
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
            offset: 0,  // New blocks always write from start; offset used for append/overwrite
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

#[allow(clippy::too_many_arguments)]
#[instrument(skip(data_node_client, settings, access_token, shared_data))]
async fn upload_block_from_shared_range(
    block_id: String,
    data_node_client: DataNodeServiceClient<tonic::transport::Channel>,
    write_offset_in_block: u64,
    shared_data: Arc<Vec<u8>>,
    range_start: usize,
    range_len: usize,
    settings: Arc<DafsSettings>,
    access_token: Arc<String>,
) -> Result<WriteBlockResponse, String> {
    let range_end = range_start
        .checked_add(range_len)
        .ok_or_else(|| "Range overflow while uploading shared block data".to_string())?;
    let range = shared_data
        .get(range_start..range_end)
        .ok_or_else(|| "Invalid shared range while uploading block".to_string())?;

    debug!(
        block_id,
        write_offset_in_block,
        bytes = range.len(),
        "Starting shared-range block upload"
    );
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
            total_size: range.len() as i64,
            offset: write_offset_in_block,
        })),
    })
    .await
    .map_err(|e| format!("Failed to send block metadata: {e}"))?;

    let chunk_size = settings.upload_chunk_size.max(1);
    for chunk in range.chunks(chunk_size) {
        hasher.update(chunk);
        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::ChunkData(chunk.to_vec())),
        })
        .await
        .map_err(|e| format!("Failed to send chunk data: {e}"))?;
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
    let resp = response.into_inner();

    // Ensure the block was durably written (fsync confirmed by datanode)
    if !resp.synced {
        return Err(format!(
            "Block {} write not confirmed as durable by datanode",
            block_id
        ));
    }
    Ok(resp)
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

#[instrument(skip(data_node_client, access_token))]
async fn download_block_to_vec(
    block_id: String,
    data_node_client: DataNodeServiceClient<tonic::transport::Channel>,
    access_token: Arc<String>,
) -> Result<Vec<u8>, String> {
    let mut hasher = Hasher::new();
    let mut client = data_node_client;
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

    let mut block_data = Vec::new();
    while let Some(msg) = stream.next().await {
        let data = msg.map_err(|e| format!("Failed to receive block chunk: {e}"))?;
        if let Some(ResponseData::Checksum(checksum)) = data.response_data {
            let calculated_checksum = hasher.finalize();
            if checksum != calculated_checksum.to_string() {
                return Err(format!(
                    "Checksum mismatch for block {block_id}: expected {checksum}, got {calculated_checksum}"
                ));
            }
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
        block_data.extend_from_slice(&chunk_data);
    }

    Ok(block_data)
}

// ─── File-level I/O ───────────────────────────────────────────────────────────

pub async fn create_file(
    settings: Arc<DafsSettings>,
    remote_path: String,
    access_token: &str,
    file_size: i64,
) -> Result<CreateFileResponse, Box<dyn Error + Send + Sync>> {
    info!(remote_path, "Creating file");
    let access_token = Arc::new(access_token.to_owned());
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut meta_client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut create_file_request = Request::new(CreateFileRequest {
        path: remote_path.clone(),
        size: file_size,
        user_id: settings.default_user_id.clone(),
    });
    common::attach_bearer_token(&mut create_file_request, access_token.as_str())?;
    let create_file_response = match meta_client.create_file(create_file_request).await {
        Ok(resp) => resp.into_inner(),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            meta_client = connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(CreateFileRequest {
                path: remote_path.clone(),
                size: file_size,
                user_id: settings.default_user_id.clone(),
            });
            common::attach_bearer_token(&mut retry_request, access_token.as_str())?;
            meta_client.create_file(retry_request).await?.into_inner()
        }
        Err(status) => return Err(Box::new(status)),
    };
    Ok(create_file_response)
}

pub async fn set_file_status_ready(
    settings: Arc<DafsSettings>,
    file_path: String,
    access_token: &str,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let mut set_status_request = Request::new(SetFileStatusRequest {
        path: file_path.clone(),
        status: settings.upload_ready_status.clone(),
    });
    common::attach_bearer_token(&mut set_status_request, access_token)?;
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut meta_client = connect_metadata_client(settings.as_ref(), preferred).await?;
    match meta_client.set_file_status(set_status_request).await {
        Ok(_) => Ok(true),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(SetFileStatusRequest {
                path: file_path,
                status: settings.upload_ready_status.clone(),
            });
            common::attach_bearer_token(&mut retry_request, access_token)?;
            retry_client.set_file_status(retry_request).await?;
            Ok(true)
        }
        Err(status) => Err(Box::new(status)),
    }
}

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
    let file_handle = std::fs::File::open(&local_path)?;
    let file_size = file_handle.metadata()?.size() as i64;
    let create_file_response = create_file(
        settings.clone(),
        remote_path.clone(),
        &access_token,
        file_size,
    )
    .await?;

    let shared_file_handle = Arc::new(file_handle);
    let semaphore = Arc::new(Semaphore::new(settings.upload_concurrency));
    let mut set = JoinSet::new();
    let logical_block_size = settings.block_size.max(1);

    let nodes_to_connect: HashSet<_> = create_file_response
        .blocks
        .iter()
        .map(|b| b.node_address.clone())
        .collect();
    let clients = build_datanode_clients(nodes_to_connect).await?;

    for block_assignment in create_file_response.blocks.clone() {
        let block_offset = (block_assignment.order.max(0) as u64)
            .saturating_mul(logical_block_size);
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
                block_offset,
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

    set_file_status_ready(settings.clone(), remote_path.clone(), &access_token).await?;
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
    let logical_block_size = settings.block_size.max(1);

    let nodes_to_connect: HashSet<_> = get_file_response
        .blocks
        .iter()
        .map(|b| b.node_address.clone())
        .collect();
    let clients = build_datanode_clients(nodes_to_connect).await?;

    for block_assignment in get_file_response.blocks.clone() {
        let permit = semaphore.clone().acquire_owned().await?;
        let block_offset = (block_assignment.order.max(0) as u64)
            .saturating_mul(logical_block_size);
        let block_id = block_assignment.block_id.clone();
        let f_handle = Arc::clone(&shared_file_handle);
        let settings_clone = Arc::clone(&settings);
        let token_clone = Arc::clone(&access_token);
        let client = clients[&block_assignment.node_address].clone();
        set.spawn(async move {
            let result = download_block(
                block_id,
                client,
                block_offset,
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

/// Set file permissions (chmod).
#[instrument(skip(settings, access_token))]
pub async fn set_file_permissions(
    settings: Arc<DafsSettings>,
    remote_path: String,
    mode: u32,
    access_token: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!(remote_path, mode = format!("{:o}", mode), "Setting file permissions");
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut request = Request::new(SetFilePermissionsRequest {
        path: remote_path.clone(),
        mode,
    });
    common::attach_bearer_token(&mut request, access_token)?;

    match client.set_file_permissions(request).await {
        Ok(_) => {
            info!(remote_path, mode = format!("{:o}", mode), "File permissions set successfully");
            Ok(())
        }
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            let mut retry_client =
                connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(SetFilePermissionsRequest {
                path: remote_path.clone(),
                mode,
            });
            common::attach_bearer_token(&mut retry_request, access_token)?;
            retry_client.set_file_permissions(retry_request).await?;
            info!(remote_path, mode = format!("{:o}", mode), "File permissions set successfully (after retry)");
            Ok(())
        }
        Err(status) => Err(Box::new(status)),
    }
}

#[instrument(skip(settings, access_token))]
pub async fn allocate_blocks(
    settings: Arc<DafsSettings>,
    file_path: String,
    file_size: u64,
    blocks_offset: u64,
    access_token: &str,
) -> Result<AllocateBlocksResponse, Box<dyn Error + Send + Sync>> {
    info!(file_path, file_size, blocks_offset, "Starting block allocation");
    let access_token = Arc::new(access_token.to_owned());
    let preferred = discover_leader_endpoint(settings.as_ref()).await;
    let mut meta_client = connect_metadata_client(settings.as_ref(), preferred).await?;
    let mut allocate_blocks_request = Request::new(AllocateBlocksRequest {
        file_id: file_path.clone(),
        size: file_size,
        offset: blocks_offset
    });
    common::attach_bearer_token(&mut allocate_blocks_request, access_token.as_str())?;
    let allocate_block_response = match meta_client.allocate_blocks(allocate_blocks_request).await {
        Ok(resp) => resp.into_inner(),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            let Some(leader_addr) = parse_leader_addr_from_message(status.message()) else {
                return Err(Box::new(status));
            };
            meta_client = connect_metadata_client(settings.as_ref(), Some(leader_addr)).await?;
            let mut retry_request = Request::new(AllocateBlocksRequest {
                file_id: file_path.clone(),
                size: file_size,
                offset: blocks_offset
            });
            common::attach_bearer_token(&mut retry_request, access_token.as_str())?;
            meta_client.allocate_blocks(retry_request).await?.into_inner()
        }
        Err(status) => return Err(Box::new(status)),
    };
    Ok(allocate_block_response)
}

#[instrument(skip(settings, access_token, write_data))]
pub async fn write_file_range(
    settings: Arc<DafsSettings>,
    remote_path: String,
    offset: u64,
    write_data: Arc<Vec<u8>>,
    access_token: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if write_data.is_empty() {
        return Ok(());
    }

    let access_token = Arc::new(access_token.to_owned());
    let allocation = allocate_blocks(
        Arc::clone(&settings),
        remote_path.clone(),
        write_data.len() as u64,
        offset,
        access_token.as_str(),
    )
    .await?;

    let nodes_to_connect: HashSet<_> = allocation
        .blocks
        .iter()
        .map(|b| b.node_address.clone())
        .collect();
    let clients = build_datanode_clients(nodes_to_connect).await?;

    let logical_block_size = settings.block_size.max(1);
    let mut cursor = 0usize;
    let mut written_block_ids: Vec<String> = Vec::new();
    let semaphore = Arc::new(Semaphore::new(settings.upload_concurrency.max(1)));
    let mut set = JoinSet::new();
    for assignment in allocation.blocks {
        let chunk_len = assignment.block_size.max(0) as usize;
        if chunk_len == 0 {
            continue;
        }
        if cursor + chunk_len > write_data.len() {
            return Err(format!(
                "AllocateBlocks returned invalid layout: chunk overruns write buffer (cursor={}, chunk_len={}, total={})",
                cursor,
                chunk_len,
                write_data.len()
            )
            .into());
        }

        let chunk_start = cursor;

        // Calculate the offset within the block
        let block_idx = assignment.order.max(0) as u64;
        let block_start = block_idx * logical_block_size;
        let file_position = offset + chunk_start as u64;
        let write_offset_in_block = file_position - block_start;

        cursor += chunk_len;

        let client = clients
            .get(&assignment.node_address)
            .ok_or_else(|| format!("No datanode client for {}", assignment.node_address))?
            .clone();

        let block_id = assignment.block_id;
        written_block_ids.push(block_id.clone());
        let settings_clone = Arc::clone(&settings);
        let token_clone = Arc::clone(&access_token);
        let sem = Arc::clone(&semaphore);
        let shared_data = Arc::clone(&write_data);
        let remote_path_for_task = remote_path.clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.map_err(|e| e.to_string())?;
            upload_block_from_shared_range(
                block_id,
                client,
                write_offset_in_block,
                shared_data,
                chunk_start,
                chunk_len,
                settings_clone,
                token_clone,
            )
            .await
            .map_err(|e| format!("Failed writing file range for {remote_path_for_task}: {e}"))
        });
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| format!("write_file_range task panicked: {e}"))??;
    }

    if cursor != write_data.len() {
        return Err(format!(
            "AllocateBlocks returned incomplete layout: wrote {} of {} bytes",
            cursor,
            write_data.len()
        )
        .into());
    }

    invalidate_metadata_cache(&remote_path).await;
    for block_id in written_block_ids {
        invalidate_block_cache(&block_id).await;
    }

    Ok(())
}

#[instrument(skip(settings, access_token))]
pub async fn read_file_range(
    settings: Arc<DafsSettings>,
    remote_path: String,
    offset: u64,
    size: u64,
    access_token: &str,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    if size == 0 {
        return Ok(Vec::new());
    }

    let access_token = Arc::new(access_token.to_owned());
    let metadata = if let Some(cached) =
        get_cached_metadata(&remote_path, settings.metadata_cache_ttl_ms).await
    {
        cached
    } else {
        let fresh =
            get_file_metadata(Arc::clone(&settings), remote_path.clone(), access_token.as_str())
                .await?;
        put_cached_metadata(remote_path.clone(), fresh.clone()).await;
        fresh
    };
    let file_size = metadata.size.max(0) as u64;
    if offset >= file_size {
        return Ok(Vec::new());
    }

    let end = std::cmp::min(offset.saturating_add(size), file_size);
    let read_len = (end - offset) as usize;
    let mut out = vec![0u8; read_len];

    // Physical block payload size varies (last/partial writes), but logical
    // block addressing always uses configured DFS block span.
    let logical_block_size = settings.block_size.max(1);

    let nodes_to_connect: HashSet<_> = metadata
        .blocks
        .iter()
        .map(|b| b.node_address.clone())
        .collect();
    let clients = build_datanode_clients(nodes_to_connect).await?;

    // Fetch every overlapping block concurrently (bounded by download_concurrency)
    // instead of serializing one network round-trip after another. Each task owns
    // a disjoint slice of `out`, so copies happen after all fetches complete.
    let semaphore = Arc::new(Semaphore::new(settings.download_concurrency.max(1)));
    let mut set = JoinSet::new();

    for block in metadata.blocks {
        let block_size = block.block_size.max(0) as u64;
        if block_size == 0 {
            continue;
        }
        let block_start = (block.order.max(0) as u64).saturating_mul(logical_block_size);
        let block_end = block_start.saturating_add(block_size);

        if block_end <= offset || block_start >= end {
            continue;
        }

        let overlap_start = std::cmp::max(block_start, offset);
        let overlap_end = std::cmp::min(block_end, end);
        let within_block_start = (overlap_start - block_start) as usize;
        let overlap_len = (overlap_end - overlap_start) as usize;
        let out_start = (overlap_start - offset) as usize;

        let client = clients
            .get(&block.node_address)
            .ok_or_else(|| format!("No datanode client for {}", block.node_address))?
            .clone();
        let block_id = block.block_id.clone();
        let sem = Arc::clone(&semaphore);
        let token_clone = Arc::clone(&access_token);
        let settings_clone = Arc::clone(&settings);
        let remote_path_for_task = remote_path.clone();

        set.spawn(async move {
            let _permit = sem.acquire_owned().await.map_err(|e| e.to_string())?;

            // Hold the block as an Arc and slice the needed sub-range out of it.
            // Avoids copying the whole block (up to `block_size`) on cache hits.
            let block_bytes: Arc<Vec<u8>> = if let Some(cached) = get_cached_block(&block_id).await {
                cached
            } else {
                let downloaded = download_block_to_vec(block_id.clone(), client, token_clone)
                    .await
                    .map_err(|e| format!("Failed reading block for {remote_path_for_task}: {e}"))?;
                put_cached_block(
                    block_id.clone(),
                    downloaded,
                    settings_clone.read_block_cache_entries,
                )
                .await
            };

            if within_block_start + overlap_len > block_bytes.len() {
                return Err(format!(
                    "Block layout mismatch while reading {remote_path_for_task}: slice [{}..{}) exceeds block size {}",
                    within_block_start,
                    within_block_start + overlap_len,
                    block_bytes.len()
                ));
            }

            Ok::<_, String>((out_start, within_block_start, overlap_len, block_bytes))
        });
    }

    while let Some(res) = set.join_next().await {
        let (out_start, within_block_start, overlap_len, block_bytes) =
            res.map_err(|e| format!("read_file_range task panicked: {e}"))??;
        out[out_start..out_start + overlap_len]
            .copy_from_slice(&block_bytes[within_block_start..within_block_start + overlap_len]);
    }

    Ok(out)
}