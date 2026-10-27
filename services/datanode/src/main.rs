use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use common::dfs::data_node_service_client::DataNodeServiceClient;
use common::dfs::data_node_service_server::{DataNodeService, DataNodeServiceServer};
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::read_block_response::ResponseData;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    ConfigureNodeRequest, ConfigureNodeResponse, DeleteBlockRequest, DeleteBlockResponse,
    HeartbeatRequest, ReadBlockRequest, ReadBlockResponse, ReplicateBlocksRequest,
    ReplicateBlocksResponse, WriteBlockRequest, WriteBlockResponse,
};
use common::raft::raft_service_client::RaftServiceClient;
use common::raft::GetLeaderRequest;
use crc32fast::Hasher;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::{self, create_dir_all, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, instrument, warn};
const DEFAULT_DATA_FOLDER: &str = "./testing/datanode";
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_LISTEN_ADDRESS: &str = "[::1]:8080";
const DEFAULT_METADATA_NODE_ADDRESS: &str = "http://[::1]:9090";
const DEFAULT_JWT_SECRET: &str = "change-me-master-secret";
const DEFAULT_SERVICE_TOKEN_EXP_SECONDS: i64 = 300;
const DEFAULT_WRITE_SYNC_POLICY: &str = "all";

#[derive(Parser, Debug, Serialize, Deserialize, Default)]
pub struct InternalSettings {
    pub listen_address: String,
    /// Address advertised to the master for other nodes to reach this datanode.
    /// Must be routable from the master (e.g. the container/DNS name in Docker).
    /// When empty, falls back to `listen_address` (fine for single-host setups).
    pub advertise_address: String,
    pub data_folder: String,
    pub log_level: String,
    pub mpsc_channel_size: u8,
    pub read_buffer_size: usize,
    pub write_buffer_size: usize,
    pub write_sync_policy: String,
    pub write_sync_interval_ms: u64,
    pub metadata_node_address: String,
    pub metadata_node_endpoints: Vec<String>,
    pub replication_max_parallel_blocks: usize,
    pub heartbeat_retry_initial_ms: u64,
    pub heartbeat_retry_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_nack_retry_ms: u64,
    pub heartbeat_cpu_usage: f32,
    pub heartbeat_free_space: i64,
    pub heartbeat_used_space: i64,
    pub jwt_secret: String,
    pub service_token_exp_seconds: i64,
}

pub fn load_config() -> InternalSettings {
    let cli = Cli::parse();

    let figment = Figment::new()
        .merge(Serialized::defaults(InternalSettings {
            listen_address: DEFAULT_LISTEN_ADDRESS.to_string(),
            advertise_address: String::new(),
            data_folder: DEFAULT_DATA_FOLDER.to_string(),
            log_level: DEFAULT_LOG_LEVEL.to_string(),
            mpsc_channel_size: 4,
            read_buffer_size: 64 * 1024,
            write_buffer_size: 128 * 1024,
            write_sync_policy: DEFAULT_WRITE_SYNC_POLICY.to_string(),
            write_sync_interval_ms: 0,
            metadata_node_address: DEFAULT_METADATA_NODE_ADDRESS.to_string(),
            metadata_node_endpoints: vec![DEFAULT_METADATA_NODE_ADDRESS.to_string()],
            replication_max_parallel_blocks: 16,
            heartbeat_retry_initial_ms: 500,
            heartbeat_retry_max_ms: 30_000,
            heartbeat_interval_ms: 3_000,
            heartbeat_nack_retry_ms: 1_000,
            heartbeat_cpu_usage: 1.0,
            heartbeat_free_space: 4,
            heartbeat_used_space: 4,
            jwt_secret: DEFAULT_JWT_SECRET.to_string(),
            service_token_exp_seconds: DEFAULT_SERVICE_TOKEN_EXP_SECONDS,
        }))
        .merge(Toml::file(&cli.config_path))
        .merge(Serialized::globals(cli));

    figment.extract().expect("Error loading configuration")
}

/// DAFS DataNode Service
#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(name = "dafs-datanode")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Server listen address
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    listen_address: Option<String>,

    /// Address advertised to the master (must be routable from it, e.g. the
    /// container/DNS name in Docker). Empty falls back to the listen address.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    advertise_address: Option<String>,

    /// Data folder location
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    data_folder: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,

    /// MPSC channel size for read/write operations
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    mpsc_channel_size: Option<u8>,

    /// Read buffer size in bytes
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    read_buffer_size: Option<usize>,

    /// Write buffer size in bytes
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    write_buffer_size: Option<usize>,

    /// Write sync policy: all | data | none
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    write_sync_policy: Option<String>,

    /// Minimum interval between sync operations for the same block (ms). 0 = always.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    write_sync_interval_ms: Option<u64>,

    /// Metadata node address
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_node_address: Option<String>,

    /// Metadata node endpoints (comma-separated)
    #[arg(long, value_delimiter = ',')]
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_node_endpoints: Option<Vec<String>>,

    /// Maximum parallel replication block tasks
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    replication_max_parallel_blocks: Option<usize>,

    /// Initial heartbeat retry delay in milliseconds
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_retry_initial_ms: Option<u64>,

    /// Maximum heartbeat retry delay in milliseconds
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_retry_max_ms: Option<u64>,

    /// Heartbeat interval in milliseconds
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_interval_ms: Option<u64>,

    /// Retry delay when heartbeat is not acknowledged in milliseconds
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_nack_retry_ms: Option<u64>,

    /// CPU usage value sent in heartbeat
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_cpu_usage: Option<f32>,

    /// Free space value sent in heartbeat
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_free_space: Option<i64>,

    /// Used space value sent in heartbeat
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_used_space: Option<i64>,

    /// Secret used to validate and issue service JWTs
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    jwt_secret: Option<String>,

    /// Internal service token validity (seconds)
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    service_token_exp_seconds: Option<i64>,

    #[arg(short, long, default_value = "config.toml")]
    #[serde(skip)]
    pub config_path: String,
}

#[derive(Default)]
pub struct Datanode {
    pub node_id: Arc<Mutex<u16>>,
    pub settings: Arc<InternalSettings>,
    pub block_last_sync: Arc<Mutex<std::collections::HashMap<String, Instant>>>,
}

/// Builds `<data_folder>/<id[0..2]>/<id[2..4]>/<id>` for a block, rejecting ids that
/// are too short to shard or could escape the data folder.
#[allow(clippy::result_large_err)]
fn block_path(data_folder: &str, block_id: &str) -> Result<PathBuf, Status> {
    let valid = block_id.len() >= 4
        && block_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !valid {
        return Err(Status::invalid_argument("Invalid block_id"));
    }

    let mut path = PathBuf::from(data_folder);
    path.push(&block_id[..2]);
    path.push(&block_id[2..4]);
    path.push(block_id);
    Ok(path)
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

fn metadata_endpoints(settings: &InternalSettings, preferred: Option<String>) -> Vec<String> {
    let mut endpoints = Vec::new();

    if let Some(ep) = preferred {
        endpoints.push(ep);
    }

    for ep in &settings.metadata_node_endpoints {
        if !endpoints.iter().any(|existing| existing == ep) {
            endpoints.push(ep.clone());
        }
    }

    if !settings.metadata_node_address.is_empty()
        && !endpoints
            .iter()
            .any(|existing| existing == &settings.metadata_node_address)
    {
        endpoints.push(settings.metadata_node_address.clone());
    }

    endpoints
}

async fn discover_leader_endpoint(settings: &InternalSettings) -> Option<String> {
    for endpoint in metadata_endpoints(settings, None) {
        let mut raft_client = match RaftServiceClient::connect(endpoint.clone()).await {
            Ok(client) => client,
            Err(err) => {
                debug!(%err, %endpoint, "Failed to connect to raft service");
                continue;
            }
        };

        let response = match raft_client
            .get_leader(Request::new(GetLeaderRequest {}))
            .await
        {
            Ok(resp) => {
                let response = resp.into_inner();
                debug!(%endpoint, "Successfully got response from raft service {response:?}");
                response
            }
            Err(err) => {
                debug!(%err, %endpoint, "Failed to get leader from raft service");
                continue;
            }
        };

        if response.known && !response.leader_address.is_empty() {
            info!(%endpoint, leader_address = %response.leader_address, "Discovered leader endpoint");
            return Some(response.leader_address);
        }
        debug!(%endpoint, "Leader address unknown from this endpoint");
    }

    None
}

async fn connect_metadata_client(
    settings: &InternalSettings,
    preferred: Option<String>,
) -> Result<
    MetadataServiceClient<tonic::transport::Channel>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;

    for endpoint in metadata_endpoints(settings, preferred) {
        match MetadataServiceClient::connect(endpoint).await {
            Ok(client) => return Ok(client),
            Err(err) => last_err = Some(Box::new(err)),
        }
    }

    Err(last_err.unwrap_or_else(|| "No metadata endpoints configured".into()))
}

impl Datanode {
    async fn should_sync_block_now(&self, block_id: &str) -> bool {
        let interval_ms = self.settings.write_sync_interval_ms;
        if interval_ms == 0 {
            return true;
        }

        let min_interval = Duration::from_millis(interval_ms);
        let now = Instant::now();
        let mut last_sync = self.block_last_sync.lock().await;
        match last_sync.get(block_id) {
            Some(prev) if now.duration_since(*prev) < min_interval => false,
            _ => {
                last_sync.insert(block_id.to_string(), now);
                true
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_any(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<common::AuthClaims, Status> {
        common::authorize_metadata(metadata, &self.settings.jwt_secret)
    }

    #[allow(clippy::result_large_err)]
    fn authorize_master(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<common::AuthClaims, Status> {
        let claims = self.authorize_any(metadata)?;
        common::require_role(&claims, common::ROLE_MASTER)?;
        Ok(claims)
    }

    async fn replicate_single_block_to_dst(
        settings: Arc<InternalSettings>,
        dst_addr: String,
        block_id: String,
    ) -> Result<(), Status> {
        let file_path = block_path(&settings.data_folder, &block_id)?;

        let mut source_file = File::open(file_path).await.map_err(|err| {
            error!(%err, block_id = %block_id, "Failed to open source block for replication");
            Status::not_found(err.to_string())
        })?;

        let mut client = DataNodeServiceClient::connect(dst_addr.clone())
            .await
            .map_err(|err| {
                error!(%err, dst = %dst_addr, "Failed to connect to destination datanode");
                Status::unavailable(err.to_string())
            })?;

        let (tx, rx) = mpsc::channel(settings.mpsc_channel_size as usize);
        let mut write_request = Request::new(ReceiverStream::new(rx));
        let token = common::create_access_token(
            &settings.jwt_secret,
            "datanode-service",
            common::ROLE_DATANODE,
            settings.service_token_exp_seconds,
        )
        .map_err(|err| {
            Status::internal(format!("Could not create datanode service token: {err}"))
        })?;
        common::attach_bearer_token(&mut write_request, &token)?;
        let write_future = client.write_block(write_request);

        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::Metadata(common::dfs::BlockMetadata {
                block_id: block_id.clone(),
                total_size: 0,
                offset: 0, // Replication always writes from block start
            })),
        })
        .await
        .map_err(|err| {
            Status::internal(format!(
                "Failed to send block metadata for {block_id}: {err}"
            ))
        })?;

        let mut hasher = Hasher::new();
        let mut buffer = vec![0u8; settings.read_buffer_size];
        loop {
            let bytes_read = source_file.read(&mut buffer).await.map_err(|err| {
                error!(%err, block_id = %block_id, "Failed to read source block during replication");
                Status::internal(err.to_string())
            })?;

            if bytes_read == 0 {
                break;
            }

            hasher.update(&buffer[..bytes_read]);
            tx.send(WriteBlockRequest {
                request_data: Some(RequestData::ChunkData(buffer[..bytes_read].to_vec())),
            })
            .await
            .map_err(|err| {
                Status::internal(format!("Failed to send chunk data for {block_id}: {err}"))
            })?;
        }

        let checksum = hasher.finalize().to_string();
        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::Checksum(checksum)),
        })
        .await
        .map_err(|err| {
            Status::internal(format!("Failed to send checksum for {block_id}: {err}"))
        })?;

        drop(tx);
        let write_response = write_future.await.map_err(|err| {
            error!(%err, block_id = %block_id, dst = %dst_addr, "Failed to write replicated block");
            Status::internal(format!("Failed to write replicated block: {err}"))
        })?;
        let write_result = write_response.into_inner();
        if !write_result.success {
            return Err(Status::internal(write_result.message));
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl DataNodeService for Datanode {
    type ReadBlockStream = ReceiverStream<Result<ReadBlockResponse, Status>>;

    #[instrument(skip(self, request))]
    async fn write_block(
        &self,
        request: Request<Streaming<WriteBlockRequest>>,
    ) -> Result<Response<WriteBlockResponse>, Status> {
        self.authorize_any(request.metadata())?;
        debug!("Received write_block request");
        let mut hasher = Hasher::new();
        let mut stream = request.into_inner();
        let mut writer: Option<BufWriter<File>> = None;
        let mut bytes_written: i64 = 0;
        let mut block_id_str = String::new();

        while let Some(msg) = stream.message().await? {
            match msg.request_data {
                Some(RequestData::Metadata(metadata)) => {
                    block_id_str = metadata.block_id.clone();
                    let block_file = block_path(&self.settings.data_folder, &block_id_str)?;

                    let write_offset = metadata.offset as i64;
                    debug!(
                        block_id = %block_id_str,
                        offset = write_offset,
                        "Received metadata for block"
                    );

                    let file_path_pathbuf = block_file;
                    if let Some(block_dir) = file_path_pathbuf.parent() {
                        create_dir_all(block_dir).await.map_err(|err| {
                            error!(%err, "Failed to create folder");
                            Status::internal(err.to_string())
                        })?;
                    }

                    // Never truncate here: iozone and similar workloads can issue
                    // partial rewrites at offset 0, and truncation would drop the
                    // untouched tail of the block.
                    let mut f = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(file_path_pathbuf)
                        .await
                        .map_err(|err| {
                            error!(%err, "Failed to create file");
                            Status::internal(err.to_string())
                        })?;

                    // Position cursor exactly at requested in-block offset.
                    use tokio::io::AsyncSeekExt;
                    f.seek(std::io::SeekFrom::Start(write_offset as u64))
                        .await
                        .map_err(|err| {
                            error!(%err, "Failed to seek to offset {}", write_offset);
                            Status::internal(err.to_string())
                        })?;

                    writer = Some(BufWriter::with_capacity(self.settings.write_buffer_size, f));
                }
                Some(RequestData::ChunkData(chunkdata)) => {
                    if let Some(ref mut w) = writer {
                        w.write_all(&chunkdata).await.map_err(|err| {
                            error!(%err, "Failed to write chunk data");
                            Status::internal(err.to_string())
                        })?;
                        hasher.update(&chunkdata);
                        bytes_written += chunkdata.len() as i64;
                    } else {
                        error!("Received chunk data before metadata");
                        return Err(Status::failed_precondition("Metadata missing"));
                    }
                }
                Some(RequestData::Checksum(expected_checksum)) => {
                    let calculated_checksum = hasher.finalize().to_string();
                    if calculated_checksum != expected_checksum {
                        error!(
                            calculated = calculated_checksum,
                            expected = expected_checksum,
                            "Checksum mismatch"
                        );
                        return Err(Status::invalid_argument("Checksum mismatch"));
                    }
                    debug!(block_id = %block_id_str, "Checksum verified successfully");
                    break;
                }
                None => {
                    warn!("Received empty message in stream");
                    return Err(Status::invalid_argument("The message is empty"));
                }
            }
        }
        if let Some(mut w) = writer {
            w.flush().await.map_err(|err| {
                error!(%err, "Failed to flush buffer");
                Status::internal(err.to_string())
            })?;

            let policy = self.settings.write_sync_policy.to_ascii_lowercase();
            let should_sync = self.should_sync_block_now(&block_id_str).await;
            match policy.as_str() {
                "none" => {}
                "data" if should_sync => {
                    w.get_mut().sync_data().await.map_err(|err| {
                        error!(%err, block_id = %block_id_str, "Failed to sync file data");
                        Status::internal(err.to_string())
                    })?;
                }
                "data" => {}
                "all" if should_sync => {
                    w.get_mut().sync_all().await.map_err(|err| {
                        error!(%err, block_id = %block_id_str, "Failed to sync file");
                        Status::internal(err.to_string())
                    })?;
                }
                "all" => {}
                other => {
                    warn!(policy = %other, "Unknown write_sync_policy, defaulting to sync_all");
                    if should_sync {
                        w.get_mut().sync_all().await.map_err(|err| {
                            error!(%err, block_id = %block_id_str, "Failed to sync file");
                            Status::internal(err.to_string())
                        })?;
                    }
                }
            }
        }
        debug!(block_id = %block_id_str, bytes_written, "Successfully wrote block");
        let synced = match self
            .settings
            .write_sync_policy
            .to_ascii_lowercase()
            .as_str()
        {
            "none" => false,
            "data" => true, // With interval_ms=0, we always sync
            "all" => true,
            _ => true,
        };
        Ok(Response::new(WriteBlockResponse {
            success: true,
            message: "Everything went well".to_string(),
            bytes_written,
            synced,
        }))
    }

    #[instrument(skip(self, request))]
    async fn read_block(
        &self,
        request: Request<ReadBlockRequest>,
    ) -> Result<Response<Self::ReadBlockStream>, Status> {
        self.authorize_any(request.metadata())?;
        let block_id = request.into_inner().block_id;
        let mut hasher = Hasher::new();
        debug!(block_id = %block_id, "Received read_block request");
        let file_path_pathbuf = block_path(&self.settings.data_folder, &block_id)?;
        let (tx, rx) = mpsc::channel(self.settings.mpsc_channel_size as usize);
        let read_buffer_size = self.settings.read_buffer_size;

        tokio::spawn(async move {
            let file_res = File::open(file_path_pathbuf).await;
            let mut file = match file_res {
                Ok(f) => f,
                Err(e) => {
                    error!(%e, block_id = %block_id, "Failed to open file for reading");
                    let _ = tx.send(Err(Status::not_found(e.to_string()))).await;
                    return;
                }
            };

            let mut buffer = vec![0u8; read_buffer_size];
            loop {
                match file.read(&mut buffer).await {
                    Ok(0) => {
                        debug!(block_id = %block_id, "Finished reading file");
                        let block_checksum = hasher.finalize().to_string();
                        let response = ReadBlockResponse {
                            response_data: Some(ResponseData::Checksum(block_checksum.clone())),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            warn!(block_id = %block_id, "Receiver dropped, stopping read");
                            return;
                        }
                        debug!(block_id = %block_id, checksum = %block_checksum, "Calculated checksum");
                        break;
                    }
                    Ok(n) => {
                        hasher.update(&buffer[..n]);
                        let response = ReadBlockResponse {
                            response_data: Some(ResponseData::ChunkData(buffer[..n].to_vec())),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            warn!(block_id = %block_id, "Receiver dropped, stopping read");
                            return;
                        }
                        continue;
                    }
                    Err(err) => {
                        error!(%err, block_id = %block_id, "Error reading file");
                        let _ = tx.send(Err(Status::internal(err.to_string()))).await;
                        break;
                    }
                }
            }
        });
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(output_stream))
    }

    #[instrument(skip(self, request))]
    async fn delete_block(
        &self,
        request: Request<DeleteBlockRequest>,
    ) -> Result<Response<DeleteBlockResponse>, Status> {
        self.authorize_master(request.metadata())?;
        let block_id = request.into_inner().block_id;
        debug!(block_id = %block_id, "Received delete_block request");

        let file_path_pathbuf = block_path(&self.settings.data_folder, &block_id)?;
        let _ = fs::remove_file(&file_path_pathbuf)
        .await
        .map_err(|err| {
            warn!(block_id = %block_id, path = ?file_path_pathbuf, "Could not delete block file: {}", err);
            Status::not_found(format!("Could not delete file {err}"))
        });
        if let Some(parent_dir) = file_path_pathbuf.parent() {
            if let Ok(mut entries) = fs::read_dir(parent_dir).await {
                if entries.next_entry().await.ok().flatten().is_none() {
                    let _ = fs::remove_dir(parent_dir).await;
                }
            }
        }
        debug!(block_id = %block_id, "Block deleted successfully");
        Ok(Response::new(DeleteBlockResponse {
            delete_result: Some(common::dfs::delete_block_response::DeleteResult::Success(
                true,
            )),
        }))
    }

    #[allow(clippy::result_large_err)]
    #[instrument(skip(self, request))]
    async fn configure_node(
        &self,
        request: Request<ConfigureNodeRequest>,
    ) -> Result<Response<ConfigureNodeResponse>, Status> {
        self.authorize_master(request.metadata())?;
        let req = request.into_inner();
        let new_node_id = req.node_id;
        info!(node_id = new_node_id, "Received configure_node request");
        *self.node_id.lock().await = new_node_id as u16;
        info!(node_id = new_node_id, "Node configured successfully");
        Ok(Response::new(ConfigureNodeResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    #[allow(clippy::result_large_err)]
    async fn replicate_blocks(
        &self,
        request: Request<ReplicateBlocksRequest>,
    ) -> Result<Response<ReplicateBlocksResponse>, Status> {
        self.authorize_master(request.metadata())?;
        let block_locs = request.into_inner().block_locations;
        let max_parallel_blocks = self.settings.replication_max_parallel_blocks.max(1);
        let sem = Arc::new(Semaphore::new(max_parallel_blocks));
        let mut set = JoinSet::new();
        for (node_to_replicate_to, blocks) in block_locs {
            for block_id in blocks.block_ids {
                let permit = sem.clone().acquire_owned().await.unwrap();
                let dst_clone = node_to_replicate_to.clone();
                let settings = Arc::clone(&self.settings);
                set.spawn(async move {
                    let _permit = permit;
                    Datanode::replicate_single_block_to_dst(settings, dst_clone, block_id).await
                });
            }
        }

        while let Some(join_result) = set.join_next().await {
            match join_result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => {
                    error!(%err, "Replication task panicked or was cancelled");
                    return Err(Status::internal(err.to_string()));
                }
            }
        }

        Ok(Response::new(ReplicateBlocksResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Arc::new(load_config());

    let _guard = common::init_tracing("datanode", &settings.log_level);
    info!("Starting DataNode with settings: {:?}", settings);
    debug!("Creating directory to store files");
    create_dir_all(&settings.data_folder).await?;
    let address: std::net::SocketAddr = settings.listen_address.parse()?;
    // Address the master (and peers) use to reach this datanode. The bind address
    // ([::] / 0.0.0.0) is only valid for listening, never for connecting back, so
    // advertise the configured routable address when one is provided.
    let advertise_address = if settings.advertise_address.is_empty() {
        address.to_string()
    } else {
        settings.advertise_address.clone()
    };
    info!(
        "Advertising datanode address to master as {}",
        advertise_address
    );
    let datanode = Datanode {
        node_id: Arc::new(Mutex::new(0)),
        settings: Arc::clone(&settings),
        block_last_sync: Arc::new(Mutex::new(std::collections::HashMap::new())),
    };
    let node_id_arc = Arc::clone(&datanode.node_id);
    let service = DataNodeServiceServer::new(datanode);

    info!("Starting DataNode server on {}", address);

    tokio::spawn(async move {
        info!("HeartBeat sender starting - will register with master");

        // Retry connection with exponential backoff
        let mut retry_delay =
            tokio::time::Duration::from_millis(settings.heartbeat_retry_initial_ms.max(1));
        let max_retry_delay =
            tokio::time::Duration::from_millis(settings.heartbeat_retry_max_ms.max(1));

        let mut client = loop {
            let preferred = discover_leader_endpoint(&settings).await;

            if preferred.is_none() {
                warn!(
                    "No leader found in cluster. \
                     Master may be in election or not started yet. Retrying in {:?}...",
                    retry_delay
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                continue;
            }

            match connect_metadata_client(&settings, preferred.clone()).await {
                Ok(c) => {
                    info!("Connected to master server at {:?}", preferred);
                    break c;
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to master at {:?}: {}. Retrying in {:?}...",
                        preferred, e, retry_delay
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                }
            }
        };

        loop {
            let current_node_id = *node_id_arc.lock().await as u32;

            debug!("Sending heartbeat");
            let token = match common::create_access_token(
                &settings.jwt_secret,
                &format!("datanode-{current_node_id}"),
                common::ROLE_DATANODE,
                settings.service_token_exp_seconds,
            ) {
                Ok(token) => token,
                Err(e) => {
                    error!("Could not create heartbeat token: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        settings.heartbeat_nack_retry_ms.max(1),
                    ))
                    .await;
                    continue;
                }
            };

            let mut heartbeat_request = Request::new(HeartbeatRequest {
                cpu_usage: settings.heartbeat_cpu_usage,
                node_id: current_node_id,
                free_space: settings.heartbeat_free_space,
                used_space: settings.heartbeat_used_space,
                addr: advertise_address.clone(),
            });
            if let Err(e) = common::attach_bearer_token(&mut heartbeat_request, &token) {
                error!("Could not attach heartbeat token metadata: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    settings.heartbeat_nack_retry_ms.max(1),
                ))
                .await;
                continue;
            }

            let result = client.heartbeat(heartbeat_request).await;

            match result {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.acknowledgement {
                        // If we sent node_id=0 (not registered), update with assigned ID
                        if current_node_id == 0 && resp.assigned_node_id > 0 {
                            let new_id = resp.assigned_node_id as u16;
                            *node_id_arc.lock().await = new_id;
                            info!(node_id = new_id, "Registered with master, assigned node_id");
                        } else {
                            debug!(node_id = current_node_id, "Heartbeat acknowledged");
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            settings.heartbeat_interval_ms.max(1),
                        ))
                        .await;
                    } else {
                        error!("Heartbeat not acknowledged by master");
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            settings.heartbeat_nack_retry_ms.max(1),
                        ))
                        .await;
                    }
                }
                Err(e) => {
                    error!("Heartbeat failed: {}. Attempting to reconnect...", e);

                    // Try to reconnect
                    retry_delay = tokio::time::Duration::from_millis(
                        settings.heartbeat_retry_initial_ms.max(1),
                    );
                    loop {
                        // Always try to discover the current leader first
                        let mut preferred = if e.code() == tonic::Code::FailedPrecondition {
                            parse_leader_addr_from_message(e.message())
                        } else {
                            None
                        };

                        // If we couldn't get leader from error message, discover it
                        if preferred.is_none() {
                            info!("No leader hint in error, discovering leader from cluster...");
                            preferred = discover_leader_endpoint(&settings).await;
                        }

                        // If still no leader found, wait before retrying discovery
                        if preferred.is_none() {
                            warn!(
                                "No leader found in cluster. \
                                 Master may be in election. Retrying in {:?}...",
                                retry_delay
                            );
                            tokio::time::sleep(retry_delay).await;
                            retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                            continue;
                        }

                        // Try to connect to the discovered leader
                        match connect_metadata_client(&settings, preferred.clone()).await {
                            Ok(c) => {
                                info!("Reconnected to master server at {:?}", preferred);
                                client = c;
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to reconnect to master at {:?}: {}. Retrying in {:?}...",
                                    preferred, e, retry_delay
                                );
                                tokio::time::sleep(retry_delay).await;
                                retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                                // Clear preferred to force rediscovery on next iteration
                                preferred = None;
                            }
                        }
                    }
                }
            }
        }
    });

    Server::builder()
        .concurrency_limit_per_connection(512)
        .initial_stream_window_size(Some(2 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024))
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        .tcp_nodelay(true)
        .add_service(service)
        .serve(address)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
