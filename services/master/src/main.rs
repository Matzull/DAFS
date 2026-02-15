mod models;
use crate::models::{BlockMetadata, DatanodeInfo, FileMetadata, FileStatus, MasterState};
use common::dfs::metadata_service_server::{MetadataService, MetadataServiceServer};
use common::dfs::{
    data_node_service_client, BlockAssignment, CreateFileRequest, CreateFileResponse,
    DeleteBlockRequest, DeleteFileRequest, DeleteFileResponse, FileInfo, GetFileRequest,
    GetFileResponse, HeartbeatRequest, HeartbeatResponse, ListDirectoryRequest,
    ListDirectoryResponse, RenameFileRequest, RenameFileResponse, SetFileStatusRequest,
    SetFileStatusResponse,
};
// use tokio::net::unix::pipe::Receiver;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{self},
};

use chrono::{DateTime, Utc};
use clap::Parser;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{self as tokio_time, Duration};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

const DEFAULT_BLOCK_SIZE: i64 = 64 * 1024;
const DEFAULT_REPLICATION: i32 = 2; //Minimum number of nodes that must contain a block
const DEFAULT_LISTEN_ADDRESS: &str = "[::1]:9090";
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_AUDIT_SERVICE_ADDR: &str = "http://[::1]:9090";
const DEFAULT_WAL_PATH: &str = "services/master/master.wal";
const DEFAULT_WAL_BATCH_SIZE: usize = 128;
const DEFAULT_WAL_FLUSH_INTERVAL_MS: u64 = 250;

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct InternalSettings {
    pub listen_address: String,
    pub block_size: i64,
    pub replication_factor: i32,
    pub log_level: String,
    pub audit_service_addr: String,
    pub wal_path: String,
    pub wal_batch_size: usize,
    pub wal_flush_interval_ms: u64,
}

pub fn load_config() -> InternalSettings {
    let cli = Cli::parse();

    let figment = Figment::new()
        .merge(Serialized::defaults(InternalSettings {
            listen_address: DEFAULT_LISTEN_ADDRESS.to_string(),
            block_size: DEFAULT_BLOCK_SIZE,
            replication_factor: DEFAULT_REPLICATION,
            log_level: DEFAULT_LOG_LEVEL.to_string(),
            audit_service_addr: DEFAULT_AUDIT_SERVICE_ADDR.to_string(),
            wal_path: DEFAULT_WAL_PATH.to_string(),
            wal_batch_size: DEFAULT_WAL_BATCH_SIZE,
            wal_flush_interval_ms: DEFAULT_WAL_FLUSH_INTERVAL_MS,
        }))
        .merge(Toml::file(&cli.config_path))
        .merge(Serialized::globals(cli));

    figment.extract().expect("Error loading configuration")
}

/// DAFS MetadataNode Service
#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(name = "dafs-master")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Server listen address
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    listen_address: Option<String>,

    /// Block size (in bytes)
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    block_size: Option<i64>,

    /// Replication factor
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    replication_factor: Option<i32>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,

    /// gRPC address for Audit Service
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    audit_service_addr: Option<String>,

    /// Path to WAL file
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_path: Option<String>,

    /// Max entries buffered before WAL flush
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_batch_size: Option<usize>,

    /// WAL flush interval in milliseconds
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_flush_interval_ms: Option<u64>,

    #[arg(short, long, default_value = "config.toml")]
    #[serde(skip)]
    pub config_path: String,
}

#[derive(Serialize, Clone)]
pub struct AuditLog {
    pub timestamp: DateTime<Utc>, //In epoch_millis but serialized
    pub action: String,
    pub resource: Option<String>,
    pub component: String,
    pub success: bool,
    pub status: String,
    pub user: String,
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalRecord {
    pub schema_version: u32,
    pub op_id: String,
    pub timestamp_ms: i64,
    pub operation: WalOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
enum WalOperation {
    CreateFile { file: FileMetadata },
    DeleteFile { path: String },
    RenameFile { old_path: String, new_path: String },
    SetFileStatus { path: String, status: String },
    RegisterDatanode { datanode: DatanodeInfo },
}

#[derive(Debug)]
pub struct Metadatanode {
    pub state: Arc<MasterState>,
    pub garbage_collector_tx: mpsc::Sender<Vec<BlockMetadata>>,
    pub audit_tx: mpsc::Sender<AuditLog>,
    wal_tx: mpsc::Sender<WalRecord>,
    pub settings: Arc<InternalSettings>,
    pub next_node_id: AtomicU16,
}

impl Clone for Metadatanode {
    fn clone(&self) -> Self {
        Metadatanode {
            state: Arc::clone(&self.state),
            garbage_collector_tx: self.garbage_collector_tx.clone(),
            audit_tx: self.audit_tx.clone(),
            wal_tx: self.wal_tx.clone(),
            settings: Arc::clone(&self.settings),
            next_node_id: AtomicU16::new(self.next_node_id.load(Ordering::SeqCst)),
        }
    }
}

impl Metadatanode {
    #[instrument(skip(state, settings))]
    async fn _create_file(
        &self,
        state: &Arc<MasterState>,
        settings: &Arc<InternalSettings>,
        file_path: String,
        file_size: i64,
    ) -> Result<FileMetadata, String> {
        if file_size < 0 {
            error!("File size cannot be negative");
            return Err("File size cannot be negative".to_string());
        }
        let file_path_pathbuf = PathBuf::from(file_path.clone());
        let full_path = file_path_pathbuf.to_string_lossy().to_string();
        let mut files_lock = state.files.write().await;

        if files_lock.contains_key(&full_path) {
            error!("File {} already exists", full_path);
            return Err(format!("File {full_path} already exists"));
        }
        let num_blocks = ((file_size + settings.block_size - 1) / settings.block_size)
            .try_into()
            .unwrap();
        let file_id = Uuid::new_v4().to_string();
        let datanodes_lock = state.available_datanodes.read().await;
        let datanodes: Vec<u16> = datanodes_lock.keys().cloned().collect();
        drop(datanodes_lock);
        if datanodes.is_empty() {
            error!("No datanodes found");
            return Err("No datanodes found".to_string());
        }

        info!(
            "Creating file ID: {} with {} blocks for {}",
            file_id, num_blocks, file_path
        );

        let mut blocks = Vec::<BlockMetadata>::new();
        for block_idx in 0..num_blocks {
            let block_id = Uuid::new_v4().to_string();
            let mut block_datanodes = vec![];
            for datanode_id in 0..min(settings.replication_factor, datanodes.len() as i32) {
                block_datanodes
                    .push(datanodes[(block_idx + datanode_id as usize) % datanodes.len()]);
            }
            let current_block_size = if (block_idx as i64 + 1) * settings.block_size < file_size {
                settings.block_size
            } else {
                file_size - (block_idx as i64) * settings.block_size
            };
            blocks.push(BlockMetadata {
                id: block_id,
                index: block_idx as i32,
                size: current_block_size,
                locations: block_datanodes.clone(),
                checksum: "TO_IMPLEMENT".to_string(),
            });
        }
        let now_miliseconds = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;

        let file = FileMetadata {
            id: file_id,
            name: file_path.split('/').next_back().unwrap_or("").to_string(),
            path: file_path.clone(),
            size: file_size,
            owner: "SYSTEM".to_string(),
            created_at: now_miliseconds,
            last_modified_at: now_miliseconds,
            file_status: FileStatus::Uploading,
            blocks,
        };

        files_lock.insert(full_path, file.clone());
        info!("File metadata created successfully for {}", file.id);
        return Ok(file);
    }
}

#[tonic::async_trait]
impl MetadataService for Metadatanode {
    #[instrument(skip(self))]
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        info!("Received create_file request");
        let file_request = request.into_inner();
        let file_metadata = self
            ._create_file(
                &self.state,
                &self.settings,
                file_request.path.clone(),
                file_request.size,
            )
            .await
            .map_err(Status::internal)?;
        let datanodes = self.state.available_datanodes.read().await;
        let block_assignments = file_metadata
            .blocks
            .iter()
            .map(|block| BlockAssignment {
                block_id: block.id.clone(),
                node_address: datanodes.get(&block.locations[0]).unwrap().addr.to_string(), //TODO: Use real nodes
                block_size: block.size,
                order: block.index as i64, //TODO: check var size i64 vs i32
            })
            .collect();
        self.audit(
            "Creating file",
            Some(file_request.path.clone()),
            true,
            "SUCCESS",
            json!({
                "size": file_request.size,
            }),
        )
        .await;
        self.wal(WalOperation::CreateFile {
            file: file_metadata.clone(),
        })
        .await;
        Ok(Response::new(CreateFileResponse {
            file_id: file_metadata.id,
            blocks: block_assignments,
        }))
    }

    #[instrument(skip(self))]
    async fn get_file_metadata(
        &self,
        request: Request<GetFileRequest>,
    ) -> Result<Response<GetFileResponse>, Status> {
        info!("Received get_file_metadata request");
        let file_path = request.into_inner().path;
        let files_lock = self.state.files.read().await;
        let file_metadata = match files_lock.get(&file_path) {
            Some(filemetadata) => filemetadata,
            None => return Err(Status::not_found("File not found")),
        };
        let datanodes = self.state.available_datanodes.read().await;
        let block_assignments = file_metadata
            .blocks
            .iter()
            .map(|block| BlockAssignment {
                block_id: block.id.clone(),
                node_address: datanodes.get(&block.locations[0]).unwrap().addr.to_string(), //TODO: Use real nodes
                block_size: block.size,
                order: block.index as i64, //TODO: check var size i64 vs i32
            })
            .collect();
        self.audit(
            "Getting file",
            Some(file_path.to_string()),
            true,
            "SUCCESS",
            json!({}),
        )
        .await;
        Ok(Response::new(GetFileResponse {
            file_id: file_metadata.id.clone(),
            path: file_metadata.path.clone(),
            size: file_metadata.size,
            owner: file_metadata.owner.clone(),
            created_at: file_metadata.created_at,
            last_modified_at: file_metadata.last_modified_at,
            // checksum: file_metadata.checksum.clone(),
            file_status: file_metadata.file_status.to_string(),
            blocks: block_assignments,
        }))
    }

    #[instrument(skip(self))]
    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        info!("Received delete_file request");
        let file_path = request.into_inner().path;
        let mut files = self.state.files.write().await;
        if let Some(filemetadata) = files.remove(&file_path) {
            let block_count = filemetadata.blocks.len();
            let to_delete = filemetadata.blocks;
            self.garbage_collector_tx.send(to_delete).await.ok();
            info!(
                file_path,
                block_count, "File deleted, blocks queued for garbage collection"
            );
            self.audit(
                "Deleting file",
                Some(file_path.to_string()),
                true,
                "SUCCESS",
                json!({
                    "queued_blocks": block_count,
                }),
            )
            .await;
            self.wal(WalOperation::DeleteFile {
                path: file_path.to_string(),
            })
            .await;
            Ok(Response::new(DeleteFileResponse {
                success: true,
                error_message: "".to_string(),
            }))
        } else {
            warn!(file_path, "Attempted to delete non-existent file");
            return Err(Status::not_found("File not found"));
        }
    }

    #[instrument(skip(self))]
    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        info!("Received list_directory request");
        let dir_path = request.into_inner().path;
        let files_lock = self.state.files.read().await;
        let files: Vec<FileInfo> = files_lock
            .iter()
            .filter(|(path, _)| path.starts_with(&dir_path))
            .map(|(_, v)| FileInfo {
                filename: v.name.clone(),
                is_directory: false,
                size: v.size,
                owner: v.owner.clone(),
                created_at: v.created_at,
                last_modified_at: v.last_modified_at,
                // checksum: v.checksum.clone(),
                file_status: v.file_status.to_string(),
            })
            .collect();
        info!(
            dir_path,
            file_count = files.len(),
            "Directory listing completed"
        );
        self.audit(
            "Listing dir",
            Some(dir_path.to_string()),
            true,
            "SUCCESS",
            json!({
                "file_count": files.len(),
            }),
        )
        .await;
        Ok(Response::new(ListDirectoryResponse { files }))
    }

    #[instrument(skip(self))]
    async fn rename_file(
        &self,
        request: Request<RenameFileRequest>,
    ) -> Result<Response<RenameFileResponse>, Status> {
        info!("Received rename_file request");
        let rename_req = request.into_inner();
        let mut files_lock = self.state.files.write().await;
        if files_lock.contains_key(&rename_req.new_path) {
            warn!(
                new_path = rename_req.new_path,
                "Rename target already exists"
            );
            return Err(Status::already_exists(format!(
                "File {} already exists",
                rename_req.new_path
            )));
        }
        if let Some(mut file_metadata) = files_lock.remove(&rename_req.old_path) {
            file_metadata.name = rename_req.new_path.clone();
            info!(
                old_path = rename_req.old_path,
                new_path = rename_req.new_path,
                "File renamed successfully"
            );
            files_lock.insert(rename_req.new_path.clone(), file_metadata);
        } else {
            warn!(old_path = rename_req.old_path, "File not found for rename");
            return Err(Status::not_found("Couldnt rename file"));
        }
        self.audit(
            "Renamed file",
            Some(rename_req.old_path.to_string()),
            true,
            "SUCCESS",
            json!({
                "new_path": rename_req.new_path,
            }),
        )
        .await;
        self.wal(WalOperation::RenameFile {
            old_path: rename_req.old_path.to_string(),
            new_path: rename_req.new_path.to_string(),
        })
        .await;

        Ok(Response::new(RenameFileResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    #[instrument(skip(self))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let node_request = request.into_inner();
        let remote_addr = node_request.addr.clone();
        let mut datanodes_lock = self.state.available_datanodes.write().await;

        // If node_id is 0, this is a new datanode registering
        if node_request.node_id == 0 {
            let new_id = self.next_node_id.fetch_add(1, Ordering::SeqCst);
            info!(assigned_node_id = new_id, "New datanode registered");

            // Extract IP from remote_addr and use standard datanode port
            let datanode_addr = format!("http://{remote_addr}");

            datanodes_lock.insert(
                new_id,
                DatanodeInfo {
                    id: new_id,
                    addr: datanode_addr.clone(),
                    last_heartbeat: time::SystemTime::now()
                        .duration_since(time::UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_millis() as i64,
                },
            );
            self.audit(
                "New datanode registration",
                Some(format!("node:{new_id}")),
                true,
                "SUCCESS",
                json!({
                    "assigned_node_id": new_id,
                    "datanode_addr": datanode_addr,
                    "cpu_usage": node_request.cpu_usage,
                    "free_space": node_request.free_space,
                    "used_space": node_request.used_space,
                }),
            )
            .await;
            self.wal(WalOperation::RegisterDatanode {
                datanode: DatanodeInfo {
                    id: new_id,
                    addr: datanode_addr,
                    last_heartbeat: time::SystemTime::now()
                        .duration_since(time::UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_millis() as i64,
                },
            })
            .await;

            return Ok(Response::new(HeartbeatResponse {
                acknowledgement: true,
                assigned_node_id: new_id as u32,
            }));
        }

        // Existing datanode heartbeat
        debug!(
            node_id = node_request.node_id,
            cpu_usage = node_request.cpu_usage,
            free_space = node_request.free_space,
            used_space = node_request.used_space,
            "Received heartbeat from node"
        );

        let mut audit_success = true;
        let mut audit_status = "SUCCESS";
        if let Some(datanode) = datanodes_lock.get_mut(&(node_request.node_id as u16)) {
            datanode.last_heartbeat = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as i64;
        } else {
            audit_success = false;
            audit_status = "UNKNOWN_NODE";
            warn!(
                node_id = node_request.node_id,
                "Heartbeat from unknown node"
            );
        }
        self.audit(
            "Heartbeat",
            Some(format!("node:{}", node_request.node_id)),
            audit_success,
            audit_status,
            json!({
                "cpu_usage": node_request.cpu_usage,
                "free_space": node_request.free_space,
                "used_space": node_request.used_space,
                "addr": node_request.addr,
            }),
        )
        .await;
        Ok(Response::new(HeartbeatResponse {
            acknowledgement: true,
            assigned_node_id: node_request.node_id,
        }))
    }

    async fn set_file_status(
        &self,
        request: Request<SetFileStatusRequest>,
    ) -> Result<Response<SetFileStatusResponse>, Status> {
        let set_file_request = request.into_inner();
        let file_path = set_file_request.path;
        let new_status = set_file_request.status;
        let mut files_lock = self.state.files.write().await;
        if let Some(file_metadata) = files_lock.get_mut(&file_path) {
            file_metadata.file_status = FileStatus::str_to_tok(&new_status);
        } else {
            return Err(Status::not_found("File not found"));
        }
        self.wal(WalOperation::SetFileStatus {
            path: file_path,
            status: new_status,
        })
        .await;
        Ok(Response::new(SetFileStatusResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }
}

impl Metadatanode {
    #[instrument(skip(self))]
    async fn notify_datanode_to_delete(
        &self,
        block_id: &str,
        datanode: String,
    ) -> Result<bool, String> {
        info!(block_id, datanode, "Requesting datanode to delete block");
        let mut client = match data_node_service_client::DataNodeServiceClient::connect(
            datanode.clone(),
        )
        .await
        {
            Ok(c) => c,
            Err(err) => {
                error!(block_id, datanode, "Could not connect to datanode: {}", err);
                self.audit(
                    "Delete block",
                    Some(block_id.to_string()),
                    false,
                    "CONNECTION_ERROR",
                    json!({
                        "datanode": datanode,
                        "error": err.to_string(),
                    }),
                )
                .await;
                return Err(format!("Could not connect to datanode: {err}"));
            }
        };
        match client
            .delete_block(DeleteBlockRequest {
                block_id: block_id.to_string(),
            })
            .await
        {
            Ok(_) => {
                info!(block_id, datanode, "Block deletion confirmed by datanode");
                self.audit(
                    "Delete block",
                    Some(block_id.to_string()),
                    true,
                    "SUCCESS",
                    json!({
                        "datanode": datanode,
                    }),
                )
                .await;
                Ok(true)
            }
            Err(err) => {
                error!(
                    block_id,
                    datanode, "Failed to delete block on datanode: {}", err
                );
                self.audit(
                    "Delete block",
                    Some(block_id.to_string()),
                    false,
                    "DELETE_ERROR",
                    json!({
                        "datanode": datanode,
                        "error": err.to_string(),
                    }),
                )
                .await;
                Err(format!("Could not delete block on datanode: {err}"))
            }
        }
    }

    async fn audit(
        &self,
        action: impl Into<String>,
        resource: Option<String>,
        success: bool,
        status: impl Into<String>,
        details: Value,
    ) {
        let now = Utc::now();
        self.audit_tx
            .send(AuditLog {
                timestamp: now,
                action: action.into(),
                resource,
                component: "MASTER".to_string(),
                success,
                status: status.into(),
                user: "1".to_string(),
                details,
            })
            .await
            .ok();
    }

    async fn wal(&self, operation: WalOperation) {
        let record = WalRecord {
            schema_version: 1,
            op_id: Uuid::new_v4().to_string(),
            timestamp_ms: Utc::now().timestamp_millis(),
            operation,
        };

        if let Err(err) = self.wal_tx.send(record).await {
            error!("Could not enqueue WAL record: {}", err);
        }
    }
}

fn garbage_collector(
    mut garbage_collector_rx: mpsc::Receiver<Vec<BlockMetadata>>,
    master_ref: Arc<Metadatanode>,
) {
    tokio::spawn(async move {
        info!("Garbage Collector started");
        while let Some(blocks) = garbage_collector_rx.recv().await {
            for block in blocks {
                for location in block.locations {
                    let datanodes = master_ref.state.available_datanodes.read().await;

                    let addr = match datanodes.get(&location) {
                        Some(a) => a.addr.clone(),
                        None => {
                            warn!(
                                "DataNode {} not available to delete block {}",
                                location, block.id
                            );
                            continue;
                        }
                    };
                    drop(datanodes);
                    if let Err(e) = master_ref.notify_datanode_to_delete(&block.id, addr).await {
                        error!("Error deleting block {} in {}: {}", block.id, location, e);
                    }
                }
            }
        }
    });
}

fn log_sender(mut audit_rx: mpsc::Receiver<AuditLog>) {
    dotenvy::from_path(std::path::Path::new("services/master/.env")).ok();
    let es_url = env::var("ELASTIC_URL").expect("URL not defined");
    let es_user = env::var("ELASTIC_USER").expect("User not defined");
    let es_pass = env::var("ELASTIC_PASSWORD").expect("Pass not defined");

    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("Failed to build client");
        info!(
            "Audit Logger started, sending logs to ElasticSearch at {}",
            es_url
        );

        while let Some(log) = audit_rx.recv().await {
            info!("Sending log");
            let res = client
                .post(&es_url)
                .basic_auth(&es_user, Some(&es_pass))
                .json(&log)
                .send()
                .await;

            match res {
                Ok(response) => info!("Respuesta Elastic: {:?}", response.status()),
                Err(e) => error!("Error enviando log: {:?}", e),
            }
        }
    });
}

async fn flush_wal_buffer(
    file: &mut tokio::fs::File,
    buffer: &mut Vec<WalRecord>,
) -> Result<(), std::io::Error> {
    if buffer.is_empty() {
        return Ok(());
    }

    let mut payload = String::new();
    for record in buffer.iter() {
        payload.push_str(&serde_json::to_string(record).unwrap_or_else(|_| "{}".to_string()));
        payload.push('\n');
    }

    tokio::io::AsyncWriteExt::write_all(file, payload.as_bytes()).await?;
    tokio::io::AsyncWriteExt::flush(file).await?;
    buffer.clear();
    Ok(())
}

fn wal_writer(
    mut wal_rx: mpsc::Receiver<WalRecord>,
    wal_path: String,
    batch_size: usize,
    flush_interval_ms: u64,
) {
    tokio::spawn(async move {
        if let Some(parent) = Path::new(&wal_path).parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(err) = tokio::fs::create_dir_all(parent).await {
                    error!("Could not create WAL directory {}: {}", parent.display(), err);
                    return;
                }
            }
        }

        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .await
        {
            Ok(f) => f,
            Err(err) => {
                error!("Could not open WAL file {}: {}", wal_path, err);
                return;
            }
        };

        let mut buffer: Vec<WalRecord> = Vec::with_capacity(batch_size.max(1));
        let mut ticker = tokio_time::interval(Duration::from_millis(flush_interval_ms.max(1)));
        info!(
            "WAL writer started at {} (batch_size={}, flush_interval_ms={})",
            wal_path, batch_size, flush_interval_ms
        );

        loop {
            tokio::select! {
                maybe_record = wal_rx.recv() => {
                    match maybe_record {
                        Some(record) => {
                            buffer.push(record);
                            if buffer.len() >= batch_size.max(1) {
                                if let Err(err) = flush_wal_buffer(&mut file, &mut buffer).await {
                                    error!("Could not flush WAL buffer: {}", err);
                                }
                            }
                        }
                        None => {
                            if let Err(err) = flush_wal_buffer(&mut file, &mut buffer).await {
                                error!("Could not flush WAL on shutdown: {}", err);
                            }
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if let Err(err) = flush_wal_buffer(&mut file, &mut buffer).await {
                        error!("Could not flush WAL by interval: {}", err);
                    }
                }
            }
        }
    });
}

fn replay_wal(
    wal_path: &str,
) -> Result<(HashMap<String, FileMetadata>, HashMap<u16, DatanodeInfo>, u16), std::io::Error> {
    let mut files = HashMap::<String, FileMetadata>::new();
    let mut datanodes = HashMap::<u16, DatanodeInfo>::new();
    let mut max_node_id = 0u16;

    if !Path::new(wal_path).exists() {
        return Ok((files, datanodes, 1));
    }

    let file = std::fs::File::open(wal_path)?;
    let reader = BufReader::new(file);

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) if !l.trim().is_empty() => l,
            Ok(_) => continue,
            Err(err) => {
                warn!("Skipping WAL line due to read error: {}", err);
                continue;
            }
        };

        let record: WalRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(err) => {
                warn!("Skipping invalid WAL entry: {}", err);
                continue;
            }
        };

        match record.operation {
            WalOperation::CreateFile { file } => {
                files.insert(file.path.clone(), file);
            }
            WalOperation::DeleteFile { path } => {
                files.remove(&path);
            }
            WalOperation::RenameFile { old_path, new_path } => {
                if let Some(mut metadata) = files.remove(&old_path) {
                    metadata.path = new_path.clone();
                    metadata.name = new_path
                        .split('/')
                        .next_back()
                        .unwrap_or("")
                        .to_string();
                    files.insert(new_path, metadata);
                }
            }
            WalOperation::SetFileStatus { path, status } => {
                if let Some(file) = files.get_mut(&path) {
                    file.file_status = FileStatus::str_to_tok(&status);
                }
            }
            WalOperation::RegisterDatanode { datanode } => {
                max_node_id = max_node_id.max(datanode.id);
                datanodes.insert(datanode.id, datanode);
            }
        }
    }

    Ok((files, datanodes, max_node_id.saturating_add(1).max(1)))
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::from_path(std::path::Path::new("services/master/.env")).ok();
    let settings = Arc::new(load_config());

    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| settings.log_level.clone());
    let filter = tracing_subscriber::EnvFilter::new("info")
        .add_directive(format!("master={log_level}").parse().unwrap());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();

    info!("Configuration loaded: {:?}", settings);
    let (files, available_data_nodes, next_node_id) = replay_wal(&settings.wal_path)?;
    info!(
        "WAL replay completed: files={}, datanodes={}, next_node_id={}",
        files.len(),
        available_data_nodes.len(),
        next_node_id
    );

    let (garbage_collector_tx, garbage_collector_rx) = mpsc::channel::<Vec<BlockMetadata>>(1000);
    let (audit_tx, audit_rx) = mpsc::channel::<AuditLog>(10000); // Buffer grande
    let (wal_tx, wal_rx) = mpsc::channel::<WalRecord>(20000);

    let metadatanode = Arc::new(Metadatanode {
        state: Arc::new(MasterState {
            files: RwLock::new(files),
            available_datanodes: RwLock::new(available_data_nodes),
        }),
        settings: Arc::clone(&settings),
        garbage_collector_tx,
        audit_tx,
        wal_tx,
        next_node_id: AtomicU16::new(next_node_id),
    });

    let address = settings.listen_address.parse()?;
    let service = MetadataServiceServer::new(metadatanode.as_ref().clone());

    info!("Starting MetadataNode server on {}", address);
    info!("Datanodes will self-register via heartbeat");
    let master_ref = Arc::clone(&metadatanode);
    garbage_collector(garbage_collector_rx, master_ref);
    log_sender(audit_rx);
    wal_writer(
        wal_rx,
        settings.wal_path.clone(),
        settings.wal_batch_size,
        settings.wal_flush_interval_ms,
    );

    Server::builder()
        .add_service(service)
        .serve(address)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
