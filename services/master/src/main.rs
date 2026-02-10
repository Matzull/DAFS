mod models;
use crate::models::{BlockMetadata, DatanodeInfo, FileMetadata, FileStatus, MasterState};
use common::dfs::metadata_service_server::{MetadataService, MetadataServiceServer};
use common::dfs::{
    data_node_service_client, BlockAssignment, CreateFileRequest, CreateFileResponse,
    DeleteBlockRequest, DeleteFileRequest, DeleteFileResponse, FileInfo, GetFileRequest,
    GetFileResponse, HeartbeatRequest, HeartbeatResponse, ListDirectoryRequest,
    ListDirectoryResponse, RenameFileRequest, RenameFileResponse,
};
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
use dotenvy::dotenv;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::env;
use tokio::sync::{mpsc, RwLock};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

const BLOCK_SIZE: i64 = 64 * 1024;
const REPLICATION: i32 = 2; //Minimum number of nodes that must contain a block

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct InternalSettings {
    pub listen_address: String,
    pub block_size: i64,
    pub replication_factor: i32,
    pub log_level: String,
}

pub fn load_config() -> InternalSettings {
    let cli = Cli::parse();

    let figment = Figment::new()
        .merge(Serialized::defaults(InternalSettings {
            listen_address: "[::1]:9090".to_string(),
            block_size: BLOCK_SIZE,
            replication_factor: REPLICATION,
            log_level: "info".to_string(),
        }))
        .merge(Toml::file(&cli.config_path));

    figment.extract().expect("Error loading configuration")
}

/// DAFS MetadataNode Service
#[derive(Parser, Debug)]
#[command(name = "dafs-master")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Server listen address
    #[arg(short, long, default_value = "[::1]:9090")]
    listen_address: Option<String>,

    /// Block size (in bytes)
    #[arg(short, long, default_value = "65536")]
    block_size: Option<i64>,

    /// Replication factor
    #[arg(short, long, default_value = "2")]
    replication_factor: Option<i32>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: Option<String>,

    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,
}

#[derive(Serialize, Clone)]
pub struct AuditLog {
    pub timestamp: DateTime<Utc>, //In epoch_millis but serialized
    pub action: String,
    pub file_path: String,
    pub status: String,
    pub user: String,
}

#[derive(Debug)]
pub struct Metadatanode {
    pub state: Arc<MasterState>,
    pub garbage_collector_tx: mpsc::Sender<Vec<BlockMetadata>>,
    pub audit_tx: mpsc::Sender<AuditLog>,
    pub settings: Arc<InternalSettings>,
    pub next_node_id: AtomicU16,
}

impl Clone for Metadatanode {
    fn clone(&self) -> Self {
        Metadatanode {
            state: Arc::clone(&self.state),
            garbage_collector_tx: self.garbage_collector_tx.clone(),
            audit_tx: self.audit_tx.clone(),
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
            });
        }
        let now_miliseconds = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;

        let file = FileMetadata {
            id: file_id,
            name: file_path.split('/').last().unwrap_or("").to_string(),
            path: file_path.clone(),
            size: file_size,
            owner: "SYSTEM".to_string(),
            created_at: now_miliseconds,
            last_modified_at: now_miliseconds,
            checksum: "TO_IMPLEMENT".to_string(),
            file_status: FileStatus::Uploading,
            blocks,
        };
        let file_path_pathbuf = PathBuf::from(file_path);
        let full_path = file_path_pathbuf.to_string_lossy().to_string();
        let mut files_lock = state.files.write().await;

        if files_lock.contains_key(&full_path) {
            error!("File {} already exists", full_path);
            return Err(format!("File {} already exists", full_path));
        }
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
            .into_iter()
            .map(|block| BlockAssignment {
                block_id: block.id,
                node_address: datanodes.get(&block.locations[0]).unwrap().addr.to_string(), //TODO: Use real nodes
                block_size: block.size,
                order: block.index as i64, //TODO: check var size i64 vs i32
            })
            .collect();
        self.audit("Creating file".to_string(), file_request.path.clone())
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
        self.audit("Getting file".to_string(), file_path.to_string())
            .await;
        Ok(Response::new(GetFileResponse {
            file_id: file_metadata.id.clone(),
            size: file_metadata.size,
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
            self.audit("Deleting file".to_string(), file_path.to_string())
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
            })
            .collect();
        info!(
            dir_path,
            file_count = files.len(),
            "Directory listing completed"
        );
        self.audit("Listing dir".to_string(), dir_path.to_string())
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
            files_lock.insert(rename_req.new_path, file_metadata);
        } else {
            warn!(old_path = rename_req.old_path, "File not found for rename");
            return Err(Status::not_found("Couldnt rename file"));
        }
        self.audit("Renamed file".to_string(), rename_req.old_path.to_string())
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
        let remote_addr = node_request.addr;
        let mut datanodes_lock = self.state.available_datanodes.write().await;

        // If node_id is 0, this is a new datanode registering
        if node_request.node_id == 0 {
            let new_id = self.next_node_id.fetch_add(1, Ordering::SeqCst);
            info!(assigned_node_id = new_id, "New datanode registered");

            // Extract IP from remote_addr and use standard datanode port
            let datanode_addr = format!("http://{}", remote_addr);

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

        if let Some(datanode) = datanodes_lock.get_mut(&(node_request.node_id as u16)) {
            datanode.last_heartbeat = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as i64;
        } else {
            warn!(
                node_id = node_request.node_id,
                "Heartbeat from unknown node"
            );
        }
        //TODO: send audit
        Ok(Response::new(HeartbeatResponse {
            acknowledgement: true,
            assigned_node_id: node_request.node_id,
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
        let mut client = data_node_service_client::DataNodeServiceClient::connect(datanode.clone())
            .await
            .map_err(|err| {
                error!(block_id, datanode, "Could not connect to datanode: {}", err);
                format!("Could not connect to datanode {}", err)
            })?;
        client
            .delete_block(DeleteBlockRequest {
                block_id: block_id.to_string(),
            })
            .await
            .map_err(|err| {
                error!(
                    block_id,
                    datanode, "Failed to delete block on datanode: {}", err
                );
                format!("Could not delete block on datanode {}", err)
            })?;
        info!(block_id, datanode, "Block deletion confirmed by datanode");
        self.audit("Notified to delete".to_string(), block_id.to_string())
            .await;
        Ok(true)
    }

    async fn audit(&self, action: String, file_path: String) {
        let now = Utc::now();
        self.audit_tx
            .send(AuditLog {
                timestamp: now,
                action: action.to_string(),
                file_path: file_path,
                status: "SUCCESS".to_string(),
                user: "1".to_string(),
            })
            .await
            .ok();
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Arc::new(load_config());

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .ok()
                .and_then(|v| tracing_subscriber::EnvFilter::try_new(v).ok())
                .unwrap_or_else(|| tracing_subscriber::EnvFilter::new(settings.log_level.clone())),
        )
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();

    let files = HashMap::<String, FileMetadata>::new();
    let available_data_nodes = HashMap::<u16, DatanodeInfo>::new();

    let (garbage_collector_tx, mut garbage_collector_rx) =
        mpsc::channel::<Vec<BlockMetadata>>(1000);
    let (audit_tx, mut audit_rx) = mpsc::channel::<AuditLog>(10000); // Buffer grande

    let metadatanode = Arc::new(Metadatanode {
        state: Arc::new(MasterState {
            files: RwLock::new(files),
            available_datanodes: RwLock::new(available_data_nodes),
        }),
        settings: Arc::clone(&settings),
        garbage_collector_tx: garbage_collector_tx,
        audit_tx: audit_tx,
        next_node_id: AtomicU16::new(1), // Start assigning IDs from 1
    });

    let address = settings.listen_address.parse()?;
    let service = MetadataServiceServer::new(metadatanode.as_ref().clone());

    info!("Starting MetadataNode server on {}", address);
    info!("Datanodes will self-register via heartbeat");
    let master_ref = Arc::clone(&metadatanode);
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
    dotenv().ok();
    env::vars().for_each(|(key, value)| {
        debug!("Env var: {}={}", key, value);
    });
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
                Ok(response) => info!("Respuesta Elastic: {:?}", response.status()), // <--- DEBUG
                Err(e) => error!("Error enviando log: {:?}", e),                     // <--- DEBUG
            }
        }
    });

    Server::builder()
        .add_service(service)
        .serve(address)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
