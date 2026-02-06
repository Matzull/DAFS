mod models;
use crate::models::{BlockMetadata, FileMetadata, FileStatus, MasterState};
use common::dfs::metadata_service_server::{MetadataService, MetadataServiceServer};
use common::dfs::{
    data_node_service_client, BlockAssignment, CreateFileRequest, CreateFileResponse,
    DeleteBlockRequest, DeleteFileRequest, DeleteFileResponse, FileInfo, GetFileRequest,
    GetFileResponse, HeartbeatRequest, HeartbeatResponse, ListDirectoryRequest,
    ListDirectoryResponse, RenameFileRequest, RenameFileResponse,
};
use serde::de::Error;
use std::path::PathBuf;
use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{self},
};
use tracing_subscriber::fmt::format;

use clap::Parser;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument, warn};
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

#[derive(Debug, Clone)]
pub struct Metadatanode {
    pub state: Arc<MasterState>,
    pub garbage_collector_tx: mpsc::Sender<Vec<BlockMetadata>>,
    pub settings: Arc<InternalSettings>,
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
        let datanodes: Vec<i32> = datanodes_lock.keys().cloned().collect();
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
                file_request.path,
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
                node_address: datanodes.get(&block.locations[0]).unwrap().to_string(), //TODO: Use real nodes
                block_size: block.size,
                order: block.index as i64, //TODO: check var size i64 vs i32
            })
            .collect();
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
                node_address: datanodes
                    .get(&block.locations[0])
                    .cloned()
                    .unwrap_or_else(|| "unknown:0".to_string()), //TODO: Use real nodes
                block_size: block.size,
                order: block.index as i64, //TODO: check var size i64 vs i32
            })
            .collect();
        Ok(Response::new(GetFileResponse {
            file_id: file_metadata.id.clone(),
            size: file_metadata.size,
            blocks: block_assignments,
        }))
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let file_path = request.into_inner().path;
        let mut files = self.state.files.write().await;
        if let Some(filemetadata) = files.remove(&file_path) {
            let to_delete = filemetadata.blocks;
            self.garbage_collector_tx.send(to_delete).await.ok();
            Ok(Response::new(DeleteFileResponse {
                success: true,
                error_message: "".to_string(),
            }))
        } else {
            return Err(Status::not_found("File not found"));
        }
    }

    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        info!("Received list_directory request");
        let dir_path = request.into_inner().path;
        let files_lock = self.state.files.read().await;
        let files = files_lock
            .iter()
            .filter(|(path, _)| path.starts_with(&dir_path))
            .map(|(_, v)| FileInfo {
                filename: v.name.clone(),
                is_directory: false,
                size: v.size,
            })
            .collect();
        Ok(Response::new(ListDirectoryResponse { files }))
    }

    async fn rename_file(
        &self,
        request: Request<RenameFileRequest>,
    ) -> Result<Response<RenameFileResponse>, Status> {
        info!("Received rename file request");
        let rename_req = request.into_inner();
        let mut files_lock = self.state.files.write().await;
        if files_lock.contains_key(&rename_req.new_path) {
            return Err(Status::already_exists(format!(
                "File {} already exists",
                rename_req.new_path
            )));
        }
        if let Some(mut file_metadata) = files_lock.remove(&rename_req.old_path) {
            file_metadata.name = rename_req.new_path.clone();
            files_lock.insert(rename_req.new_path, file_metadata);
        } else {
            return Err(Status::not_found("Couldnt rename file"));
        }
        Ok(Response::new(RenameFileResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        info!("Received heartbeat from node");
        let node_request = request.into_inner();
        // let mut state_lock = self.state.available_datanodes.write().await;
        info!("{} {}", node_request.node_id, node_request.cpu_usage);
        // if let Some(datanode) = state_lock.get(&5){ //&node_request.node_id
        //     info!("")
        // }
        //TODO: send audit
        Ok(Response::new(HeartbeatResponse {
            acknowledgement: true,
        }))
    }
}

impl Metadatanode {
    async fn notify_datanode_to_delete(
        &self,
        block_id: &str,
        datanode: String,
    ) -> Result<bool, String> {
        let mut client = data_node_service_client::DataNodeServiceClient::connect(datanode)
            .await
            .map_err(|err| format!("Could not connect to datanode {}", err.to_string()))?;
        client
            .delete_block(DeleteBlockRequest {
                block_id: block_id.to_string(),
            })
            .await
            .map_err(|err| format!("Could not connect to datanode {}", err.to_string()))?;
        Ok(true)
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
    let mut available_data_nodes = HashMap::<i32, String>::new();

    available_data_nodes.insert(1, "http://[::1]:8080".to_string());
    // available_data_nodes.insert(2, "http://[::1]:8081".to_string());
    let (tx, mut rx) = mpsc::channel::<Vec<BlockMetadata>>(1000);

    let metadatanode = Arc::new(Metadatanode {
        state: Arc::new(MasterState {
            files: RwLock::new(files),
            available_datanodes: RwLock::new(available_data_nodes),
        }),
        settings: Arc::clone(&settings),
        garbage_collector_tx: tx,
    });

    let address = settings.listen_address.parse()?;
    let service = MetadataServiceServer::new((*metadatanode).clone());

    info!("Starting MetadataNode server on {}", address);
    let master_ref = Arc::clone(&metadatanode);
    tokio::spawn(async move {
        info!("Garbage Collector started");
        while let Some(blocks) = rx.recv().await {
            for block in blocks {
                for location in block.locations {
                    let datanode_addr = {
                        let nodes = master_ref.state.available_datanodes.read().await;
                        nodes.get(&location).cloned()
                    };

                    // 2. Manejamos el caso de que el DataNode ya no exista
                    let addr = match datanode_addr {
                        Some(a) => a,
                        None => {
                            warn!(
                                "DataNode {} not available to delete block {}",
                                location, block.id
                            );
                            continue;
                        }
                    };
                    if let Err(e) = master_ref.notify_datanode_to_delete(&block.id, addr).await {
                        error!("Error deleting block {} in {}: {}", block.id, location, e);
                    }
                }
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
