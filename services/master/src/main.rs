mod models;
use crate::models::{BlockMetadata, FileMetadata, FileStatus, MasterState};
use common::dfs::metadata_service_server::{MetadataService, MetadataServiceServer};
use common::dfs::{
    BlockAssignment, CreateFileRequest, CreateFileResponse, DeleteFileRequest, DeleteFileResponse,
    FileInfo, GetFileRequest, GetFileResponse, ListDirectoryRequest, ListDirectoryResponse,
};
use std::path::PathBuf;
use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{self},
};

use tokio::sync::RwLock;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};
use uuid::Uuid;

const BLOCK_SIZE: i64 = 64 * 1024;
const REPLICATION: i32 = 2; //Minimum number of nodes that must contain a block

#[derive(Debug)]
pub struct Metadatanode {
    pub state: Arc<MasterState>,
}

impl Metadatanode {
    #[instrument(skip(state))]
    async fn _create_file(
        &self,
        state: &Arc<MasterState>,
        file_path: String,
        file_size: i64,
    ) -> Result<FileMetadata, String> {
        if file_size < 0 {
            error!("File size cannot be negative");
            return Err("File size cannot be negative".to_string());
        }

        let num_blocks = ((file_size + BLOCK_SIZE - 1) / BLOCK_SIZE)
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
            for datanode_id in 0..min(REPLICATION, datanodes.len() as i32) {
                block_datanodes
                    .push(datanodes[(block_idx + datanode_id) as usize % datanodes.len()]);
            }
            let current_block_size = if (block_idx as i64 + 1) * BLOCK_SIZE < file_size {
                BLOCK_SIZE
            } else {
                file_size - (block_idx as i64) * BLOCK_SIZE
            };
            blocks.push(BlockMetadata {
                id: block_id,
                index: block_idx,
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
            ._create_file(&self.state, file_request.path, file_request.size)
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
        !todo!()
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
}
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();

    let files = HashMap::<String, FileMetadata>::new();
    let mut available_data_nodes = HashMap::<i32, String>::new();

    available_data_nodes.insert(1, "http://[::1]:8080".to_string());
    // available_data_nodes.insert(2, "localhost:5051".to_string());

    let metadatanode = Metadatanode {
        state: Arc::new(MasterState {
            files: RwLock::new(files),
            available_datanodes: RwLock::new(available_data_nodes),
        }),
    };

    let address = "[::1]:9090".parse()?;
    let service = MetadataServiceServer::new(metadatanode);

    info!("Starting MetadataNode server on {}", address);

    Server::builder()
        .add_service(service)
        .serve(address)
        .await?;
    Ok(())

    // let file = metadatanode
    //     ._create_file(
    //         &metadatanode.state,
    //         "/home/user/test.txt".to_string(),
    //         100_000_000,
    //     )
    //     .await;

    // match file {
    //     Ok(f) => info!("File created: {:?}", f),
    //     Err(e) => error!("Error creating file: {}", e),
    // }
}
