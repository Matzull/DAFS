mod models;
use crate::models::{BlockMetadata, FileMetadata, FileStatus, MasterState};
use std::path::PathBuf;
use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{self},
};
use tokio::sync::RwLock;
use uuid::Uuid;

const BLOCK_SIZE: i64 = 64_000_000;
const REPLICATION: i32 = 2; //Minimum number of nodes that must contain a block

pub async fn create_file(
    state: &Arc<MasterState>,
    file_name: String,
    file_path: String,
    file_size: i64,
) -> Result<FileMetadata, String> {
    if file_size < 0 {
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
        return Err("No datanodes found".to_string());
    }
    let mut blocks = Vec::<BlockMetadata>::new();
    for block_idx in 0..num_blocks {
        let block_id = Uuid::new_v4().to_string();
        let mut block_datanodes = vec![];
        for datanode_id in 0..min(REPLICATION, datanodes.len() as i32) {
            block_datanodes.push(datanodes[(block_idx + datanode_id) as usize % datanodes.len()]);
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
        name: file_name.clone(),
        path: file_path.clone(),
        size: file_size,
        owner: "SYSTEM".to_string(),
        created_at: now_miliseconds,
        last_modified_at: now_miliseconds,
        checksum: "TO_IMPLEMENT".to_string(),
        file_status: FileStatus::Uploading,
        blocks: blocks,
    };
    let mut file_path_pathbuf = PathBuf::from(file_path);
    file_path_pathbuf.push(file_name);
    let full_path = file_path_pathbuf.to_string_lossy().to_string();
    let mut files_lock = state.files.write().await;

    if files_lock.contains_key(&full_path) {
        return Err(format!("File {} already exists", full_path));
    }
    files_lock.insert(full_path, file.clone());
    return Ok(file);
}

#[tokio::main]
pub async fn main() {
    let files = HashMap::<String, FileMetadata>::new();
    let mut available_data_nodes = HashMap::<i32, String>::new();

    available_data_nodes.insert(1, "localhost:5050".to_string());
    available_data_nodes.insert(2, "localhost:5051".to_string());

    let state = Arc::new(MasterState {
        files: RwLock::new(files),
        available_datanodes: RwLock::new(available_data_nodes),
    });
}
