use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
pub enum FileStatus {
    Uploading, // File is being written to, reading is bloqued
    Ready,     // File can be read and written to
    Deleted,   // Marked to be deleted
}

#[derive(Clone, Debug)]
pub struct DatanodeInfo {
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct BlockMetadata {
    pub id: String,
    pub index: i32,
    pub size: i64,

    pub locations: Vec<i32>, //The ids of the datanodes containing that block
}
#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub id: String,
    pub name: String,
    pub path: String,
    pub size: i64,

    pub owner: String,
    pub created_at: i64,       //In millis since epoch
    pub last_modified_at: i64, //In millis since epoch
    pub checksum: String,

    pub file_status: FileStatus,

    pub blocks: Vec<BlockMetadata>,
}

#[derive(Debug)]
pub struct MasterState {
    pub files: RwLock<HashMap<String, FileMetadata>>,
    pub available_datanodes: RwLock<HashMap<i32, String>>,
}
