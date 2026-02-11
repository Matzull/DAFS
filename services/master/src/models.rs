use std::collections::HashMap;
use std::fmt::Display;
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
pub enum FileStatus {
    Uploading, // File is being written to, reading is bloqued
    Ready,     // File can be read and written to
    Deleted,   // Marked to be deleted
}

impl Display for FileStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileStatus::Uploading => write!(f, "Uploading"),
            FileStatus::Ready => write!(f, "Ready"),
            FileStatus::Deleted => write!(f, "Deleted"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DatanodeInfo {
    pub id: u16,
    pub addr: String,
    pub last_heartbeat: i64,
}

#[derive(Clone, Debug)]
pub struct BlockMetadata {
    pub id: String,
    pub index: i32,
    pub size: i64,
    pub checksum: String,
    pub locations: Vec<u16>, //The ids of the datanodes containing that block
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

    pub file_status: FileStatus,

    pub blocks: Vec<BlockMetadata>,
}

#[derive(Debug)]
pub struct MasterState {
    pub files: RwLock<HashMap<String, FileMetadata>>,
    pub available_datanodes: RwLock<HashMap<u16, DatanodeInfo>>,
}

// impl BlockMetadata {
//     /// Create a new block metadata
//     pub fn new(id: String, index: i32, size: i64, locations: Vec<i32>) -> Self {
//         BlockMetadata {
//             id,
//             index,
//             size,
//             locations,
//         }
//     }
// }

impl FileMetadata {
    /// Check if the file is ready for reading
    pub fn is_ready(&self) -> bool {
        matches!(self.file_status, FileStatus::Ready)
    }

    /// Check if the file is being uploaded
    pub fn is_uploading(&self) -> bool {
        matches!(self.file_status, FileStatus::Uploading)
    }

    /// Check if the file is marked for deletion
    pub fn is_deleted(&self) -> bool {
        matches!(self.file_status, FileStatus::Deleted)
    }

    /// Get the total number of blocks
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Calculate total size from blocks
    pub fn calculated_size(&self) -> i64 {
        self.blocks.iter().map(|b| b.size).sum()
    }
}

#[cfg(test)]
#[path = "models_tests.rs"]
mod tests;
