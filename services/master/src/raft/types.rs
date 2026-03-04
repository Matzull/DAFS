use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use crate::models::FileMetadata;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MetadataCommand {
    CreateFile {
        file: FileMetadata,
    },
    DeleteFile {
        path: String,
    },
    RenameFile {
        old_path: String,
        new_path: String,
    },
    SetFileStatus {
        path: String,
        status: String,
    },
    DatanodeRegister {
        id: u16,
        addr: String,
        last_beat: i64,
    },
    DatanodeHeartbeat {
        id: u16,
        last_beat: i64,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetadataResponse {
    pub success: bool,
    pub data: Option<String>,
}

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = MetadataCommand,
        R = MetadataResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
