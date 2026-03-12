mod auth;
mod models;
mod raft {
    pub mod membership_manager;
    pub mod runtime;
    pub mod state_machine;
    pub mod types;
}
use crate::auth::AuthManager;
use crate::models::{BlockMetadata, DatanodeInfo, FileMetadata, FileStatus, MasterState};
use common::dfs::metadata_service_server::{MetadataService, MetadataServiceServer};
use common::dfs::{
    data_node_service_client, BlockAssignment, BlockList, CreateFileRequest, CreateFileResponse,
    DeleteBlockRequest, DeleteFileRequest, DeleteFileResponse, FileInfo, GetFileRequest,
    GetFileResponse, HeartbeatRequest, HeartbeatResponse, ListDirectoryRequest,
    ListDirectoryResponse, LoginUserRequest, LoginUserResponse, RegisterUserRequest,
    RegisterUserResponse, RenameFileRequest, RenameFileResponse, ReplicateBlocksRequest,
    SetFileStatusRequest, SetFileStatusResponse,
};
use common::raft::raft_service_server::{RaftService, RaftServiceServer};
use common::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ChangeMembershipRequest, ChangeMembershipResponse,
    GetLeaderRequest, GetLeaderResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::error::InstallSnapshotError;
use tokio::task::JoinSet;
// use tokio::net::unix::pipe::Receiver;
use chrono::{DateTime, Utc};
use clap::Parser;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use radix_trie::{Trie, TrieCommon};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{self},
};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::raft::membership_manager::MembershipManager;
use crate::raft::runtime::MasterRaftContext;
use crate::raft::types::{MetadataCommand, MetadataResponse, NodeId};

const DEFAULT_BLOCK_SIZE: i64 = 64 * 1024;
const DEFAULT_REPLICATION: i32 = 2;
const DEFAULT_LISTEN_ADDRESS: &str = "[::1]:9090";
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_AUDIT_SERVICE_ADDR: &str = "http://[::1]:9090";
const DEFAULT_WAL_PATH: &str = "services/master/master.wal";
const DEFAULT_WAL_BATCH_SIZE: usize = 128;
const DEFAULT_WAL_FLUSH_INTERVAL_MS: u64 = 250;
const DEFAULT_ENV_FILE_PATH: &str = "services/master/.env";
const DEFAULT_GARBAGE_COLLECTOR_CHANNEL_SIZE: usize = 1000;
const DEFAULT_BLOCK_REPLICATOR_CHANNEL_SIZE: usize = 1000;
const DEFAULT_AUDIT_CHANNEL_SIZE: usize = 10000;
const DEFAULT_WAL_CHANNEL_SIZE: usize = 20000;
const DEFAULT_REPLICATION_MAX_PARALLEL_SOURCES: usize = 8;
const DEFAULT_AUDIT_DEFAULT_USER: &str = "system";
const DEFAULT_FILE_DEFAULT_OWNER: &str = "SYSTEM";
const DEFAULT_ELASTIC_ALLOW_INVALID_CERTS: bool = true;
const DEFAULT_AUTH_DB_PATH: &str = "services/master/users.db";
const DEFAULT_JWT_SECRET: &str = "change-me-master-secret";
const DEFAULT_JWT_EXP_SECONDS: i64 = 3600;
const DEFAULT_SERVICE_TOKEN_EXP_SECONDS: i64 = 300;
const DEFAULT_RAFT_NODE_ID: u64 = 1;
const DEFAULT_RAFT_CLUSTER_NAME: &str = "dafs-metadata";
const DEFAULT_RAFT_BOOTSTRAP: bool = false;

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
    pub env_file_path: String,
    pub garbage_collector_channel_size: usize,
    pub block_replicator_channel_size: usize,
    pub audit_channel_size: usize,
    pub wal_channel_size: usize,
    pub replication_max_parallel_sources: usize,
    pub audit_default_user: String,
    pub file_default_owner: String,
    pub elastic_allow_invalid_certs: bool,
    pub auth_db_path: String,
    pub jwt_secret: String,
    pub jwt_exp_seconds: i64,
    pub service_token_exp_seconds: i64,
    pub raft_node_id: u64,
    pub raft_cluster_name: String,
    pub raft_bootstrap: bool,
    pub raft_initial_members: Vec<String>,
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
            env_file_path: DEFAULT_ENV_FILE_PATH.to_string(),
            garbage_collector_channel_size: DEFAULT_GARBAGE_COLLECTOR_CHANNEL_SIZE,
            block_replicator_channel_size: DEFAULT_BLOCK_REPLICATOR_CHANNEL_SIZE,
            audit_channel_size: DEFAULT_AUDIT_CHANNEL_SIZE,
            wal_channel_size: DEFAULT_WAL_CHANNEL_SIZE,
            replication_max_parallel_sources: DEFAULT_REPLICATION_MAX_PARALLEL_SOURCES,
            audit_default_user: DEFAULT_AUDIT_DEFAULT_USER.to_string(),
            file_default_owner: DEFAULT_FILE_DEFAULT_OWNER.to_string(),
            elastic_allow_invalid_certs: DEFAULT_ELASTIC_ALLOW_INVALID_CERTS,
            auth_db_path: DEFAULT_AUTH_DB_PATH.to_string(),
            jwt_secret: DEFAULT_JWT_SECRET.to_string(),
            jwt_exp_seconds: DEFAULT_JWT_EXP_SECONDS,
            service_token_exp_seconds: DEFAULT_SERVICE_TOKEN_EXP_SECONDS,
            raft_node_id: DEFAULT_RAFT_NODE_ID,
            raft_cluster_name: DEFAULT_RAFT_CLUSTER_NAME.to_string(),
            raft_bootstrap: DEFAULT_RAFT_BOOTSTRAP,
            raft_initial_members: Vec::new(),
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

    /// Dotenv file path
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    env_file_path: Option<String>,

    /// Garbage collector channel size
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    garbage_collector_channel_size: Option<usize>,

    /// Block replicator channel size
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    block_replicator_channel_size: Option<usize>,

    /// Audit channel size
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    audit_channel_size: Option<usize>,

    /// WAL channel size
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_channel_size: Option<usize>,

    /// Maximum parallel source datanodes for replication tasks
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    replication_max_parallel_sources: Option<usize>,

    /// Default user identity used in audit logs when no actor is available
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    audit_default_user: Option<String>,

    /// Default owner used for new file metadata
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    file_default_owner: Option<String>,

    /// Allow invalid TLS certificates for Elasticsearch client
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    elastic_allow_invalid_certs: Option<bool>,

    /// SQLite path for users database
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_db_path: Option<String>,

    /// Secret used to sign and verify JWTs
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    jwt_secret: Option<String>,

    /// Access token validity (seconds)
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    jwt_exp_seconds: Option<i64>,

    /// Internal service token validity (seconds)
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    service_token_exp_seconds: Option<i64>,

    /// Raft node id for this master
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    raft_node_id: Option<u64>,

    /// Logical raft cluster name
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    raft_cluster_name: Option<String>,

    /// Bootstrap cluster membership on startup if node is pristine
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    raft_bootstrap: Option<bool>,

    /// Initial raft members in format <id>=<addr>; can be specified multiple times
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    raft_initial_members: Option<Vec<String>>,

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

pub struct Metadatanode {
    pub state: Arc<MasterState>,
    pub garbage_collector_tx: mpsc::Sender<Vec<BlockMetadata>>,
    pub block_replicator_tx: mpsc::Sender<FileMetadata>,
    pub audit_tx: mpsc::Sender<AuditLog>,
    pub settings: Arc<InternalSettings>,
    pub auth: Arc<AuthManager>,
    pub next_node_id: AtomicU16,
    pub raft_ctx: Arc<MasterRaftContext>,
}

impl std::fmt::Debug for Metadatanode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metadatanode")
            .field("settings", &self.settings)
            .field(
                "next_node_id",
                &self.next_node_id.load(std::sync::atomic::Ordering::SeqCst),
            )
            .finish()
    }
}

#[derive(Clone)]
struct RaftRpcNode {
    raft_ctx: Arc<MasterRaftContext>,
}

impl RaftRpcNode {
    fn new(raft_ctx: Arc<MasterRaftContext>) -> Self {
        Self { raft_ctx }
    }
}

#[tonic::async_trait]
impl RaftService for RaftRpcNode {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let rpc: openraft::raft::AppendEntriesRequest<crate::raft::types::TypeConfig> =
            serde_json::from_slice(&request.into_inner().payload).map_err(|err| {
                Status::invalid_argument(format!("Invalid append_entries payload: {err}"))
            })?;

        let result = self.raft_ctx.raft.append_entries(rpc).await;
        let payload = serde_json::to_vec(&result).map_err(|err| {
            Status::internal(format!("Could not encode append_entries response: {err}"))
        })?;

        Ok(Response::new(AppendEntriesResponse { payload }))
    }

    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let rpc: openraft::raft::VoteRequest<NodeId> =
            serde_json::from_slice(&request.into_inner().payload)
                .map_err(|err| Status::invalid_argument(format!("Invalid vote payload: {err}")))?;

        let result = self.raft_ctx.raft.vote(rpc).await;
        let payload = serde_json::to_vec(&result)
            .map_err(|err| Status::internal(format!("Could not encode vote response: {err}")))?;

        Ok(Response::new(VoteResponse { payload }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let rpc: openraft::raft::InstallSnapshotRequest<crate::raft::types::TypeConfig> =
            serde_json::from_slice(&request.into_inner().payload).map_err(|err| {
                Status::invalid_argument(format!("Invalid install_snapshot payload: {err}"))
            })?;

        let result: Result<
            openraft::raft::InstallSnapshotResponse<NodeId>,
            openraft::error::RaftError<NodeId, InstallSnapshotError>,
        > = self.raft_ctx.raft.install_snapshot(rpc).await;
        let payload = serde_json::to_vec(&result).map_err(|err| {
            Status::internal(format!("Could not encode install_snapshot response: {err}"))
        })?;

        Ok(Response::new(InstallSnapshotResponse { payload }))
    }

    async fn get_leader(
        &self,
        _request: Request<GetLeaderRequest>,
    ) -> Result<Response<GetLeaderResponse>, Status> {
        if let Some((leader_id, leader_address)) = self.raft_ctx.leader_hint().await {
            return Ok(Response::new(GetLeaderResponse {
                known: true,
                leader_id,
                leader_address,
            }));
        }

        Ok(Response::new(GetLeaderResponse {
            known: false,
            leader_id: 0,
            leader_address: String::new(),
        }))
    }

    async fn change_membership(
        &self,
        request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        let req = request.into_inner();

        if req.voter_ids.len() == 2 {
            warn!(
                voters = ?req.voter_ids,
                "Requested 2-voter membership: if one node disconnects, cluster loses quorum"
            );
        } else if req.voter_ids.len() > 2 && req.voter_ids.len() % 2 == 0 {
            warn!(
                voters = ?req.voter_ids,
                "Requested even number of voters; odd voter counts (3,5,...) are recommended for Raft availability"
            );
        }

        let result = self
            .raft_ctx
            .raft
            .change_membership(req.voter_ids, req.retain_removed_as_learners)
            .await;

        match result {
            Ok(_) => Ok(Response::new(ChangeMembershipResponse {
                accepted: true,
                message: String::new(),
            })),
            Err(err) => Ok(Response::new(ChangeMembershipResponse {
                accepted: false,
                message: err.to_string(),
            })),
        }
    }
}

impl Clone for Metadatanode {
    fn clone(&self) -> Self {
        Metadatanode {
            state: Arc::clone(&self.state),
            garbage_collector_tx: self.garbage_collector_tx.clone(),
            block_replicator_tx: self.block_replicator_tx.clone(),
            audit_tx: self.audit_tx.clone(),
            settings: Arc::clone(&self.settings),
            auth: Arc::clone(&self.auth),
            next_node_id: AtomicU16::new(self.next_node_id.load(Ordering::SeqCst)),
            raft_ctx: Arc::clone(&self.raft_ctx),
        }
    }
}

impl Metadatanode {
    #[allow(clippy::result_large_err)]
    fn authorize_any(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<common::AuthClaims, Status> {
        common::authorize_metadata(metadata, self.auth.jwt_secret())
    }

    #[allow(clippy::result_large_err)]
    fn authorize_user_or_master(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<common::AuthClaims, Status> {
        let claims = self.authorize_any(metadata)?;
        if claims.role != common::ROLE_USER && claims.role != common::ROLE_MASTER {
            return Err(Status::permission_denied("User or master role required"));
        }
        Ok(claims)
    }

    #[allow(clippy::result_large_err)]
    fn authorize_datanode(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<common::AuthClaims, Status> {
        let claims = self.authorize_any(metadata)?;
        common::require_role(&claims, common::ROLE_DATANODE)?;
        Ok(claims)
    }

    fn can_access_path(&self, owner: &str, claims: &common::AuthClaims) -> bool {
        claims.role == common::ROLE_MASTER || owner == claims.sub
    }

    async fn leader_hint_message(&self) -> String {
        if let Some((leader_id, leader_addr)) = self.raft_ctx.leader_hint().await {
            return format!("not leader; leader_id={leader_id}; leader_addr={leader_addr}");
        }
        "not leader; leader unknown".to_string()
    }

    async fn ensure_leader_for_write(&self) -> Result<(), Status> {
        if self.raft_ctx.is_local_leader().await {
            return Ok(());
        }

        Err(Status::failed_precondition(
            self.leader_hint_message().await,
        ))
    }

    async fn raft_write(&self, command: MetadataCommand) -> Result<MetadataResponse, Status> {
        let resp = match self.raft_ctx.raft.client_write(command).await {
            Ok(response) => response,
            Err(_) => {
                return Err(Status::failed_precondition(
                    self.leader_hint_message().await,
                ));
            }
        };

        Ok(resp.data)
    }

    #[instrument(skip(state, settings))]
    async fn _create_file(
        &self,
        state: &Arc<MasterState>,
        settings: &Arc<InternalSettings>,
        file_path: String,
        file_size: i64,
        owner: String,
    ) -> Result<FileMetadata, (i8, String)> {
        if file_size < 0 {
            error!("File size cannot be negative");
            return Err((0, "File size cannot be negative".to_string()));
        }
        let file_path_pathbuf = PathBuf::from(file_path.clone());
        let full_path = file_path_pathbuf.to_string_lossy().to_string();
        let files_lock = state.files.read().await;
        if files_lock.get(&full_path).is_some() {
            error!("File {} already exists", full_path);
            return Err((1, format!("File {full_path} already exists")));
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
            return Err((2, "No datanodes found".to_string()));
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
            owner,
            created_at: now_miliseconds,
            last_modified_at: now_miliseconds,
            file_status: FileStatus::Uploading,
            blocks,
        };

        info!("File metadata planned for {}", file.id);
        return Ok(file);
    }

    async fn update_file_status_internal(
        &self,
        file_path: String,
        new_status: String,
    ) -> Result<(), Status> {
        if self.state.files.read().await.get(&file_path).is_none() {
            self.audit_failure(
                "FILE_SET_STATUS",
                Some(file_path.clone()),
                "NOT_FOUND",
                "MASTER".to_string(),
                json!({
                    "status": new_status,
                }),
            )
            .await;
            return Err(Status::not_found("File not found"));
        }

        self.raft_write(MetadataCommand::SetFileStatus {
            path: file_path.clone(),
            status: new_status.clone(),
        })
        .await?;

        let queued_for_replication = FileStatus::str_to_tok(&new_status) == FileStatus::Ready;
        if queued_for_replication {
            if let Some(file_metadata) = self.state.files.read().await.get(&file_path).cloned() {
                self.block_replicator_tx.send(file_metadata).await.ok();
            }
        }

        self.audit_success(
            "FILE_SET_STATUS",
            Some(file_path),
            "MASTER".to_string(),
            json!({
                "status": new_status,
                "queued_for_replication": queued_for_replication,
            }),
        )
        .await;

        Ok(())
    }

    async fn update_file_status_by_id(
        &self,
        file_id: &str,
        new_status: String,
    ) -> Result<(), Status> {
        // Resolve the current path from the id, since the file may have been
        // renamed while replication was in progress.
        let current_path = {
            let files_lock = self.state.files.read().await;
            files_lock
                .iter()
                .find(|(_, v)| v.id == file_id)
                .map(|(k, _)| k.clone())
        };

        match current_path {
            Some(path) => self.update_file_status_internal(path, new_status).await,
            None => {
                self.audit_failure(
                    "FILE_SET_STATUS",
                    Some(file_id.to_string()),
                    "NOT_FOUND",
                    "MASTER".to_string(),
                    json!({ "status": new_status, "reason": "id not found" }),
                )
                .await;
                Err(Status::not_found("File not found"))
            }
        }
    }
}

#[tonic::async_trait]
impl MetadataService for Metadatanode {
    #[instrument(skip(self, request))]
    async fn register_user(
        &self,
        request: Request<RegisterUserRequest>,
    ) -> Result<Response<RegisterUserResponse>, Status> {
        let payload = request.into_inner();
        self.auth
            .register_user(&payload.username, &payload.password)
            .map_err(Status::invalid_argument)?;

        self.audit_success(
            "USER_REGISTER",
            Some(payload.username.clone()),
            payload.username,
            json!({}),
        )
        .await;

        Ok(Response::new(RegisterUserResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn login_user(
        &self,
        request: Request<LoginUserRequest>,
    ) -> Result<Response<LoginUserResponse>, Status> {
        let payload = request.into_inner();
        let (access_token, expires_at) = self
            .auth
            .login_user(&payload.username, &payload.password)
            .map_err(Status::unauthenticated)?;

        self.audit_success(
            "USER_LOGIN",
            Some(payload.username.clone()),
            payload.username,
            json!({ "expires_at": expires_at }),
        )
        .await;

        Ok(Response::new(LoginUserResponse {
            access_token,
            expires_at,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        self.ensure_leader_for_write().await?;
        info!("Received create_file request");
        let claims = self.authorize_user_or_master(request.metadata())?;
        let file_request = request.into_inner();
        let file_metadata = match self
            ._create_file(
                &self.state,
                &self.settings,
                file_request.path.clone(),
                file_request.size,
                claims.sub.clone(),
            )
            .await
        {
            Ok(metadata) => metadata,
            Err(err) => {
                self.audit_failure(
                    "FILE_CREATE",
                    Some(file_request.path.clone()),
                    "CREATE_ERROR",
                    claims.sub.clone(),
                    json!({
                        "size": file_request.size,
                        "error": err.1,
                    }),
                )
                .await;
                match err.0 {
                    0 => return Err(Status::invalid_argument(err.1)),
                    1 => return Err(Status::already_exists(err.1)),
                    2 => return Err(Status::failed_precondition(err.1)),
                    _ => return Err(Status::internal("Could not create file metadata")),
                }
            }
        };

        self.raft_write(MetadataCommand::CreateFile {
            file: file_metadata.clone(),
        })
        .await?;

        let datanodes = self.state.available_datanodes.read().await;
        let block_assignments: Vec<BlockAssignment> = file_metadata
            .blocks
            .iter()
            .map(|block| BlockAssignment {
                block_id: block.id.clone(),
                node_address: datanodes.get(&block.locations[0]).unwrap().addr.to_string(), //TODO: Use real nodes
                block_size: block.size,
                order: block.index as i64, //TODO: check var size i64 vs i32
            })
            .collect();
        self.audit_success(
            "FILE_CREATE",
            Some(file_request.path.clone()),
            claims.sub.clone(),
            json!({
                "size": file_request.size,
                "file_id": file_metadata.id.clone(),
                "block_count": block_assignments.len(),
            }),
        )
        .await;

        Ok(Response::new(CreateFileResponse {
            file_id: file_metadata.id,
            blocks: block_assignments,
        }))
    }

    #[instrument(skip(self, request))]
    async fn get_file_metadata(
        &self,
        request: Request<GetFileRequest>,
    ) -> Result<Response<GetFileResponse>, Status> {
        info!("Received get_file_metadata request");
        let claims = self.authorize_user_or_master(request.metadata())?;
        let file_path = request.into_inner().path;
        let files_lock = self.state.files.read().await;
        let file_metadata = match files_lock.get(&file_path) {
            Some(filemetadata) => filemetadata,
            None => {
                self.audit_failure(
                    "FILE_GET_METADATA",
                    Some(file_path.clone()),
                    "NOT_FOUND",
                    claims.sub.clone(),
                    json!({}),
                )
                .await;
                return Err(Status::not_found("File not found"));
            }
        };
        if !self.can_access_path(&file_metadata.owner, &claims) {
            self.audit_failure(
                "FILE_GET_METADATA",
                Some(file_path.clone()),
                "FORBIDDEN",
                claims.sub.clone(),
                json!({}),
            )
            .await;
            error!(
                "User {} is not allowed to access metadata for file {}",
                claims.sub, file_path
            );
            return Err(Status::permission_denied("Not allowed to access this file"));
        }
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
        self.audit_success(
            "FILE_GET_METADATA",
            Some(file_path.to_string()),
            claims.sub.clone(),
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

    #[instrument(skip(self, request))]
    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        self.ensure_leader_for_write().await?;
        info!("Received delete_file request");
        let claims = self.authorize_user_or_master(request.metadata())?;
        let file_path = request.into_inner().path;
        let existing_file = {
            let files = self.state.files.read().await;
            files.get(&file_path).cloned()
        };

        if let Some(existing) = existing_file.as_ref() {
            if !self.can_access_path(&existing.owner, &claims) {
                self.audit_failure(
                    "FILE_DELETE",
                    Some(file_path.clone()),
                    "FORBIDDEN",
                    claims.sub.clone(),
                    json!({}),
                )
                .await;
                return Err(Status::permission_denied("Not allowed to delete this file"));
            }
        }

        if let Some(filemetadata) = existing_file {
            self.raft_write(MetadataCommand::DeleteFile {
                path: file_path.clone(),
            })
            .await?;

            let block_count = filemetadata.blocks.len();
            let to_delete = filemetadata.blocks.clone();
            self.garbage_collector_tx.send(to_delete).await.ok();
            info!(
                file_path,
                block_count, "File deleted, blocks queued for garbage collection"
            );
            self.audit_success(
                "FILE_DELETE",
                Some(file_path.to_string()),
                claims.sub.clone(),
                json!({
                    "queued_blocks": block_count,
                }),
            )
            .await;

            Ok(Response::new(DeleteFileResponse {
                success: true,
                error_message: "".to_string(),
            }))
        } else {
            warn!(file_path, "Attempted to delete non-existent file");
            self.audit_failure(
                "FILE_DELETE",
                Some(file_path.to_string()),
                "NOT_FOUND",
                claims.sub.clone(),
                json!({}),
            )
            .await;
            return Err(Status::not_found("File not found"));
        }
    }

    #[instrument(skip(self, request))]
    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        info!("Received list_directory request");
        let claims = self.authorize_user_or_master(request.metadata())?;
        let mut dir_path = request.into_inner().path;
        if !dir_path.ends_with('/') {
            dir_path.push('/');
        }
        let files_lock = self.state.files.read().await;
        let files = match files_lock.get_raw_descendant(&dir_path) {
            Some(sub_trie) => {
                sub_trie
                    .iter()
                    .filter(|(path, file)| {
                        path.starts_with(&dir_path)
                            && (claims.role == common::ROLE_MASTER || file.owner == claims.sub)
                    })
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
                    .collect()
            }
            None => Vec::new(),
        };
        info!(
            dir_path,
            file_count = files.len(),
            "Directory listing completed"
        );
        self.audit_success(
            "DIRECTORY_LIST",
            Some(dir_path.to_string()),
            claims.sub.clone(),
            json!({
                "file_count": files.len(),
            }),
        )
        .await;
        Ok(Response::new(ListDirectoryResponse { files }))
    }

    #[instrument(skip(self, request))]
    async fn rename_file(
        &self,
        request: Request<RenameFileRequest>,
    ) -> Result<Response<RenameFileResponse>, Status> {
        self.ensure_leader_for_write().await?;
        info!("Received rename_file request");
        let claims = self.authorize_user_or_master(request.metadata())?;

        let rename_req = request.into_inner();
        let files_lock = self.state.files.read().await;
        if files_lock.get(&rename_req.new_path).is_some() {
            warn!(
                new_path = rename_req.new_path,
                "Rename target already exists"
            );
            self.audit_failure(
                "FILE_RENAME",
                Some(rename_req.old_path.clone()),
                "ALREADY_EXISTS",
                claims.sub.clone(),
                json!({
                    "new_path": rename_req.new_path,
                }),
            )
            .await;
            return Err(Status::already_exists(format!(
                "File {} already exists",
                rename_req.new_path
            )));
        }

        if let Some(existing) = files_lock.get(&rename_req.old_path) {
            if !self.can_access_path(&existing.owner, &claims) {
                self.audit_failure(
                    "FILE_RENAME",
                    Some(rename_req.old_path.clone()),
                    "FORBIDDEN",
                    claims.sub.clone(),
                    json!({
                        "new_path": rename_req.new_path,
                    }),
                )
                .await;
                return Err(Status::permission_denied("Not allowed to rename this file"));
            }
        } else {
            warn!(old_path = rename_req.old_path, "File not found for rename");
            self.audit_failure(
                "FILE_RENAME",
                Some(rename_req.old_path.clone()),
                "NOT_FOUND",
                claims.sub.clone(),
                json!({
                    "new_path": rename_req.new_path,
                }),
            )
            .await;
            return Err(Status::not_found("Couldnt rename file"));
        }
        drop(files_lock);

        self.raft_write(MetadataCommand::RenameFile {
            old_path: rename_req.old_path.clone(),
            new_path: rename_req.new_path.clone(),
        })
        .await?;

        info!(
            old_path = rename_req.old_path,
            new_path = rename_req.new_path,
            "File renamed successfully"
        );

        self.audit_success(
            "FILE_RENAME",
            Some(rename_req.old_path.to_string()),
            claims.sub.clone(),
            json!({
                "new_path": rename_req.new_path,
            }),
        )
        .await;

        Ok(Response::new(RenameFileResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.ensure_leader_for_write().await?;
        let claims = self.authorize_datanode(request.metadata())?;
        debug!(subject = %claims.sub, "Authenticated datanode heartbeat");
        let node_request = request.into_inner();
        let remote_addr = node_request.addr.clone();

        // If node_id is 0, this is a new datanode registering
        if node_request.node_id == 0 {
            let new_id = self.next_node_id.fetch_add(1, Ordering::SeqCst);
            info!(assigned_node_id = new_id, "New datanode registered");

            // Extract IP from remote_addr and use standard datanode port
            let datanode_addr = format!("http://{remote_addr}");
            let last_beat = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as i64;

            self.raft_write(MetadataCommand::DatanodeRegister {
                id: new_id,
                addr: datanode_addr.clone(),
                last_beat,
            })
            .await?;

            self.audit_success(
                "DATANODE_REGISTER",
                Some(format!("node:{new_id}")),
                claims.sub.clone(),
                json!({
                    "assigned_node_id": new_id,
                    "datanode_addr": datanode_addr,
                    "cpu_usage": node_request.cpu_usage,
                    "free_space": node_request.free_space,
                    "used_space": node_request.used_space,
                }),
            )
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
        let node_id = node_request.node_id as u16;

        if self
            .state
            .available_datanodes
            .read()
            .await
            .get(&node_id)
            .is_some()
        {
            let last_beat = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as i64;

            self.raft_write(MetadataCommand::DatanodeHeartbeat {
                id: node_id,
                last_beat,
            })
            .await?;
        } else {
            audit_success = false;
            audit_status = "UNKNOWN_NODE";
            warn!(
                node_id = node_request.node_id,
                "Heartbeat from unknown node"
            );
        }
        if audit_success {
            self.audit_success(
                "DATANODE_HEARTBEAT",
                Some(format!("node:{}", node_request.node_id)),
                claims.sub.clone(),
                json!({
                    "cpu_usage": node_request.cpu_usage,
                    "free_space": node_request.free_space,
                    "used_space": node_request.used_space,
                    "addr": node_request.addr,
                }),
            )
            .await;
        } else {
            self.audit_failure(
                "DATANODE_HEARTBEAT",
                Some(format!("node:{}", node_request.node_id)),
                audit_status,
                claims.sub.clone(),
                json!({
                    "cpu_usage": node_request.cpu_usage,
                    "free_space": node_request.free_space,
                    "used_space": node_request.used_space,
                    "addr": node_request.addr,
                }),
            )
            .await;
        }
        Ok(Response::new(HeartbeatResponse {
            acknowledgement: true,
            assigned_node_id: node_request.node_id,
        }))
    }

    async fn set_file_status(
        &self,
        request: Request<SetFileStatusRequest>,
    ) -> Result<Response<SetFileStatusResponse>, Status> {
        self.ensure_leader_for_write().await?;
        self.authorize_user_or_master(request.metadata())?;
        let set_file_request = request.into_inner();
        self.update_file_status_internal(set_file_request.path, set_file_request.status)
            .await?;
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
        debug!(block_id, datanode, "Requesting datanode to delete block");
        let mut client = match data_node_service_client::DataNodeServiceClient::connect(
            datanode.clone(),
        )
        .await
        {
            Ok(c) => c,
            Err(err) => {
                error!(block_id, datanode, "Could not connect to datanode: {}", err);
                self.audit_failure(
                    "BLOCK_DELETE",
                    Some(block_id.to_string()),
                    "CONNECTION_ERROR",
                    "MASTER".to_string(),
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
            .delete_block({
                let mut req = Request::new(DeleteBlockRequest {
                    block_id: block_id.to_string(),
                });
                let token = self
                    .auth
                    .create_master_service_token()
                    .map_err(|err| format!("Could not create service token: {err}"))?;
                common::attach_bearer_token(&mut req, &token)
                    .map_err(|err| format!("Could not attach token metadata: {err}"))?;
                req
            })
            .await
        {
            Ok(_) => {
                debug!(block_id, datanode, "Block deletion confirmed by datanode");
                self.audit_success(
                    "BLOCK_DELETE",
                    Some(block_id.to_string()),
                    "MASTER".to_string(),
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
                self.audit_failure(
                    "BLOCK_DELETE",
                    Some(block_id.to_string()),
                    "DELETE_ERROR",
                    "MASTER".to_string(),
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

    async fn notify_datanodes_to_replicate(
        &self,
        mapping: HashMap<u16, HashMap<u16, Vec<String>>>,
    ) -> Result<bool, String> {
        debug!("Notifying datanodes");
        let datanodes = self.state.available_datanodes.read().await;
        let max_parallel_src = self.settings.replication_max_parallel_sources.max(1);
        let sem = Arc::new(Semaphore::new(max_parallel_src));
        let mut set = JoinSet::new();
        for (datanode_src, targets) in mapping {
            let src_addr = match datanodes.get(&datanode_src) {
                Some(info) => info.addr.clone(),
                None => {
                    warn!(
                        datanode_src,
                        "Source DataNode not available for replication"
                    );
                    continue;
                }
            };

            let block_locations: HashMap<String, BlockList> = targets
                .into_iter()
                .filter_map(|(dst_id, block_ids)| {
                    datanodes
                        .get(&dst_id)
                        .map(|info| (info.addr.clone(), BlockList { block_ids }))
                })
                .collect();

            if block_locations.is_empty() {
                continue;
            }

            // drop(datanodes);
            debug!("Connecting to datanode {src_addr}");

            let mut client =
                match data_node_service_client::DataNodeServiceClient::connect(src_addr.clone())
                    .await
                {
                    Ok(c) => c,
                    Err(err) => {
                        error!(datanode_src, "Could not connect to datanode: {}", err);
                        self.audit_failure(
                            "BLOCK_REPLICATE",
                            Some(format!("datanode:{datanode_src}")),
                            "CONNECTION_ERROR",
                            "MASTER".to_string(),
                            json!({
                                "datanode": datanode_src,
                                "error": err.to_string(),
                            }),
                        )
                        .await;
                        return Err(format!("Could not connect to datanode: {err}"));
                    }
                };
            let mut request = Request::new(ReplicateBlocksRequest { block_locations });
            let token = self
                .auth
                .create_master_service_token()
                .map_err(|err| format!("Could not create service token: {err}"))?;
            common::attach_bearer_token(&mut request, &token)
                .map_err(|err| format!("Could not attach token metadata: {err}"))?;
            let permit = sem.clone().acquire_owned().await.unwrap();
            let this = self.clone();
            set.spawn(async move {
                let _permit = permit;
                match client.replicate_blocks(request).await {
                    Ok(_) => {
                        debug!(datanode_src, "Block replication confirmed by datanode");
                        this.audit_success(
                            "BLOCK_REPLICATE",
                            Some(format!("datanode:{datanode_src}")),
                            "MASTER".to_string(),
                            json!({
                                "datanode_src": datanode_src,
                            }),
                        )
                        .await;
                    }
                    Err(err) => {
                        error!(
                            datanode_src,
                            "Failed to replicate block on datanode: {}", err
                        );
                        this.audit_failure(
                            "BLOCK_REPLICATE",
                            Some(format!("datanode:{datanode_src}")),
                            "REPLICATION_ERROR",
                            "MASTER".to_string(),
                            json!({
                                "datanode_src": datanode_src,
                                "error": err.to_string(),
                            }),
                        )
                        .await;
                    }
                }
            });
        }
        while let Some(res) = set.join_next().await {
            match res {
                Ok(()) => {}
                Err(join_err) => {
                    error!("Task join error: {}", join_err);
                    return Err(join_err.to_string());
                }
            }
        }
        Ok(true)
    }

    async fn audit_success(
        &self,
        action: &'static str,
        resource: Option<String>,
        user: String,
        details: Value,
    ) {
        self.audit(action, resource, true, "SUCCESS", user, details)
            .await;
    }

    async fn audit_failure(
        &self,
        action: &'static str,
        resource: Option<String>,
        status: &'static str,
        user: String,
        details: Value,
    ) {
        self.audit(action, resource, false, status, user, details)
            .await;
    }

    async fn audit(
        &self,
        action: impl Into<String>,
        resource: Option<String>,
        success: bool,
        status: impl Into<String>,
        user: impl Into<String>,
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
                user: user.into(),
                details,
            })
            .await
            .ok();
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

fn block_replicator(
    mut block_replicator_rx: mpsc::Receiver<FileMetadata>,
    master_ref: Arc<Metadatanode>,
) {
    tokio::spawn(async move {
        info!("Block replicator started");
        while let Some(file_metadata) = block_replicator_rx.recv().await {
            debug!("Got file to replicate");
            // Capture the id now; the path may change if the file is renamed
            // while replication is in progress.
            let file_id = file_metadata.id.clone();
            let mut mapping = HashMap::<u16, HashMap<u16, Vec<String>>>::new(); //Datanode_src:[Datanode_dst: [Blocks]]
            for block in file_metadata.blocks {
                let mut locations = block.locations.iter();
                let block_location = match locations.next() {
                    Some(loc) => *loc,
                    None => {
                        warn!("Block {} has no locations, skipping replication", block.id);
                        continue;
                    }
                };
                for dst_node in locations {
                    mapping
                        .entry(block_location)
                        .or_default()
                        .entry(*dst_node)
                        .or_default()
                        .push(block.id.clone());
                }
            }
            let _ = master_ref.notify_datanodes_to_replicate(mapping).await;
            if let Err(e) = master_ref
                .update_file_status_by_id(&file_id, FileStatus::Replicated.to_string())
                .await
            {
                warn!("Could not set file {} status to Replicated: {}", file_id, e);
            }
        }
    });
}

fn log_sender(mut audit_rx: mpsc::Receiver<AuditLog>, allow_invalid_certs: bool) {
    let es_url = env::var("ELASTIC_URL").expect("URL not defined");
    let es_user = env::var("ELASTIC_USER").expect("User not defined");
    let es_pass = env::var("ELASTIC_PASSWORD").expect("Pass not defined");

    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(allow_invalid_certs)
            .build()
            .expect("Failed to build client");
        info!(
            "Audit Logger started, sending logs to ElasticSearch at {}",
            es_url
        );

        while let Some(log) = audit_rx.recv().await {
            debug!("Sending log");
            let res = client
                .post(&es_url)
                .basic_auth(&es_user, Some(&es_pass))
                .json(&log)
                .send()
                .await;

            match res {
                Ok(response) => debug!("Respuesta Elastic: {:?}", response.status()),
                Err(e) => error!("Error enviando log: {:?}", e),
            }
        }
    });
}

#[allow(clippy::type_complexity)]
fn replay_wal(
    wal_path: &str,
) -> Result<(Trie<String, FileMetadata>, HashMap<u16, DatanodeInfo>, u16), std::io::Error> {
    let mut files = Trie::<String, FileMetadata>::new();
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
                    metadata.name = new_path.split('/').next_back().unwrap_or("").to_string();
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
    let settings = Arc::new(load_config());
    dotenvy::from_path(std::path::Path::new(&settings.env_file_path)).ok();

    common::init_tracing("master", &settings.log_level);

    info!("Configuration loaded: {:?}", settings);

    if settings.jwt_secret == DEFAULT_JWT_SECRET {
        warn!("Using default JWT secret. Set jwt_secret in config.toml for production use");
    }

    if let Some(parent) = Path::new(&settings.auth_db_path).parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }

    let auth_manager = Arc::new(AuthManager::new(
        settings.auth_db_path.clone(),
        settings.jwt_secret.clone(),
        settings.jwt_exp_seconds,
        settings.service_token_exp_seconds,
    )?);

    let (files, available_data_nodes, next_node_id) = replay_wal(&settings.wal_path)?;
    info!(
        "WAL replay completed: files={}, datanodes={}, next_node_id={}",
        files.len(),
        available_data_nodes.len(),
        next_node_id
    );

    let shared_state = Arc::new(MasterState {
        files: RwLock::new(files),
        available_datanodes: RwLock::new(available_data_nodes),
    });

    let raft_ctx =
        Arc::new(MasterRaftContext::bootstrap(&settings, Arc::clone(&shared_state)).await?);

    let (garbage_collector_tx, garbage_collector_rx) =
        mpsc::channel::<Vec<BlockMetadata>>(settings.garbage_collector_channel_size.max(1));
    let (block_replicator_tx, block_replicator_rx) =
        mpsc::channel::<FileMetadata>(settings.block_replicator_channel_size.max(1));
    let (audit_tx, audit_rx) = mpsc::channel::<AuditLog>(settings.audit_channel_size.max(1)); // Buffer grande

    let metadatanode = Arc::new(Metadatanode {
        state: shared_state,
        settings: Arc::clone(&settings),
        garbage_collector_tx,
        block_replicator_tx,
        audit_tx,
        auth: auth_manager,
        next_node_id: AtomicU16::new(next_node_id),
        raft_ctx: Arc::clone(&raft_ctx),
    });

    let address = settings.listen_address.parse()?;
    let service = MetadataServiceServer::new(metadatanode.as_ref().clone());
    let raft_service = RaftServiceServer::new(RaftRpcNode::new(Arc::clone(&raft_ctx)));

    info!("Starting MetadataNode server on {}", address);
    info!("Datanodes will self-register via heartbeat");
    
    let master_ref_gc = Arc::clone(&metadatanode);
    garbage_collector(garbage_collector_rx, master_ref_gc);
    log_sender(audit_rx, settings.elastic_allow_invalid_certs);
    let master_ref_br = Arc::clone(&metadatanode);
    block_replicator(block_replicator_rx, master_ref_br);

    // Spawn membership manager to automatically manage cluster membership
    let membership_manager = MembershipManager::new(
        Arc::clone(&raft_ctx),
        Arc::clone(&settings),
    );
    membership_manager.spawn();

    Server::builder()
        .add_service(service)
        .add_service(raft_service)
        .serve(address)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
