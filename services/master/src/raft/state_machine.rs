use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::{Cursor, Error as IoError};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::storage::{
    LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta,
};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, StorageError,
    StoredMembership, ToStorageResult, Vote,
};
use radix_trie::{Trie, TrieCommon};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::models::{DatanodeInfo, FileStatus, MasterState};

use super::types::{MetadataCommand, MetadataResponse, NodeId, TypeConfig};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SnapshotState {
    files: Vec<(String, crate::models::FileMetadata)>,
    available_datanodes: HashMap<u16, DatanodeInfo>,
}

#[derive(Debug, Clone)]
struct StoreInner {
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    last_purged_log_id: Option<LogId<NodeId>>,
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    logs: BTreeMap<u64, Entry<TypeConfig>>,
    current_snapshot: Option<Snapshot<TypeConfig>>,
}

impl Default for StoreInner {
    fn default() -> Self {
        Self {
            vote: None,
            committed: None,
            last_purged_log_id: None,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            logs: BTreeMap::new(),
            current_snapshot: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MasterStateMachine {
    inner: Arc<RwLock<StoreInner>>,
    state: Arc<MasterState>,
}

impl Default for MasterStateMachine {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StoreInner::default())),
            state: Arc::new(MasterState {
                files: RwLock::new(Trie::new()),
                available_datanodes: RwLock::new(HashMap::new()),
            }),
        }
    }
}

impl MasterStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_state(state: Arc<MasterState>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(StoreInner::default())),
            state,
        }
    }

    fn in_range<RB>(range: &RB, index: u64) -> bool
    where
        RB: RangeBounds<u64>,
    {
        let start_ok = match range.start_bound() {
            Bound::Included(v) => index >= *v,
            Bound::Excluded(v) => index > *v,
            Bound::Unbounded => true,
        };

        let end_ok = match range.end_bound() {
            Bound::Included(v) => index <= *v,
            Bound::Excluded(v) => index < *v,
            Bound::Unbounded => true,
        };

        start_ok && end_ok
    }

    fn snapshot_id(last_applied: Option<LogId<NodeId>>) -> String {
        match last_applied {
            Some(log_id) => format!("{}-{}", log_id.leader_id, log_id.index),
            None => "empty-0".to_string(),
        }
    }

    async fn snapshot_state(&self) -> SnapshotState {
        let files_lock = self.state.files.read().await;
        let files = files_lock
            .iter()
            .map(|(path, file)| (path.clone(), file.clone()))
            .collect::<Vec<_>>();
        drop(files_lock);

        let datanodes = self.state.available_datanodes.read().await.clone();

        SnapshotState {
            files,
            available_datanodes: datanodes,
        }
    }
}

impl RaftLogReader<TypeConfig> for MasterStateMachine {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;

        Ok(inner
            .logs
            .iter()
            .filter_map(|(index, entry)| {
                if Self::in_range(&range, *index) {
                    Some(entry.clone())
                } else {
                    None
                }
            })
            .collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MasterStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let state = self.snapshot_state().await;

        let mut inner = self.inner.write().await;
        let data = serde_json::to_vec(&state)
            .map_err(IoError::other)
            .sto_res(|| (ErrorSubject::Snapshot(None), ErrorVerb::Write))?;

        let meta = SnapshotMeta {
            last_log_id: inner.last_applied_log,
            last_membership: inner.last_membership.clone(),
            snapshot_id: Self::snapshot_id(inner.last_applied_log),
        };

        let snapshot = Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data)),
        };

        inner.current_snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }
}

impl RaftStorage<TypeConfig> for MasterStateMachine {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.committed)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.read().await;

        let last_log_id = inner
            .logs
            .iter()
            .next_back()
            .map(|(_, ent)| ent.log_id)
            .or(inner.last_purged_log_id);

        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut inner = self.inner.write().await;

        for entry in entries {
            inner.logs.insert(entry.log_id.index, entry);
        }

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.logs.retain(|idx, _| *idx < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.logs.retain(|idx, _| *idx > log_id.index);
        inner.last_purged_log_id = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.read().await;
        Ok((inner.last_applied_log, inner.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<MetadataResponse>, StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            let result = match &entry.payload {
                EntryPayload::Blank => MetadataResponse {
                    success: true,
                    data: None,
                },
                EntryPayload::Membership(membership) => {
                    inner.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    MetadataResponse {
                        success: true,
                        data: None,
                    }
                }
                EntryPayload::Normal(command) => {
                    match command {
                        MetadataCommand::CreateFile { file } => {
                            let mut files = self.state.files.write().await;
                            files.insert(file.path.clone(), file.clone());
                        }
                        MetadataCommand::DeleteFile { path } => {
                            let mut files = self.state.files.write().await;
                            files.remove(path);
                        }
                        MetadataCommand::RenameFile { old_path, new_path } => {
                            let mut files = self.state.files.write().await;
                            if let Some(mut metadata) = files.remove(old_path) {
                                metadata.path = new_path.clone();
                                metadata.name =
                                    new_path.split('/').next_back().unwrap_or("").to_string();
                                files.insert(new_path.clone(), metadata);
                            }
                        }
                        MetadataCommand::SetFileStatus { path, status } => {
                            let mut files = self.state.files.write().await;
                            if let Some(file_metadata) = files.get_mut(path) {
                                file_metadata.file_status = FileStatus::str_to_tok(status);
                            }
                        }
                        MetadataCommand::DatanodeRegister {
                            id,
                            addr,
                            last_beat,
                        } => {
                            let mut datanodes = self.state.available_datanodes.write().await;
                            datanodes.insert(
                                *id,
                                DatanodeInfo {
                                    id: *id,
                                    addr: addr.clone(),
                                    last_heartbeat: *last_beat,
                                },
                            );
                        }
                        MetadataCommand::DatanodeHeartbeat { id, last_beat } => {
                            let mut datanodes = self.state.available_datanodes.write().await;
                            if let Some(datanode) = datanodes.get_mut(id) {
                                datanode.last_heartbeat = *last_beat;
                            }
                        }
                    }

                    MetadataResponse {
                        success: true,
                        data: None,
                    }
                }
            };

            inner.last_applied_log = Some(entry.log_id);
            responses.push(result);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = (*snapshot).into_inner();

        let decoded: SnapshotState = serde_json::from_slice(&data)
            .map_err(IoError::other)
            .sto_res(|| (ErrorSubject::StateMachine, ErrorVerb::Read))?;

        let mut files = Trie::new();
        for (path, file) in decoded.files {
            files.insert(path, file);
        }
        *self.state.files.write().await = files;
        *self.state.available_datanodes.write().await = decoded.available_datanodes;

        let mut inner = self.inner.write().await;
        inner.last_applied_log = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data)),
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.current_snapshot.clone())
    }
}
