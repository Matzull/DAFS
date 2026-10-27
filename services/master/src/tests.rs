use super::*;
use std::io::Write;

fn sample_file(path: &str, status: FileStatus) -> FileMetadata {
    FileMetadata {
        id: "file-id-1".to_string(),
        name: path.split('/').next_back().unwrap_or("").to_string(),
        path: path.to_string(),
        size: 128,
        owner: "SYSTEM".to_string(),
        created_at: 1,
        last_modified_at: 2,
        file_status: status,
        is_directory: false,
        blocks: vec![BlockMetadata {
            id: "block-1".to_string(),
            index: 0,
            size: 128,
            checksum: "checksum".to_string(),
            locations: vec![1],
        }],
        mode: 0o644,
    }
}

#[test]
fn replay_wal_missing_file_returns_empty_state() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let wal_path = tmp_dir.path().join("does-not-exist.wal");

    let (files, datanodes, next_node_id) =
        replay_wal(wal_path.to_str().expect("temp path should be valid UTF-8"))
            .expect("replay should succeed");

    assert!(files.is_empty());
    assert!(datanodes.is_empty());
    assert_eq!(next_node_id, 1);
}

#[test]
fn replay_wal_applies_mutating_operations_in_order() {
    let tmp_file = tempfile::NamedTempFile::new().expect("named temp file");
    let wal_path = tmp_file.path().to_path_buf();

    let records = vec![
        WalRecord {
            schema_version: 1,
            op_id: "op-create".to_string(),
            timestamp_ms: 100,
            operation: WalOperation::CreateFile {
                file: sample_file("/tmp/a.txt", FileStatus::Uploading),
            },
        },
        WalRecord {
            schema_version: 1,
            op_id: "op-status".to_string(),
            timestamp_ms: 101,
            operation: WalOperation::SetFileStatus {
                path: "/tmp/a.txt".to_string(),
                status: "Ready".to_string(),
            },
        },
        WalRecord {
            schema_version: 1,
            op_id: "op-rename".to_string(),
            timestamp_ms: 102,
            operation: WalOperation::RenameFile {
                old_path: "/tmp/a.txt".to_string(),
                new_path: "/tmp/b.txt".to_string(),
            },
        },
        WalRecord {
            schema_version: 1,
            op_id: "op-register".to_string(),
            timestamp_ms: 103,
            operation: WalOperation::RegisterDatanode {
                datanode: DatanodeInfo {
                    id: 7,
                    addr: "http://127.0.0.1:8080".to_string(),
                    last_heartbeat: 1234,
                },
            },
        },
    ];

    {
        let file = tmp_file.reopen().expect("reopen temp file");
        let mut writer = std::io::BufWriter::new(file);
        for record in records {
            writeln!(
                writer,
                "{}",
                serde_json::to_string(&record).expect("serialize wal record")
            )
            .expect("write wal line");
        }
        writer.flush().expect("flush wal file");
    }

    let (files, datanodes, next_node_id) =
        replay_wal(wal_path.to_str().expect("temp path should be valid UTF-8"))
            .expect("replay should succeed");

    let file = files.get("/tmp/b.txt").expect("renamed file should exist");
    assert_eq!(file.name, "b.txt");
    assert!(matches!(file.file_status, FileStatus::Ready));
    assert_eq!(
        datanodes.get(&7).expect("registered datanode").addr,
        "http://127.0.0.1:8080"
    );
    assert_eq!(next_node_id, 8);
}

#[test]
fn plan_blocks_splits_file_and_sizes_last_block() {
    let blocks = plan_blocks(2_500, 1_000, 1, &[1]);

    let sizes: Vec<i64> = blocks.iter().map(|b| b.size).collect();
    assert_eq!(sizes, vec![1_000, 1_000, 500]);
    assert_eq!(
        blocks.iter().map(|b| b.index).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert!(plan_blocks(0, 1_000, 1, &[1]).is_empty());
}

#[test]
fn plan_blocks_uses_distinct_replicas_capped_by_available_nodes() {
    let blocks = plan_blocks(3_000, 1_000, 2, &[10, 20, 30]);
    for block in &blocks {
        assert_eq!(block.locations.len(), 2);
        assert_ne!(block.locations[0], block.locations[1]);
    }
    // The first replica rotates so load is spread across nodes
    let primaries: Vec<u16> = blocks.iter().map(|b| b.locations[0]).collect();
    assert_eq!(primaries, vec![10, 20, 30]);

    let capped = plan_blocks(1_000, 1_000, 3, &[10]);
    assert_eq!(capped[0].locations, vec![10]);
}

fn test_auth() -> (tempfile::TempDir, AuthManager) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("users.db");
    let auth = AuthManager::new(
        db_path.to_str().expect("utf-8 path").to_string(),
        "test-secret".to_string(),
        3600,
        300,
    )
    .expect("auth manager");
    (dir, auth)
}

#[test]
fn auth_register_then_login_issues_user_token() {
    let (_dir, auth) = test_auth();
    auth.register_user("alice", "s3cret").expect("register");

    let (token, _) = auth.login_user("alice", "s3cret").expect("login");
    let claims = common::decode_access_token(&token, "test-secret").expect("valid token");
    assert_eq!(claims.sub, "alice");
    assert_eq!(claims.role, common::ROLE_USER);
}

#[test]
fn auth_rejects_wrong_password_and_duplicate_user() {
    let (_dir, auth) = test_auth();
    auth.register_user("alice", "s3cret").expect("register");

    assert!(auth.login_user("alice", "wrong").is_err());
    assert!(auth.login_user("bob", "s3cret").is_err());
    assert_eq!(
        auth.register_user("alice", "other").unwrap_err(),
        "Username already exists"
    );
}

mod state_machine {
    use crate::models::{FileStatus, MasterState};
    use crate::raft::state_machine::MasterStateMachine;
    use crate::raft::types::MetadataCommand;
    use openraft::storage::{RaftSnapshotBuilder, RaftStorage};
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
    use radix_trie::Trie;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn empty_state() -> Arc<MasterState> {
        Arc::new(MasterState {
            files: RwLock::new(Trie::new()),
            available_datanodes: RwLock::new(HashMap::new()),
        })
    }

    fn entries(commands: Vec<MetadataCommand>) -> Vec<Entry<crate::raft::types::TypeConfig>> {
        commands
            .into_iter()
            .enumerate()
            .map(|(i, command)| Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 0), i as u64 + 1),
                payload: EntryPayload::Normal(command),
            })
            .collect()
    }

    fn create(path: &str) -> MetadataCommand {
        MetadataCommand::CreateFile {
            file: super::sample_file(path, FileStatus::Uploading),
        }
    }

    #[tokio::test]
    async fn applies_namespace_commands() {
        let state = empty_state();
        let mut sm = MasterStateMachine::new_with_state(state.clone());

        sm.apply_to_state_machine(&entries(vec![
            create("/data/a.txt"),
            create("/data/gone.txt"),
            MetadataCommand::SetFileStatus {
                path: "/data/a.txt".to_string(),
                status: "Ready".to_string(),
            },
            MetadataCommand::RenameFile {
                old_path: "/data/a.txt".to_string(),
                new_path: "/data/b.txt".to_string(),
            },
            MetadataCommand::DeleteFile {
                path: "/data/gone.txt".to_string(),
            },
        ]))
        .await
        .expect("apply");

        let files = state.files.read().await;
        assert!(files.get("/data/a.txt").is_none());
        assert!(files.get("/data/gone.txt").is_none());
        let renamed = files.get("/data/b.txt").expect("renamed file");
        assert_eq!(renamed.name, "b.txt");
        assert_eq!(renamed.file_status, FileStatus::Ready);
    }

    #[tokio::test]
    async fn snapshot_restores_state_on_another_node() {
        let mut leader = MasterStateMachine::new_with_state(empty_state());
        leader
            .apply_to_state_machine(&entries(vec![
                create("/data/a.txt"),
                MetadataCommand::DatanodeRegister {
                    id: 3,
                    addr: "http://dn3:8080".to_string(),
                    last_beat: 42,
                },
            ]))
            .await
            .expect("apply");
        let snapshot = leader.build_snapshot().await.expect("build snapshot");

        let follower_state = empty_state();
        let mut follower = MasterStateMachine::new_with_state(follower_state.clone());
        follower
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        assert!(follower_state
            .files
            .read()
            .await
            .get("/data/a.txt")
            .is_some());
        let datanodes = follower_state.available_datanodes.read().await;
        assert_eq!(datanodes.get(&3).expect("datanode").addr, "http://dn3:8080");
        let (last_applied, _) = follower.last_applied_state().await.expect("state");
        assert_eq!(last_applied, snapshot.meta.last_log_id);
    }
}
