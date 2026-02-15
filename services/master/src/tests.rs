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
        blocks: vec![BlockMetadata {
            id: "block-1".to_string(),
            index: 0,
            size: 128,
            checksum: "checksum".to_string(),
            locations: vec![1],
        }],
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
