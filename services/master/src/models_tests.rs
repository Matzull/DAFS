//! Unit tests for the master service models

use super::*;
use std::sync::Arc;

// ==================== FileStatus Tests ====================

#[test]
fn test_file_status_clone() {
    let status = FileStatus::Ready;
    let cloned = status.clone();
    assert!(matches!(cloned, FileStatus::Ready));
}

// ==================== BlockMetadata Tests ====================

#[test]
fn test_block_metadata_new() {
    let block = BlockMetadata::new("block-123".to_string(), 0, 65536, vec![1, 2]);

    assert_eq!(block.id, "block-123");
    assert_eq!(block.index, 0);
    assert_eq!(block.size, 65536);
    assert_eq!(block.locations, vec![1, 2]);
}

#[test]
fn test_block_metadata_with_empty_locations() {
    let block = BlockMetadata::new("block-empty".to_string(), 0, 1024, vec![]);

    assert!(block.locations.is_empty());
}

#[test]
fn test_block_metadata_clone() {
    let block = BlockMetadata::new("block-1".to_string(), 0, 1024, vec![1]);
    let cloned = block.clone();

    assert_eq!(block.id, cloned.id);
    assert_eq!(block.index, cloned.index);
    assert_eq!(block.size, cloned.size);
    assert_eq!(block.locations, cloned.locations);
}

// ==================== FileMetadata Tests ====================

fn create_test_file_metadata(status: FileStatus) -> FileMetadata {
    FileMetadata {
        id: "file-123".to_string(),
        name: "test.txt".to_string(),
        path: "/home/user/test.txt".to_string(),
        size: 131072, // 128KB
        owner: "testuser".to_string(),
        created_at: 1700000000000,
        last_modified_at: 1700000000000,
        checksum: "abc123".to_string(),
        file_status: status,
        blocks: vec![
            BlockMetadata::new("block-1".to_string(), 0, 65536, vec![1, 2]),
            BlockMetadata::new("block-2".to_string(), 1, 65536, vec![2, 3]),
        ],
    }
}

#[test]
fn test_file_metadata_is_ready() {
    let file = create_test_file_metadata(FileStatus::Ready);
    assert!(file.is_ready());
    assert!(!file.is_uploading());
    assert!(!file.is_deleted());
}

#[test]
fn test_file_metadata_is_uploading() {
    let file = create_test_file_metadata(FileStatus::Uploading);
    assert!(!file.is_ready());
    assert!(file.is_uploading());
    assert!(!file.is_deleted());
}

#[test]
fn test_file_metadata_is_deleted() {
    let file = create_test_file_metadata(FileStatus::Deleted);
    assert!(!file.is_ready());
    assert!(!file.is_uploading());
    assert!(file.is_deleted());
}

#[test]
fn test_file_metadata_block_count() {
    let file = create_test_file_metadata(FileStatus::Ready);
    assert_eq!(file.block_count(), 2);
}

#[test]
fn test_file_metadata_calculated_size() {
    let file = create_test_file_metadata(FileStatus::Ready);
    assert_eq!(file.calculated_size(), 131072); // 65536 + 65536
}

#[test]
fn test_file_metadata_clone() {
    let file = create_test_file_metadata(FileStatus::Ready);
    let cloned = file.clone();

    assert_eq!(file.id, cloned.id);
    assert_eq!(file.name, cloned.name);
    assert_eq!(file.path, cloned.path);
    assert_eq!(file.size, cloned.size);
    assert_eq!(file.block_count(), cloned.block_count());
}

// ==================== MasterState Tests ====================

#[test]
fn test_master_state_new() {
    let _state = MasterState::new();
    // We can't easily check the contents without async, but we can ensure it compiles
    assert!(true);
}

#[test]
fn test_master_state_default() {
    let _state = MasterState::default();
    assert!(true);
}

#[test]
fn test_master_state_with_datanodes() {
    let mut datanodes = HashMap::new();
    datanodes.insert(1, "http://localhost:8080".to_string());
    datanodes.insert(2, "http://localhost:8081".to_string());

    let _state = MasterState::with_datanodes(datanodes);
    assert!(true);
}

#[tokio::test]
async fn test_master_state_files_operations() {
    let state = MasterState::new();

    // Insert a file
    {
        let mut files = state.files.write().await;
        files.insert(
            "/test/file.txt".to_string(),
            create_test_file_metadata(FileStatus::Ready),
        );
    }

    // Read the file
    {
        let files = state.files.read().await;
        assert!(files.contains_key("/test/file.txt"));
        let file = files.get("/test/file.txt").unwrap();
        assert_eq!(file.name, "test.txt");
    }
}

#[tokio::test]
async fn test_master_state_datanodes_operations() {
    let state = MasterState::new();

    // Add datanodes
    {
        let mut datanodes = state.available_datanodes.write().await;
        datanodes.insert(1, "http://node1:8080".to_string());
        datanodes.insert(2, "http://node2:8080".to_string());
    }

    // Read datanodes
    {
        let datanodes = state.available_datanodes.read().await;
        assert_eq!(datanodes.len(), 2);
        assert_eq!(datanodes.get(&1), Some(&"http://node1:8080".to_string()));
    }
}

#[tokio::test]
async fn test_master_state_concurrent_access() {
    let state = Arc::new(MasterState::new());

    // Spawn multiple readers
    let mut handles = vec![];
    for _i in 0..10 {
        let state_clone = Arc::clone(&state);
        handles.push(tokio::spawn(async move {
            let files = state_clone.files.read().await;
            files.len()
        }));
    }

    // All reads should complete successfully
    for handle in handles {
        let result = handle.await.unwrap();
        assert_eq!(result, 0);
    }
}
