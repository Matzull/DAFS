//! Unit tests for the Master/Metadata service

use super::*;
use common::dfs::metadata_service_server::MetadataService;

// ==================== Test Helpers ====================

pub fn create_test_metadatanode() -> Metadatanode {
    let mut datanodes = HashMap::new();
    datanodes.insert(1, "http://node1:8080".to_string());
    datanodes.insert(2, "http://node2:8080".to_string());
    datanodes.insert(3, "http://node3:8080".to_string());

    Metadatanode {
        state: Arc::new(MasterState {
            files: RwLock::new(HashMap::new()),
            available_datanodes: RwLock::new(datanodes),
        }),
    }
}

pub fn create_metadatanode_without_datanodes() -> Metadatanode {
    Metadatanode {
        state: Arc::new(MasterState {
            files: RwLock::new(HashMap::new()),
            available_datanodes: RwLock::new(HashMap::new()),
        }),
    }
}

// ==================== _create_file Tests ====================

#[tokio::test]
async fn test_create_file_single_block() {
    let node = create_test_metadatanode();

    let result = node
        ._create_file(&node.state, "/test/small.txt".to_string(), 1024)
        .await;

    assert!(result.is_ok());
    let file = result.unwrap();
    assert_eq!(file.path, "/test/small.txt");
    assert_eq!(file.name, "small.txt");
    assert_eq!(file.size, 1024);
    assert_eq!(file.block_count(), 1);
    assert!(file.is_uploading());
}

#[tokio::test]
async fn test_create_file_multiple_blocks() {
    let node = create_test_metadatanode();

    // Create a file that requires 3 blocks (BLOCK_SIZE = 64KB)
    let file_size = BLOCK_SIZE * 3 - 100; // Just under 3 full blocks
    let result = node
        ._create_file(&node.state, "/test/large.txt".to_string(), file_size)
        .await;

    assert!(result.is_ok());
    let file = result.unwrap();
    assert_eq!(file.block_count(), 3);

    // Check block sizes
    assert_eq!(file.blocks[0].size, BLOCK_SIZE);
    assert_eq!(file.blocks[1].size, BLOCK_SIZE);
    assert_eq!(file.blocks[2].size, file_size - BLOCK_SIZE * 2);
}

#[tokio::test]
async fn test_create_file_exact_block_boundary() {
    let node = create_test_metadatanode();

    // Create a file that is exactly 2 blocks
    let file_size = BLOCK_SIZE * 2;
    let result = node
        ._create_file(&node.state, "/test/exact.txt".to_string(), file_size)
        .await;

    assert!(result.is_ok());
    let file = result.unwrap();
    assert_eq!(file.block_count(), 2);
    assert_eq!(file.blocks[0].size, BLOCK_SIZE);
    assert_eq!(file.blocks[1].size, BLOCK_SIZE);
}

#[tokio::test]
async fn test_create_file_zero_size() {
    let node = create_test_metadatanode();

    let result = node
        ._create_file(&node.state, "/test/empty.txt".to_string(), 0)
        .await;

    assert!(result.is_ok());
    let file = result.unwrap();
    assert_eq!(file.size, 0);
    assert_eq!(file.block_count(), 0);
}

#[tokio::test]
async fn test_create_file_negative_size_fails() {
    let node = create_test_metadatanode();

    let result = node
        ._create_file(&node.state, "/test/negative.txt".to_string(), -100)
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("negative"));
}

#[tokio::test]
async fn test_create_file_no_datanodes_fails() {
    let node = create_metadatanode_without_datanodes();

    let result = node
        ._create_file(&node.state, "/test/file.txt".to_string(), 1024)
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No datanodes"));
}

#[tokio::test]
async fn test_create_file_duplicate_fails() {
    let node = create_test_metadatanode();

    // Create first file
    let result1 = node
        ._create_file(&node.state, "/test/duplicate.txt".to_string(), 1024)
        .await;
    assert!(result1.is_ok());

    // Try to create the same file again
    let result2 = node
        ._create_file(&node.state, "/test/duplicate.txt".to_string(), 2048)
        .await;
    assert!(result2.is_err());
    assert!(result2.unwrap_err().contains("already exists"));
}

#[tokio::test]
async fn test_create_file_block_replication() {
    let node = create_test_metadatanode();

    let result = node
        ._create_file(&node.state, "/test/replicated.txt".to_string(), 1024)
        .await;

    assert!(result.is_ok());
    let file = result.unwrap();

    // Each block should have REPLICATION number of locations (or less if fewer datanodes)
    for block in &file.blocks {
        assert!(block.locations.len() <= REPLICATION as usize);
        assert!(!block.locations.is_empty());
    }
}

#[tokio::test]
async fn test_create_file_unique_ids() {
    let node = create_test_metadatanode();

    let file1 = node
        ._create_file(&node.state, "/test/file1.txt".to_string(), 1024)
        .await
        .unwrap();

    let file2 = node
        ._create_file(&node.state, "/test/file2.txt".to_string(), 1024)
        .await
        .unwrap();

    // File IDs should be unique
    assert_ne!(file1.id, file2.id);

    // Block IDs should be unique
    assert_ne!(file1.blocks[0].id, file2.blocks[0].id);
}

// ==================== gRPC Service Tests ====================

#[tokio::test]
async fn test_grpc_create_file() {
    let node = create_test_metadatanode();

    let request = Request::new(CreateFileRequest {
        path: "/grpc/test.txt".to_string(),
        size: BLOCK_SIZE * 2,
        user_id: "user1".to_string(),
    });

    let response = node.create_file(request).await;

    assert!(response.is_ok());
    let resp = response.unwrap().into_inner();
    assert!(!resp.file_id.is_empty());
    assert_eq!(resp.blocks.len(), 2);
}

#[tokio::test]
async fn test_grpc_get_file_metadata() {
    let node = create_test_metadatanode();

    // First create a file
    let create_req = Request::new(CreateFileRequest {
        path: "/grpc/get_test.txt".to_string(),
        size: 1024,
        user_id: "user1".to_string(),
    });
    let create_resp = node.create_file(create_req).await.unwrap().into_inner();

    // Now get the file metadata
    let get_req = Request::new(GetFileRequest {
        path: "/grpc/get_test.txt".to_string(),
        user_id: "user1".to_string(),
    });
    let get_resp = node.get_file_metadata(get_req).await;

    assert!(get_resp.is_ok());
    let resp = get_resp.unwrap().into_inner();
    assert_eq!(resp.file_id, create_resp.file_id);
    assert_eq!(resp.size, 1024);
}

#[tokio::test]
async fn test_grpc_get_file_not_found() {
    let node = create_test_metadatanode();

    let get_req = Request::new(GetFileRequest {
        path: "/nonexistent/file.txt".to_string(),
        user_id: "user1".to_string(),
    });
    let result = node.get_file_metadata(get_req).await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_grpc_list_directory() {
    let node = create_test_metadatanode();

    // Create some files
    for i in 0..3 {
        let req = Request::new(CreateFileRequest {
            path: format!("/listdir/file{}.txt", i),
            size: 1024,
            user_id: "user1".to_string(),
        });
        node.create_file(req).await.unwrap();
    }

    // Also create a file in a different directory
    let other_req = Request::new(CreateFileRequest {
        path: "/other/file.txt".to_string(),
        size: 1024,
        user_id: "user1".to_string(),
    });
    node.create_file(other_req).await.unwrap();

    // List the directory
    let list_req = Request::new(ListDirectoryRequest {
        path: "/listdir".to_string(),
    });
    let list_resp = node.list_directory(list_req).await;

    assert!(list_resp.is_ok());
    let resp = list_resp.unwrap().into_inner();
    assert_eq!(resp.files.len(), 3);
}

#[tokio::test]
async fn test_grpc_list_empty_directory() {
    let node = create_test_metadatanode();

    let list_req = Request::new(ListDirectoryRequest {
        path: "/empty".to_string(),
    });
    let list_resp = node.list_directory(list_req).await;

    assert!(list_resp.is_ok());
    let resp = list_resp.unwrap().into_inner();
    assert!(resp.files.is_empty());
}

#[tokio::test]
async fn test_grpc_list_root_directory() {
    let node = create_test_metadatanode();

    // Create files in different directories
    let paths = vec!["/a/file1.txt", "/b/file2.txt", "/file3.txt"];
    for path in &paths {
        let req = Request::new(CreateFileRequest {
            path: path.to_string(),
            size: 1024,
            user_id: "user1".to_string(),
        });
        node.create_file(req).await.unwrap();
    }

    // List root
    let list_req = Request::new(ListDirectoryRequest {
        path: "/".to_string(),
    });
    let list_resp = node.list_directory(list_req).await;

    assert!(list_resp.is_ok());
    let resp = list_resp.unwrap().into_inner();
    assert_eq!(resp.files.len(), 3);
}

// ==================== Block Assignment Tests ====================

#[tokio::test]
async fn test_block_assignment_addresses() {
    let node = create_test_metadatanode();

    let request = Request::new(CreateFileRequest {
        path: "/block/test.txt".to_string(),
        size: BLOCK_SIZE * 2,
        user_id: "user1".to_string(),
    });

    let response = node.create_file(request).await.unwrap().into_inner();

    // All blocks should have valid node addresses
    for block in &response.blocks {
        assert!(!block.node_address.is_empty());
        assert!(block.node_address.starts_with("http://"));
    }
}

#[tokio::test]
async fn test_block_assignment_order() {
    let node = create_test_metadatanode();

    let request = Request::new(CreateFileRequest {
        path: "/order/test.txt".to_string(),
        size: BLOCK_SIZE * 5,
        user_id: "user1".to_string(),
    });

    let response = node.create_file(request).await.unwrap().into_inner();

    // Blocks should be ordered 0, 1, 2, 3, 4
    for (i, block) in response.blocks.iter().enumerate() {
        assert_eq!(block.order, i as i64);
    }
}

// ==================== Concurrency Tests ====================

#[tokio::test]
async fn test_concurrent_file_creation() {
    let node = Arc::new(create_test_metadatanode());

    let mut handles = vec![];
    for i in 0..20 {
        let node_clone = Arc::clone(&node);
        handles.push(tokio::spawn(async move {
            let req = Request::new(CreateFileRequest {
                path: format!("/concurrent/file{}.txt", i),
                size: 1024,
                user_id: format!("user{}", i),
            });
            node_clone.create_file(req).await
        }));
    }

    // All file creations should succeed
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    // Verify all files were created
    let files = node.state.files.read().await;
    assert_eq!(files.len(), 20);
}

#[tokio::test]
async fn test_concurrent_read_and_write() {
    let node = Arc::new(create_test_metadatanode());

    // Create some initial files
    for i in 0..5 {
        let req = Request::new(CreateFileRequest {
            path: format!("/rw/file{}.txt", i),
            size: 1024,
            user_id: "user1".to_string(),
        });
        node.create_file(req).await.unwrap();
    }

    let mut read_handles = vec![];
    let mut write_handles = vec![];

    // Spawn readers
    for _ in 0..10 {
        let node_clone = Arc::clone(&node);
        read_handles.push(tokio::spawn(async move {
            let req = Request::new(ListDirectoryRequest {
                path: "/rw".to_string(),
            });
            node_clone.list_directory(req).await
        }));
    }

    // Spawn writers
    for i in 5..15 {
        let node_clone = Arc::clone(&node);
        write_handles.push(tokio::spawn(async move {
            let req = Request::new(CreateFileRequest {
                path: format!("/rw/file{}.txt", i),
                size: 1024,
                user_id: "user1".to_string(),
            });
            node_clone.create_file(req).await
        }));
    }

    // All operations should complete
    for handle in read_handles {
        let _ = handle.await.unwrap();
    }
    for handle in write_handles {
        let _ = handle.await.unwrap();
    }
}
