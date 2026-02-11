//! Unit tests for the DataNode service

use super::*;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

// We need a way to inject the data folder for testing
// For now, we'll test the individual components

/// Helper to create test chunk data
fn create_test_chunk(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Helper to create a metadata message
fn create_metadata_msg(block_id: &str, total_size: i64) -> WriteBlockRequest {
    WriteBlockRequest {
        request_data: Some(RequestData::Metadata(common::dfs::BlockMetadata {
            block_id: block_id.to_string(),
            total_size,
        })),
    }
}

/// Helper to create a chunk data message
fn create_chunk_msg(data: Vec<u8>) -> WriteBlockRequest {
    WriteBlockRequest {
        request_data: Some(RequestData::ChunkData(data)),
    }
}

// ==================== Message Validation Tests ====================

#[test]
fn test_create_metadata_message() {
    let msg = create_metadata_msg("block-123", 1024);

    match msg.request_data {
        Some(RequestData::Metadata(meta)) => {
            assert_eq!(meta.block_id, "block-123");
            assert_eq!(meta.total_size, 1024);
        }
        _ => panic!("Expected metadata message"),
    }
}

#[test]
fn test_create_chunk_message() {
    let data = create_test_chunk(100);
    let msg = create_chunk_msg(data.clone());

    match msg.request_data {
        Some(RequestData::ChunkData(chunk)) => {
            assert_eq!(chunk.len(), 100);
            assert_eq!(chunk, data);
        }
        _ => panic!("Expected chunk data message"),
    }
}

#[test]
fn test_empty_message() {
    let msg = WriteBlockRequest { request_data: None };

    assert!(msg.request_data.is_none());
}

// ==================== Chunk Data Tests ====================

#[test]
fn test_chunk_data_various_sizes() {
    // Test different chunk sizes
    let sizes = vec![0, 1, 100, 1024, 64 * 1024, 64 * 1024 + 1];

    for size in sizes {
        let data = create_test_chunk(size);
        assert_eq!(data.len(), size);
    }
}

#[test]
fn test_chunk_data_pattern() {
    let data = create_test_chunk(512);

    // Verify the pattern repeats every 256 bytes
    for (i, &byte) in data.iter().enumerate() {
        assert_eq!(byte, (i % 256) as u8);
    }
}

// ==================== Datanode Struct Tests ====================

#[test]
fn test_datanode_default() {
    let _datanode = Datanode::default();
    // Just ensure it can be created
    assert!(true);
}

// ==================== Stream Simulation Tests ====================

#[tokio::test]
async fn test_stream_message_ordering() {
    let (tx, rx) = mpsc::channel::<WriteBlockRequest>(10);

    // Send messages in order
    tx.send(create_metadata_msg("block-1", 256)).await.unwrap();
    tx.send(create_chunk_msg(create_test_chunk(128)))
        .await
        .unwrap();
    tx.send(create_chunk_msg(create_test_chunk(128)))
        .await
        .unwrap();
    drop(tx);

    // Verify messages are received in order
    let mut stream = ReceiverStream::new(rx);

    let msg1 = stream.next().await.unwrap();
    assert!(matches!(msg1.request_data, Some(RequestData::Metadata(_))));

    let msg2 = stream.next().await.unwrap();
    assert!(matches!(msg2.request_data, Some(RequestData::ChunkData(_))));

    let msg3 = stream.next().await.unwrap();
    assert!(matches!(msg3.request_data, Some(RequestData::ChunkData(_))));

    let msg4 = stream.next().await;
    assert!(msg4.is_none());
}

#[tokio::test]
async fn test_stream_accumulates_bytes() {
    let (tx, rx) = mpsc::channel::<WriteBlockRequest>(10);

    let chunk_sizes = vec![100, 200, 300, 400];
    let total_size: usize = chunk_sizes.iter().sum();

    tx.send(create_metadata_msg("block-1", total_size as i64))
        .await
        .unwrap();
    for size in &chunk_sizes {
        tx.send(create_chunk_msg(create_test_chunk(*size)))
            .await
            .unwrap();
    }
    drop(tx);

    let mut stream = ReceiverStream::new(rx);
    let mut accumulated_bytes = 0;

    while let Some(msg) = stream.next().await {
        if let Some(RequestData::ChunkData(data)) = msg.request_data {
            accumulated_bytes += data.len();
        }
    }

    assert_eq!(accumulated_bytes, total_size);
}

// ==================== File I/O Tests ====================

#[tokio::test]
async fn test_file_write_and_read() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test_block");

    // Write data
    let test_data = create_test_chunk(1024);
    {
        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(&test_data).await.unwrap();
        file.sync_all().await.unwrap();
    }

    // Read data back
    let mut read_data = Vec::new();
    {
        let mut file = File::open(&file_path).await.unwrap();
        file.read_to_end(&mut read_data).await.unwrap();
    }

    assert_eq!(test_data, read_data);
}

#[tokio::test]
async fn test_file_chunked_read() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("chunked_block");

    // Write a larger file
    let test_data = create_test_chunk(256 * 1024); // 256KB
    {
        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(&test_data).await.unwrap();
        file.sync_all().await.unwrap();
    }

    // Read in chunks (simulating read_block behavior)
    let mut all_chunks = Vec::new();
    {
        let mut file = File::open(&file_path).await.unwrap();
        let mut buffer = [0u8; 64 * 1024]; // 64KB buffer

        loop {
            let n = file.read(&mut buffer).await.unwrap();
            if n == 0 {
                break;
            }
            all_chunks.extend_from_slice(&buffer[..n]);
        }
    }

    assert_eq!(test_data, all_chunks);
}

#[tokio::test]
async fn test_file_create_new_fails_if_exists() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("existing_block");

    // Create the file first
    File::create(&file_path).await.unwrap();

    // Try to create_new - should fail
    let result = File::create_new(&file_path).await;
    assert!(result.is_err());
}

// ==================== Response Tests ====================

#[test]
fn test_write_block_response_success() {
    let response = WriteBlockResponse {
        success: true,
        message: "Block written successfully".to_string(),
        bytes_written: 1024,
    };

    assert!(response.success);
    assert_eq!(response.bytes_written, 1024);
}

#[test]
fn test_write_block_response_failure() {
    let response = WriteBlockResponse {
        success: false,
        message: "Disk full".to_string(),
        bytes_written: 0,
    };

    assert!(!response.success);
    assert_eq!(response.bytes_written, 0);
}

#[test]
fn test_read_block_response() {
    let data = create_test_chunk(512);
    let response = ReadBlockResponse {
        chunk_data: data.clone(),
    };

    assert_eq!(response.chunk_data.len(), 512);
    assert_eq!(response.chunk_data, data);
}

// ==================== Path Tests ====================

#[test]
fn test_data_folder_path_construction() {
    let block_id = "test-block-123";
    let mut path = PathBuf::from(DATA_FOLDER);
    path.push(block_id);

    assert!(path.to_str().unwrap().contains(block_id));
    assert!(path.to_str().unwrap().contains("datanode"));
}

// ==================== Edge Case Tests ====================

#[tokio::test]
async fn test_empty_file_write() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("empty_block");

    // Write empty data
    {
        let file = File::create(&file_path).await.unwrap();
        file.sync_all().await.unwrap();
    }

    // Verify file exists and is empty
    let metadata = tokio::fs::metadata(&file_path).await.unwrap();
    assert_eq!(metadata.len(), 0);
}

#[tokio::test]
async fn test_large_chunk_handling() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("large_block");

    // Create a large chunk (1MB)
    let large_data = create_test_chunk(1024 * 1024);

    {
        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(&large_data).await.unwrap();
        file.sync_all().await.unwrap();
    }

    let metadata = tokio::fs::metadata(&file_path).await.unwrap();
    assert_eq!(metadata.len(), 1024 * 1024);
}

// ==================== Concurrent Access Tests ====================

#[tokio::test]
async fn test_concurrent_file_reads() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("concurrent_read");

    // Write test data
    let test_data = create_test_chunk(4096);
    {
        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(&test_data).await.unwrap();
        file.sync_all().await.unwrap();
    }

    // Spawn multiple concurrent readers
    let mut handles = vec![];
    for _ in 0..10 {
        let path = file_path.clone();
        let expected = test_data.clone();
        handles.push(tokio::spawn(async move {
            let mut file = File::open(&path).await.unwrap();
            let mut data = Vec::new();
            file.read_to_end(&mut data).await.unwrap();
            assert_eq!(data, expected);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

// ==================== Directory Tests ====================

#[tokio::test]
async fn test_create_dir_all_nested() {
    let temp_dir = TempDir::new().unwrap();
    let nested_path = temp_dir.path().join("a/b/c/d");

    create_dir_all(&nested_path).await.unwrap();

    assert!(nested_path.exists());
}

#[tokio::test]
async fn test_create_dir_all_idempotent() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("idempotent");

    // Create multiple times - should not fail
    create_dir_all(&path).await.unwrap();
    create_dir_all(&path).await.unwrap();
    create_dir_all(&path).await.unwrap();

    assert!(path.exists());
}
