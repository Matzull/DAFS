//! Unit tests for the common library (protobuf messages)

use super::dfs::*;

// ==================== CreateFileRequest Tests ====================

#[test]
fn test_create_file_request() {
    let request = CreateFileRequest {
        path: "/home/user/test.txt".to_string(),
        size: 1024,
        user_id: "user123".to_string(),
    };

    assert_eq!(request.path, "/home/user/test.txt");
    assert_eq!(request.size, 1024);
    assert_eq!(request.user_id, "user123");
}

#[test]
fn test_create_file_request_empty_path() {
    let request = CreateFileRequest {
        path: "".to_string(),
        size: 0,
        user_id: "".to_string(),
    };

    assert!(request.path.is_empty());
}

// ==================== CreateFileResponse Tests ====================

#[test]
fn test_create_file_response() {
    let response = CreateFileResponse {
        file_id: "file-uuid-123".to_string(),
        blocks: vec![
            BlockAssignment {
                block_id: "block-1".to_string(),
                node_address: "http://localhost:8080".to_string(),
                block_size: 65536,
                order: 0,
            },
            BlockAssignment {
                block_id: "block-2".to_string(),
                node_address: "http://localhost:8081".to_string(),
                block_size: 65536,
                order: 1,
            },
        ],
    };

    assert_eq!(response.file_id, "file-uuid-123");
    assert_eq!(response.blocks.len(), 2);
    assert_eq!(response.blocks[0].order, 0);
    assert_eq!(response.blocks[1].order, 1);
}

// ==================== BlockAssignment Tests ====================

#[test]
fn test_block_assignment() {
    let assignment = BlockAssignment {
        block_id: "block-abc".to_string(),
        node_address: "http://192.168.1.100:8080".to_string(),
        block_size: 65536,
        order: 5,
    };

    assert_eq!(assignment.block_id, "block-abc");
    assert_eq!(assignment.node_address, "http://192.168.1.100:8080");
    assert_eq!(assignment.block_size, 65536);
    assert_eq!(assignment.order, 5);
}

// ==================== GetFileRequest Tests ====================

#[test]
fn test_get_file_request() {
    let request = GetFileRequest {
        path: "/data/important.bin".to_string(),
        user_id: "admin".to_string(),
    };

    assert_eq!(request.path, "/data/important.bin");
    assert_eq!(request.user_id, "admin");
}

// ==================== GetFileResponse Tests ====================

#[test]
fn test_get_file_response() {
    let response = GetFileResponse {
        file_id: "file-xyz".to_string(),
        size: 1048576, // 1MB
        blocks: vec![],
    };

    assert_eq!(response.file_id, "file-xyz");
    assert_eq!(response.size, 1048576);
    assert!(response.blocks.is_empty());
}

// ==================== DeleteFileRequest Tests ====================

#[test]
fn test_delete_file_request() {
    let request = DeleteFileRequest {
        path: "/tmp/old_file.txt".to_string(),
        user_id: "cleaner".to_string(),
    };

    assert_eq!(request.path, "/tmp/old_file.txt");
    assert_eq!(request.user_id, "cleaner");
}

// ==================== DeleteFileResponse Tests ====================

#[test]
fn test_delete_file_response_success() {
    let response = DeleteFileResponse {
        success: true,
        error_message: "".to_string(),
    };

    assert!(response.success);
    assert!(response.error_message.is_empty());
}

#[test]
fn test_delete_file_response_failure() {
    let response = DeleteFileResponse {
        success: false,
        error_message: "File not found".to_string(),
    };

    assert!(!response.success);
    assert_eq!(response.error_message, "File not found");
}

// ==================== ListDirectoryRequest Tests ====================

#[test]
fn test_list_directory_request() {
    let request = ListDirectoryRequest {
        path: "/home/user".to_string(),
    };

    assert_eq!(request.path, "/home/user");
}

// ==================== ListDirectoryResponse Tests ====================

#[test]
fn test_list_directory_response() {
    let response = ListDirectoryResponse {
        files: vec![
            FileInfo {
                filename: "file1.txt".to_string(),
                is_directory: false,
                size: 1024,
            },
            FileInfo {
                filename: "subdir".to_string(),
                is_directory: true,
                size: 0,
            },
        ],
    };

    assert_eq!(response.files.len(), 2);
    assert!(!response.files[0].is_directory);
    assert!(response.files[1].is_directory);
}

// ==================== FileInfo Tests ====================

#[test]
fn test_file_info_file() {
    let info = FileInfo {
        filename: "document.pdf".to_string(),
        is_directory: false,
        size: 2048576,
    };

    assert_eq!(info.filename, "document.pdf");
    assert!(!info.is_directory);
    assert_eq!(info.size, 2048576);
}

#[test]
fn test_file_info_directory() {
    let info = FileInfo {
        filename: "photos".to_string(),
        is_directory: true,
        size: 0,
    };

    assert_eq!(info.filename, "photos");
    assert!(info.is_directory);
    assert_eq!(info.size, 0);
}

// ==================== WriteBlockRequest Tests ====================

#[test]
fn test_write_block_request_metadata() {
    let request = WriteBlockRequest {
        request_data: Some(write_block_request::RequestData::Metadata(BlockMetadata {
            block_id: "block-123".to_string(),
            total_size: 65536,
        })),
    };

    match request.request_data {
        Some(write_block_request::RequestData::Metadata(meta)) => {
            assert_eq!(meta.block_id, "block-123");
            assert_eq!(meta.total_size, 65536);
        }
        _ => panic!("Expected metadata"),
    }
}

#[test]
fn test_write_block_request_chunk() {
    let data = vec![1, 2, 3, 4, 5];
    let request = WriteBlockRequest {
        request_data: Some(write_block_request::RequestData::ChunkData(data.clone())),
    };

    match request.request_data {
        Some(write_block_request::RequestData::ChunkData(chunk)) => {
            assert_eq!(chunk, data);
        }
        _ => panic!("Expected chunk data"),
    }
}

#[test]
fn test_write_block_request_empty() {
    let request = WriteBlockRequest { request_data: None };

    assert!(request.request_data.is_none());
}

// ==================== BlockMetadata Tests ====================

#[test]
fn test_block_metadata() {
    let metadata = BlockMetadata {
        block_id: "blk-001".to_string(),
        total_size: 131072, // 128KB
    };

    assert_eq!(metadata.block_id, "blk-001");
    assert_eq!(metadata.total_size, 131072);
}

// ==================== WriteBlockResponse Tests ====================

#[test]
fn test_write_block_response_success() {
    let response = WriteBlockResponse {
        success: true,
        message: "Block written successfully".to_string(),
        bytes_written: 65536,
    };

    assert!(response.success);
    assert_eq!(response.bytes_written, 65536);
}

#[test]
fn test_write_block_response_failure() {
    let response = WriteBlockResponse {
        success: false,
        message: "Disk full".to_string(),
        bytes_written: 32768,
    };

    assert!(!response.success);
    assert_eq!(response.message, "Disk full");
    assert_eq!(response.bytes_written, 32768);
}

// ==================== ReadBlockRequest Tests ====================

#[test]
fn test_read_block_request() {
    let request = ReadBlockRequest {
        block_id: "block-to-read".to_string(),
    };

    assert_eq!(request.block_id, "block-to-read");
}

// ==================== ReadBlockResponse Tests ====================

#[test]
fn test_read_block_response() {
    let data = vec![10, 20, 30, 40, 50];
    let response = ReadBlockResponse {
        chunk_data: data.clone(),
    };

    assert_eq!(response.chunk_data, data);
}

#[test]
fn test_read_block_response_empty() {
    let response = ReadBlockResponse { chunk_data: vec![] };

    assert!(response.chunk_data.is_empty());
}

// ==================== AuditRequest Tests ====================

#[test]
fn test_audit_request() {
    let request = AuditRequest {
        timestamp: 1700000000000,
        user_id: "user123".to_string(),
        action: "CREATE".to_string(),
        resource: "/home/user/file.txt".to_string(),
        component: "MASTER".to_string(),
        status: "SUCCESS".to_string(),
        details: "File created successfully".to_string(),
    };

    assert_eq!(request.timestamp, 1700000000000);
    assert_eq!(request.user_id, "user123");
    assert_eq!(request.action, "CREATE");
    assert_eq!(request.status, "SUCCESS");
}

#[test]
fn test_audit_request_display() {
    let request = AuditRequest {
        timestamp: 1700000000000,
        user_id: "admin".to_string(),
        action: "DELETE".to_string(),
        resource: "/data/old.txt".to_string(),
        component: "MASTER".to_string(),
        status: "DENIED".to_string(),
        details: "Permission denied".to_string(),
    };

    let display = format!("{}", request);
    assert!(display.contains("admin"));
    assert!(display.contains("DELETE"));
    assert!(display.contains("DENIED"));
}

// ==================== AuditResponse Tests ====================

#[test]
fn test_audit_response_ack() {
    let response = AuditResponse { ack: true };
    assert!(response.ack);
}

#[test]
fn test_audit_response_nack() {
    let response = AuditResponse { ack: false };
    assert!(!response.ack);
}

// ==================== Edge Cases ====================

#[test]
fn test_large_file_size() {
    let response = GetFileResponse {
        file_id: "large-file".to_string(),
        size: i64::MAX,
        blocks: vec![],
    };

    assert_eq!(response.size, i64::MAX);
}

#[test]
fn test_unicode_filenames() {
    let info = FileInfo {
        filename: "文档.txt".to_string(),
        is_directory: false,
        size: 100,
    };

    assert_eq!(info.filename, "文档.txt");
}

#[test]
fn test_special_characters_in_path() {
    let request = CreateFileRequest {
        path: "/home/user/file with spaces & special!.txt".to_string(),
        size: 100,
        user_id: "user".to_string(),
    };

    assert!(request.path.contains("spaces"));
    assert!(request.path.contains("&"));
}
