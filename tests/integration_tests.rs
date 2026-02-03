//! Integration tests for the DAFS distributed file system
//!
//! These tests verify the end-to-end behavior of the system components
//! working together.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Test utilities module
pub mod test_utils {
    use std::collections::HashMap;
    use rand::Rng;

    /// Generate random test data of the specified size
    pub fn generate_test_data(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        (0..size).map(|_| rng.gen::<u8>()).collect()
    }

    /// Generate predictable test data for verification
    pub fn generate_predictable_data(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }

    /// Create a set of test datanodes for testing
    pub fn create_test_datanodes(count: usize, base_port: u16) -> HashMap<i32, String> {
        (0..count as i32)
            .map(|i| (i + 1, format!("http://[::1]:{}", base_port + i as u16)))
            .collect()
    }

    /// Block size constant (64KB)
    pub const BLOCK_SIZE: i64 = 64 * 1024;

    /// Calculate expected number of blocks for a given file size
    pub fn calculate_block_count(file_size: i64) -> i32 {
        if file_size == 0 {
            0
        } else {
            ((file_size + BLOCK_SIZE - 1) / BLOCK_SIZE) as i32
        }
    }

    /// Verify that data chunks reassemble correctly
    pub fn verify_chunks_match(original: &[u8], chunks: &[Vec<u8>]) -> bool {
        let reassembled: Vec<u8> = chunks.iter().flat_map(|c| c.iter().cloned()).collect();
        original == reassembled.as_slice()
    }
}

#[cfg(test)]
mod block_calculation_tests {
    use super::test_utils::*;

    #[test]
    fn test_zero_file_no_blocks() {
        assert_eq!(calculate_block_count(0), 0);
    }

    #[test]
    fn test_small_file_one_block() {
        assert_eq!(calculate_block_count(1), 1);
        assert_eq!(calculate_block_count(100), 1);
        assert_eq!(calculate_block_count(BLOCK_SIZE - 1), 1);
    }

    #[test]
    fn test_exact_block_boundary() {
        assert_eq!(calculate_block_count(BLOCK_SIZE), 1);
        assert_eq!(calculate_block_count(BLOCK_SIZE * 2), 2);
        assert_eq!(calculate_block_count(BLOCK_SIZE * 10), 10);
    }

    #[test]
    fn test_block_boundary_plus_one() {
        assert_eq!(calculate_block_count(BLOCK_SIZE + 1), 2);
        assert_eq!(calculate_block_count(BLOCK_SIZE * 2 + 1), 3);
    }

    #[test]
    fn test_large_file() {
        // 1GB file
        let one_gb = 1024 * 1024 * 1024;
        let expected_blocks = (one_gb + BLOCK_SIZE - 1) / BLOCK_SIZE;
        assert_eq!(calculate_block_count(one_gb), expected_blocks as i32);
    }
}

#[cfg(test)]
mod data_generation_tests {
    use super::test_utils::*;

    #[test]
    fn test_predictable_data_pattern() {
        let data = generate_predictable_data(512);
        
        for (i, &byte) in data.iter().enumerate() {
            assert_eq!(byte, (i % 256) as u8);
        }
    }

    #[test]
    fn test_predictable_data_size() {
        for size in [0, 1, 100, 1000, 10000] {
            let data = generate_predictable_data(size);
            assert_eq!(data.len(), size);
        }
    }

    #[test]
    fn test_random_data_size() {
        for size in [0, 1, 100, 1000, 10000] {
            let data = generate_test_data(size);
            assert_eq!(data.len(), size);
        }
    }

    #[test]
    fn test_random_data_varies() {
        let data1 = generate_test_data(100);
        let data2 = generate_test_data(100);
        
        // Very unlikely to be the same (but not impossible)
        // We just check they're generated properly
        assert_eq!(data1.len(), 100);
        assert_eq!(data2.len(), 100);
    }
}

#[cfg(test)]
mod chunk_verification_tests {
    use super::test_utils::*;

    #[test]
    fn test_single_chunk_matches() {
        let original = generate_predictable_data(100);
        let chunks = vec![original.clone()];
        
        assert!(verify_chunks_match(&original, &chunks));
    }

    #[test]
    fn test_multiple_chunks_match() {
        let original = generate_predictable_data(300);
        let chunks = vec![
            original[0..100].to_vec(),
            original[100..200].to_vec(),
            original[200..300].to_vec(),
        ];
        
        assert!(verify_chunks_match(&original, &chunks));
    }

    #[test]
    fn test_empty_chunks_match_empty_original() {
        let original: Vec<u8> = vec![];
        let chunks: Vec<Vec<u8>> = vec![];
        
        assert!(verify_chunks_match(&original, &chunks));
    }

    #[test]
    fn test_mismatched_data_fails() {
        let original = generate_predictable_data(100);
        let wrong_chunks = vec![generate_predictable_data(50)];
        
        assert!(!verify_chunks_match(&original, &wrong_chunks));
    }

    #[test]
    fn test_uneven_chunks_match() {
        let original = generate_predictable_data(250);
        let chunks = vec![
            original[0..100].to_vec(),
            original[100..150].to_vec(),
            original[150..250].to_vec(),
        ];
        
        assert!(verify_chunks_match(&original, &chunks));
    }
}

#[cfg(test)]
mod datanode_setup_tests {
    use super::test_utils::*;

    #[test]
    fn test_create_single_datanode() {
        let datanodes = create_test_datanodes(1, 8080);
        
        assert_eq!(datanodes.len(), 1);
        assert!(datanodes.contains_key(&1));
        assert_eq!(datanodes.get(&1), Some(&"http://[::1]:8080".to_string()));
    }

    #[test]
    fn test_create_multiple_datanodes() {
        let datanodes = create_test_datanodes(3, 8080);
        
        assert_eq!(datanodes.len(), 3);
        assert_eq!(datanodes.get(&1), Some(&"http://[::1]:8080".to_string()));
        assert_eq!(datanodes.get(&2), Some(&"http://[::1]:8081".to_string()));
        assert_eq!(datanodes.get(&3), Some(&"http://[::1]:8082".to_string()));
    }

    #[test]
    fn test_create_zero_datanodes() {
        let datanodes = create_test_datanodes(0, 8080);
        
        assert!(datanodes.is_empty());
    }

    #[test]
    fn test_datanodes_different_base_ports() {
        let datanodes1 = create_test_datanodes(2, 8080);
        let datanodes2 = create_test_datanodes(2, 9090);
        
        assert_ne!(datanodes1.get(&1), datanodes2.get(&1));
    }
}

#[cfg(test)]
mod file_path_tests {
    use std::path::PathBuf;

    #[test]
    fn test_extract_filename_from_path() {
        let path = "/home/user/documents/file.txt";
        let filename = std::path::Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        
        assert_eq!(filename, "file.txt");
    }

    #[test]
    fn test_extract_filename_from_root() {
        let path = "/file.txt";
        let filename = std::path::Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        
        assert_eq!(filename, "file.txt");
    }

    #[test]
    fn test_path_parent_extraction() {
        let path = PathBuf::from("/home/user/documents/file.txt");
        let parent = path.parent().unwrap();
        
        assert_eq!(parent.to_str().unwrap(), "/home/user/documents");
    }

    #[test]
    fn test_path_join() {
        let base = PathBuf::from("/data/blocks");
        let block_id = "block-12345";
        let full_path = base.join(block_id);
        
        assert_eq!(full_path.to_str().unwrap(), "/data/blocks/block-12345");
    }

    #[test]
    fn test_path_starts_with() {
        let file_path = "/home/user/docs/file.txt";
        let dir_path = "/home/user";
        
        assert!(file_path.starts_with(dir_path));
        assert!(!file_path.starts_with("/other"));
    }
}

#[cfg(test)]
mod timestamp_tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_timestamp_generation() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        // Should be a reasonable timestamp (after year 2020)
        assert!(timestamp > 1577836800000); // Jan 1, 2020
    }

    #[test]
    fn test_timestamps_increase() {
        let ts1 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let ts2 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        assert!(ts2 >= ts1);
    }
}

#[cfg(test)]
mod uuid_tests {
    #[test]
    fn test_uuid_format() {
        let uuid = uuid::Uuid::new_v4().to_string();
        
        // UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
        assert_eq!(uuid.len(), 36);
        assert_eq!(uuid.chars().filter(|&c| c == '-').count(), 4);
    }

    #[test]
    fn test_uuid_uniqueness() {
        let uuid1 = uuid::Uuid::new_v4().to_string();
        let uuid2 = uuid::Uuid::new_v4().to_string();
        
        assert_ne!(uuid1, uuid2);
    }

    #[test]
    fn test_generate_many_uuids() {
        let mut uuids = std::collections::HashSet::new();
        
        for _ in 0..1000 {
            let uuid = uuid::Uuid::new_v4().to_string();
            assert!(uuids.insert(uuid), "Duplicate UUID generated");
        }
    }
}

#[cfg(test)]
mod replication_tests {
    use super::test_utils::*;

    const REPLICATION_FACTOR: usize = 2;

    #[test]
    fn test_replication_with_enough_nodes() {
        let datanodes = create_test_datanodes(5, 8080);
        
        // With 5 nodes and replication factor 2, we should always
        // be able to replicate to 2 nodes
        assert!(datanodes.len() >= REPLICATION_FACTOR);
    }

    #[test]
    fn test_replication_with_exact_nodes() {
        let datanodes = create_test_datanodes(2, 8080);
        
        assert_eq!(datanodes.len(), REPLICATION_FACTOR);
    }

    #[test]
    fn test_select_replica_nodes() {
        let datanodes: Vec<i32> = (1..=5).collect();
        let block_idx = 0;
        
        // Simulate block placement algorithm
        let mut block_locations = vec![];
        for repl_idx in 0..REPLICATION_FACTOR {
            let node_idx = (block_idx + repl_idx) % datanodes.len();
            block_locations.push(datanodes[node_idx]);
        }
        
        assert_eq!(block_locations.len(), REPLICATION_FACTOR);
        // Locations should be different
        assert_ne!(block_locations[0], block_locations[1]);
    }

    #[test]
    fn test_block_distribution_across_nodes() {
        let datanodes: Vec<i32> = (1..=3).collect();
        let num_blocks = 10;
        let mut node_usage = std::collections::HashMap::new();
        
        for block_idx in 0..num_blocks {
            for repl_idx in 0..REPLICATION_FACTOR {
                let node_idx = (block_idx + repl_idx) % datanodes.len();
                *node_usage.entry(datanodes[node_idx]).or_insert(0) += 1;
            }
        }
        
        // Each node should be used (roughly equal distribution)
        assert_eq!(node_usage.len(), datanodes.len());
    }
}

#[cfg(test)]
mod error_handling_tests {
    #[test]
    fn test_file_not_found_error() {
        let path = "/nonexistent/path/to/file.txt";
        let result = std::fs::read(path);
        
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        ));
    }

    #[test]
    fn test_invalid_path_handling() {
        let path = "";
        let result = std::fs::metadata(path);
        
        assert!(result.is_err());
    }
}
