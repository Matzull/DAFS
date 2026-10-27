use super::*;

fn test_datanode() -> Datanode {
    test_datanode_in("./testing/datanode-tests")
}

fn test_datanode_in(data_folder: &str) -> Datanode {
    Datanode {
        node_id: Arc::new(Mutex::new(0)),
        settings: Arc::new(InternalSettings {
            listen_address: "[::1]:18080".to_string(),
            advertise_address: String::new(),
            data_folder: data_folder.to_string(),
            log_level: "info".to_string(),
            mpsc_channel_size: 4,
            read_buffer_size: 64 * 1024,
            write_buffer_size: 128 * 1024,
            write_sync_policy: DEFAULT_WRITE_SYNC_POLICY.to_string(),
            write_sync_interval_ms: 0,
            metadata_node_address: "http://[::1]:19090".to_string(),
            metadata_node_endpoints: vec!["http://[::1]:19090".to_string()],
            replication_max_parallel_blocks: 4,
            heartbeat_retry_initial_ms: 500,
            heartbeat_retry_max_ms: 30_000,
            heartbeat_interval_ms: 3_000,
            heartbeat_nack_retry_ms: 1_000,
            heartbeat_cpu_usage: 1.0,
            heartbeat_free_space: 4,
            heartbeat_used_space: 4,
            jwt_secret: "test-secret".to_string(),
            service_token_exp_seconds: 300,
        }),
        block_last_sync: Arc::new(Mutex::new(std::collections::HashMap::new())),
    }
}

fn user_token() -> String {
    common::create_access_token("test-secret", "tester", common::ROLE_USER, 3600)
        .expect("token generation should succeed")
}

fn master_token() -> String {
    common::create_access_token("test-secret", "master", common::ROLE_MASTER, 3600)
        .expect("token generation should succeed")
}

#[tokio::test]
async fn read_block_rejects_invalid_block_id() {
    let datanode = test_datanode();
    let token = user_token();
    let mut request = Request::new(ReadBlockRequest {
        block_id: "../escape".to_string(),
    });
    common::attach_bearer_token(&mut request, &token).expect("attach token");

    let result = datanode.read_block(request).await;

    let status = result.expect_err("invalid block id should fail");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn configure_node_updates_node_id() {
    let datanode = test_datanode();
    let token = master_token();
    let mut request = Request::new(ConfigureNodeRequest {
        node_id: 42,
        config: "".to_string(),
        listen_address: "[::1]:18080".to_string(),
    });
    common::attach_bearer_token(&mut request, &token).expect("attach token");

    let response = datanode
        .configure_node(request)
        .await
        .expect("configure_node should succeed")
        .into_inner();

    assert!(response.success);
    assert_eq!(*datanode.node_id.lock().await, 42);
}

#[test]
fn block_path_shards_by_id_prefix() {
    let path = block_path("/data", "abcd-1234").expect("valid id");
    assert_eq!(path, PathBuf::from("/data/ab/cd/abcd-1234"));
}

#[test]
fn block_path_rejects_short_and_traversal_ids() {
    for bad in ["", "ab", "../../etc/passwd", "abcd/efgh", "ab.cd", "abñd"] {
        let status = block_path("/data", bad).expect_err(bad);
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{bad}");
    }
}

#[tokio::test]
async fn read_block_streams_stored_data_and_checksum() {
    use tokio_stream::StreamExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let data_folder = dir.path().to_str().expect("utf-8 path");
    let block_id = "0123456789abcdef";
    let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let path = block_path(data_folder, block_id).expect("valid id");
    std::fs::create_dir_all(path.parent().unwrap()).expect("create dirs");
    std::fs::write(&path, &data).expect("write block");

    let datanode = test_datanode_in(data_folder);
    let mut request = Request::new(ReadBlockRequest {
        block_id: block_id.to_string(),
    });
    common::attach_bearer_token(&mut request, &user_token()).expect("attach token");
    let mut stream = datanode
        .read_block(request)
        .await
        .expect("read")
        .into_inner();

    let mut received = Vec::new();
    let mut checksum = None;
    while let Some(message) = stream.next().await {
        match message.expect("chunk").response_data {
            Some(ResponseData::ChunkData(chunk)) => received.extend_from_slice(&chunk),
            Some(ResponseData::Checksum(sum)) => checksum = Some(sum),
            None => {}
        }
    }

    assert_eq!(received, data);
    assert_eq!(checksum, Some(crc32fast::hash(&data).to_string()));
}

#[tokio::test]
async fn read_block_requires_token() {
    let datanode = test_datanode();
    let request = Request::new(ReadBlockRequest {
        block_id: "0123456789abcdef".to_string(),
    });

    let status = datanode.read_block(request).await.expect_err("no token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}
