use super::*;

fn test_datanode() -> Datanode {
    Datanode {
        node_id: Arc::new(Mutex::new(0)),
        settings: Arc::new(InternalSettings {
            listen_address: "[::1]:18080".to_string(),
            data_folder: "./testing/datanode-tests".to_string(),
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
