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
            metadata_node_address: "http://[::1]:19090".to_string(),
        }),
    }
}

#[tokio::test]
async fn read_block_rejects_invalid_block_id() {
    let datanode = test_datanode();

    let result = datanode
        .read_block(Request::new(ReadBlockRequest {
            block_id: "../escape".to_string(),
        }))
        .await;

    let status = result.expect_err("invalid block id should fail");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn configure_node_updates_node_id() {
    let datanode = test_datanode();

    let response = datanode
        .configure_node(Request::new(ConfigureNodeRequest {
            node_id: 42,
            config: "".to_string(),
            listen_address: "[::1]:18080".to_string(),
        }))
        .await
        .expect("configure_node should succeed")
        .into_inner();

    assert!(response.success);
    assert_eq!(*datanode.node_id.lock().await, 42);
}
