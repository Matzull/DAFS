use clap::Parser;
use common::dfs::data_node_service_client::DataNodeServiceClient;
use common::dfs::data_node_service_server::{DataNodeService, DataNodeServiceServer};
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::read_block_response::ResponseData;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    ConfigureNodeRequest, ConfigureNodeResponse, DeleteBlockRequest, DeleteBlockResponse,
    HeartbeatRequest, ReadBlockRequest, ReadBlockResponse, ReplicateBlocksRequest,
    ReplicateBlocksResponse, WriteBlockRequest, WriteBlockResponse,
};
use crc32fast::Hasher;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, create_dir_all, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Server, Uri};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, instrument, warn};
const DEFAULT_DATA_FOLDER: &str = "./testing/datanode";
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_LISTEN_ADDRESS: &str = "[::1]:8080";
const DEFAULT_METADATA_NODE_ADDRESS: &str = "http://[::1]:9090";

#[derive(Parser, Debug, Serialize, Deserialize, Default)]
pub struct InternalSettings {
    pub listen_address: String,
    pub data_folder: String,
    pub log_level: String,
    pub mpsc_channel_size: u8,
    pub read_buffer_size: usize,
    pub metadata_node_address: String,
}

pub fn load_config() -> InternalSettings {
    let cli = Cli::parse();

    let figment = Figment::new()
        .merge(Serialized::defaults(InternalSettings {
            listen_address: DEFAULT_LISTEN_ADDRESS.to_string(),
            data_folder: DEFAULT_DATA_FOLDER.to_string(),
            log_level: DEFAULT_LOG_LEVEL.to_string(),
            mpsc_channel_size: 4,
            read_buffer_size: 64 * 1024,
            metadata_node_address: DEFAULT_METADATA_NODE_ADDRESS.to_string(),
        }))
        .merge(Toml::file(&cli.config_path))
        .merge(Serialized::globals(cli));

    figment.extract().expect("Error loading configuration")
}

/// DAFS DataNode Service
#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(name = "dafs-datanode")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Server listen address
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    listen_address: Option<String>,

    /// Data folder location
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    data_folder: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,

    #[arg(short, long, default_value = "config.toml")]
    #[serde(skip)]
    pub config_path: String,
}

#[derive(Default)]
pub struct Datanode {
    pub node_id: Arc<Mutex<u16>>,
    pub settings: Arc<InternalSettings>,
}

impl Datanode {
    async fn replicate_single_block_to_dst(
        settings: Arc<InternalSettings>,
        dst_addr: String,
        block_id: String,
    ) -> Result<(), Status> {
        if block_id.contains('/') || block_id.contains('.') {
            return Err(Status::invalid_argument("Invalid block_id"));
        }

        let mut file_path = PathBuf::from(&settings.data_folder);
        file_path.push(&block_id[..2]);
        file_path.push(&block_id[2..4]);
        file_path.push(&block_id);

        let mut source_file = File::open(file_path).await.map_err(|err| {
            error!(%err, block_id = %block_id, "Failed to open source block for replication");
            Status::not_found(err.to_string())
        })?;

        let mut client = DataNodeServiceClient::connect(dst_addr.clone())
            .await
            .map_err(|err| {
                error!(%err, dst = %dst_addr, "Failed to connect to destination datanode");
                Status::unavailable(err.to_string())
            })?;

        let (tx, rx) = mpsc::channel(settings.mpsc_channel_size as usize);
        let write_future = client.write_block(Request::new(ReceiverStream::new(rx)));

        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::Metadata(common::dfs::BlockMetadata {
                block_id: block_id.clone(),
                total_size: 0,
            })),
        })
        .await
        .map_err(|err| {
            Status::internal(format!("Failed to send block metadata for {block_id}: {err}"))
        })?;

        let mut hasher = Hasher::new();
        let mut buffer = vec![0u8; settings.read_buffer_size];
        loop {
            let bytes_read = source_file.read(&mut buffer).await.map_err(|err| {
                error!(%err, block_id = %block_id, "Failed to read source block during replication");
                Status::internal(err.to_string())
            })?;

            if bytes_read == 0 {
                break;
            }

            hasher.update(&buffer[..bytes_read]);
            tx.send(WriteBlockRequest {
                request_data: Some(RequestData::ChunkData(buffer[..bytes_read].to_vec())),
            })
            .await
            .map_err(|err| {
                Status::internal(format!("Failed to send chunk data for {block_id}: {err}"))
            })?;
        }

        let checksum = hasher.finalize().to_string();
        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::Checksum(checksum)),
        })
        .await
        .map_err(|err| Status::internal(format!("Failed to send checksum for {block_id}: {err}")))?;

        drop(tx);
        let write_response = write_future.await.map_err(|err| {
            error!(%err, block_id = %block_id, dst = %dst_addr, "Failed to write replicated block");
            Status::internal(format!("Failed to write replicated block: {err}"))
        })?;
        let write_result = write_response.into_inner();
        if !write_result.success {
            return Err(Status::internal(write_result.message));
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl DataNodeService for Datanode {
    type ReadBlockStream = ReceiverStream<Result<ReadBlockResponse, Status>>;

    #[instrument(skip(self, request))]
    async fn write_block(
        &self,
        request: Request<Streaming<WriteBlockRequest>>,
    ) -> Result<Response<WriteBlockResponse>, Status> {
        debug!("Received write_block request");
        let mut hasher = Hasher::new();
        let mut stream = request.into_inner();
        // let mut file: Option<File> = None;
        let mut writer: Option<BufWriter<File>> = None;
        let mut bytes_written: i64 = 0;
        let mut block_id_str = String::new();

        while let Some(msg) = stream.message().await? {
            match msg.request_data {
                Some(RequestData::Metadata(metadata)) => {
                    block_id_str = metadata.block_id.clone();
                    if block_id_str.contains('/') || block_id_str.contains('.') {
                        return Err(Status::invalid_argument("Invalid block_id"));
                    }
                    debug!(block_id = %block_id_str, "Received metadata for block");
                    let mut file_path_pathbuf = PathBuf::from(&self.settings.data_folder);
                    file_path_pathbuf.push(&block_id_str[..2]);
                    file_path_pathbuf.push(&block_id_str[2..4]);
                    create_dir_all(&file_path_pathbuf).await.map_err(|err| {
                        error!(%err, "Failed to create folder");
                        Status::internal(err.to_string())
                    })?;
                    file_path_pathbuf.push(&block_id_str);
                    let f = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(file_path_pathbuf)
                        .await
                        .map_err(|err| {
                            error!(%err, "Failed to create file");
                            Status::internal(err.to_string())
                        })?;

                    writer = Some(BufWriter::with_capacity(128 * 1024, f));
                    // file = Some(f);
                }
                Some(RequestData::ChunkData(chunkdata)) => {
                    if let Some(ref mut w) = writer {
                        w.write_all(&chunkdata).await.map_err(|err| {
                            error!(%err, "Failed to write chunk data");
                            Status::internal(err.to_string())
                        })?;
                        hasher.update(&chunkdata);
                        bytes_written += chunkdata.len() as i64;
                    } else {
                        error!("Received chunk data before metadata");
                        return Err(Status::failed_precondition("Metadata missing"));
                    }
                }
                Some(RequestData::Checksum(expected_checksum)) => {
                    let calculated_checksum = hasher.finalize().to_string();
                    if calculated_checksum != expected_checksum {
                        error!(
                            calculated = calculated_checksum,
                            expected = expected_checksum,
                            "Checksum mismatch"
                        );
                        return Err(Status::invalid_argument("Checksum mismatch"));
                    }
                    debug!(block_id = %block_id_str, "Checksum verified successfully");
                    break;
                }
                None => {
                    warn!("Received empty message in stream");
                    return Err(Status::invalid_argument("The message is empty"));
                }
            }
        }
        if let Some(mut w) = writer {
            w.flush().await.map_err(|err| {
                error!(%err, "Failed to flush buffer");
                Status::internal(err.to_string())
            })?;
            w.get_mut().sync_all().await.map_err(|err| {
                error!(%err, "Failed to sync file");
                Status::internal(err.to_string())
            })?;
        }
        debug!(block_id = %block_id_str, bytes_written, "Successfully wrote block");
        Ok(Response::new(WriteBlockResponse {
            success: true,
            message: "Everything went well".to_string(),
            bytes_written,
        }))
    }

    #[instrument(skip(self, request))]
    async fn read_block(
        &self,
        request: Request<ReadBlockRequest>,
    ) -> Result<Response<Self::ReadBlockStream>, Status> {
        let block_id = request.into_inner().block_id;
        let mut hasher = Hasher::new();
        debug!(block_id = %block_id, "Received read_block request");
        if block_id.contains('/') || block_id.contains('.') {
            return Err(Status::invalid_argument("Invalid block_id"));
        }
        let mut file_path_pathbuf = PathBuf::from(&self.settings.data_folder);
        file_path_pathbuf.push(&block_id[..2]);
        file_path_pathbuf.push(&block_id[2..4]);
        file_path_pathbuf.push(&block_id);
        let (tx, rx) = mpsc::channel(self.settings.mpsc_channel_size as usize);

        tokio::spawn(async move {
            let file_res = File::open(file_path_pathbuf).await;
            let mut file = match file_res {
                Ok(f) => f,
                Err(e) => {
                    error!(%e, block_id = %block_id, "Failed to open file for reading");
                    let _ = tx.send(Err(Status::not_found(e.to_string()))).await;
                    return;
                }
            };

            let mut buffer = [0u8; 64 * 1024];
            loop {
                match file.read(&mut buffer).await {
                    Ok(0) => {
                        debug!(block_id = %block_id, "Finished reading file");
                        let block_checksum = hasher.finalize().to_string();
                        let response = ReadBlockResponse {
                            response_data: Some(ResponseData::Checksum(block_checksum.clone())),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            warn!(block_id = %block_id, "Receiver dropped, stopping read");
                            return;
                        }
                        debug!(block_id = %block_id, checksum = %block_checksum, "Calculated checksum");
                        break;
                    }
                    Ok(n) => {
                        hasher.update(&buffer[..n]);
                        let response = ReadBlockResponse {
                            response_data: Some(ResponseData::ChunkData(buffer[..n].to_vec())),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            warn!(block_id = %block_id, "Receiver dropped, stopping read");
                            return;
                        }
                        continue;
                    }
                    Err(err) => {
                        error!(%err, block_id = %block_id, "Error reading file");
                        let _ = tx.send(Err(Status::internal(err.to_string()))).await;
                        break;
                    }
                }
            }
        });
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(output_stream))
    }

    #[instrument(skip(self, request))]
    async fn delete_block(
        &self,
        request: Request<DeleteBlockRequest>,
    ) -> Result<Response<DeleteBlockResponse>, Status> {
        let block_id = request.into_inner().block_id;
        debug!(block_id = %block_id, "Received delete_block request");

        let mut file_path_pathbuf = PathBuf::from(&self.settings.data_folder);
        file_path_pathbuf.push(&block_id[..2]);
        file_path_pathbuf.push(&block_id[2..4]);
        let _ = fs::remove_file(&file_path_pathbuf)
            .await
            .map_err(|err| {
                warn!(block_id = %block_id, path = ?file_path_pathbuf, "Could not delete block file: {}", err);
                Status::not_found(format!("Could not delete file {err}"))
            });
        debug!(block_id = %block_id, "Block deleted successfully");
        Ok(Response::new(DeleteBlockResponse {
            delete_result: Some(common::dfs::delete_block_response::DeleteResult::Success(
                true,
            )),
        }))
    }

    #[instrument(skip(self, request))]
    async fn configure_node(
        &self,
        request: Request<ConfigureNodeRequest>,
    ) -> Result<Response<ConfigureNodeResponse>, Status> {
        let req = request.into_inner();
        let new_node_id = req.node_id;
        info!(node_id = new_node_id, "Received configure_node request");
        *self.node_id.lock().await = new_node_id as u16;
        info!(node_id = new_node_id, "Node configured successfully");
        Ok(Response::new(ConfigureNodeResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }

    async fn replicate_blocks(
        &self,
        request: Request<ReplicateBlocksRequest>,
    ) -> Result<Response<ReplicateBlocksResponse>, Status> {
        let block_locs = request.into_inner().block_locations;
        let max_parallel_blocks = 16;
        let sem = Arc::new(Semaphore::new(max_parallel_blocks));
        let mut set = JoinSet::new();
        for (node_to_replicate_to, blocks) in block_locs {
            for block_id in blocks.block_ids {
                let permit = sem.clone().acquire_owned().await.unwrap();
                let dst_clone = node_to_replicate_to.clone();
                let settings = Arc::clone(&self.settings);
                set.spawn(async move {
                    let _permit = permit;
                    Datanode::replicate_single_block_to_dst(settings, dst_clone, block_id).await
                });
            }
        }

        while let Some(join_result) = set.join_next().await {
            match join_result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => {
                    error!(%err, "Replication task panicked or was cancelled");
                    return Err(Status::internal(err.to_string()));
                }
            }
        }

        Ok(Response::new(ReplicateBlocksResponse {
            success: true,
            error_message: "".to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Arc::new(load_config());

    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| settings.log_level.clone());
    let filter = tracing_subscriber::EnvFilter::new("info")
        .add_directive(format!("datanode={log_level}").parse().unwrap());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();
    info!("Starting DataNode with settings: {:?}", settings);
    debug!("Creating directory to store files");
    create_dir_all(&settings.data_folder).await?;
    let address: std::net::SocketAddr = settings.listen_address.parse()?;
    let datanode = Datanode {
        node_id: Arc::new(Mutex::new(0)),
        settings: Arc::clone(&settings),
    };
    let node_id_arc = Arc::clone(&datanode.node_id);
    let service = DataNodeServiceServer::new(datanode);

    info!("Starting DataNode server on {}", address);

    tokio::spawn(async move {
        info!("HeartBeat sender starting - will register with master");

        // Retry connection with exponential backoff
        let mut retry_delay = tokio::time::Duration::from_millis(500);
        let max_retry_delay = tokio::time::Duration::from_secs(30);

        let mut client = loop {
            match MetadataServiceClient::connect(
                settings.metadata_node_address.parse::<Uri>().unwrap(),
            )
            .await
            {
                Ok(c) => {
                    info!("Connected to master server");
                    break c;
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to master: {}. Retrying in {:?}...",
                        e, retry_delay
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                }
            }
        };

        loop {
            let current_node_id = *node_id_arc.lock().await as u32;

            info!("Sending heartbeat");
            let result = client
                .heartbeat(HeartbeatRequest {
                    cpu_usage: 1.0,
                    node_id: current_node_id,
                    free_space: 4,
                    used_space: 4,
                    addr: address.to_string(),
                })
                .await;

            match result {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.acknowledgement {
                        // If we sent node_id=0 (not registered), update with assigned ID
                        if current_node_id == 0 && resp.assigned_node_id > 0 {
                            let new_id = resp.assigned_node_id as u16;
                            *node_id_arc.lock().await = new_id;
                            info!(node_id = new_id, "Registered with master, assigned node_id");
                        } else {
                            debug!(node_id = current_node_id, "Heartbeat acknowledged");
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
                    } else {
                        error!("Heartbeat not acknowledged by master");
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    }
                }
                Err(e) => {
                    error!("Heartbeat failed: {}. Attempting to reconnect...", e);
                    // Try to reconnect
                    retry_delay = tokio::time::Duration::from_millis(500);
                    loop {
                        match MetadataServiceClient::connect(
                            settings.metadata_node_address.parse::<Uri>().unwrap(),
                        )
                        .await
                        {
                            Ok(c) => {
                                info!("Reconnected to master server");
                                client = c;
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to reconnect to master: {}. Retrying in {:?}...",
                                    e, retry_delay
                                );
                                tokio::time::sleep(retry_delay).await;
                                retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                            }
                        }
                    }
                }
            }
        }
    });

    Server::builder()
        .add_service(service)
        .serve(address)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
