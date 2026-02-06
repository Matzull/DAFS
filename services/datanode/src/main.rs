use clap::Parser;
use common::dfs::data_node_service_server::{DataNodeService, DataNodeServiceServer};
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    ConfigureNodeRequest, ConfigureNodeResponse, DeleteBlockRequest, DeleteBlockResponse,
    HeartbeatRequest, ReadBlockRequest, ReadBlockResponse, WriteBlockRequest, WriteBlockResponse,
};
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, create_dir_all, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, instrument, warn};

const DATA_FOLDER: &str = "/home/matzul/Documents/DAFS/testing/datanode";

#[derive(Parser, Debug, Serialize, Deserialize, Default)]
pub struct InternalSettings {
    pub listen_address: String,
    pub data_folder: String,
    pub log_level: String,
    pub mpsc_channel_size: u8,
    pub read_buffer_size: usize,
}

pub fn load_config() -> InternalSettings {
    let cli = Cli::parse();

    let figment = Figment::new()
        .merge(Serialized::defaults(InternalSettings {
            listen_address: "[::1]:8080".to_string(),
            data_folder: DATA_FOLDER.to_string(),
            log_level: "info".to_string(),
            mpsc_channel_size: 4,
            read_buffer_size: 64 * 1024,
        }))
        .merge(Toml::file(&cli.config_path));

    figment.extract().expect("Error loading configuration")
}

/// DAFS DataNode Service
#[derive(Parser, Debug)]
#[command(name = "dafs-datanode")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Server listen address
    #[arg(short, long, default_value = "[::1]:8080")]
    listen_address: Option<String>,

    /// Data folder location
    #[arg(
        short,
        long,
        default_value = "/home/matzul/Documents/DAFS/testing/datanode"
    )]
    data_folder: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: Option<String>,

    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,
}

#[derive(Default)]
pub struct Datanode {
    pub node_id: Arc<Mutex<u16>>,
    pub settings: Arc<InternalSettings>,
}

#[tonic::async_trait]
impl DataNodeService for Datanode {
    type ReadBlockStream = ReceiverStream<Result<ReadBlockResponse, Status>>;

    #[instrument(skip(self, request))]
    async fn write_block(
        &self,
        request: Request<Streaming<WriteBlockRequest>>,
    ) -> Result<Response<WriteBlockResponse>, Status> {
        info!("Received write_block request");
        let mut stream = request.into_inner();
        let mut file: Option<File> = None;
        let mut bytes_written: i64 = 0;
        let mut block_id_str = String::new();

        while let Some(msg) = stream.message().await? {
            match msg.request_data {
                Some(RequestData::Metadata(metadata)) => {
                    block_id_str = metadata.block_id.clone();
                    debug!(block_id = %block_id_str, "Received metadata for block");
                    let mut file_path_pathbuf = PathBuf::from(&self.settings.data_folder);
                    file_path_pathbuf.push(&block_id_str[..2]);
                    file_path_pathbuf.push(&block_id_str[2..4]);
                    create_dir_all(&file_path_pathbuf).await.map_err(|err| {
                        error!(%err, "Failed to create folder");
                        Status::internal(err.to_string())
                    })?;
                    file_path_pathbuf.push(&block_id_str);
                    file = Some(File::create_new(file_path_pathbuf).await.map_err(|err| {
                        error!(%err, "Failed to create file");
                        Status::internal(err.to_string())
                    })?);
                }
                Some(RequestData::ChunkData(chunkdata)) => {
                    if let Some(ref mut f) = file {
                        f.write_all(&chunkdata).await.map_err(|err| {
                            error!(%err, "Failed to write chunk data");
                            Status::internal(err.to_string())
                        })?;
                        bytes_written += chunkdata.len() as i64;
                    } else {
                        error!("Received chunk data before metadata");
                        return Err(Status::failed_precondition("Metadata missing"));
                    }
                }
                None => {
                    warn!("Received empty message in stream");
                    return Err(Status::invalid_argument("The message is empty"));
                }
            }
        }
        if let Some(f) = file {
            f.sync_all().await.map_err(|err| {
                error!(%err, "Failed to sync file");
                Status::internal(err.to_string())
            })?;
        }
        info!(block_id = %block_id_str, bytes_written, "Successfully wrote block");
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
        info!(block_id = %block_id, "Received read_block request");

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
                        break;
                    }
                    Ok(n) => {
                        let response = ReadBlockResponse {
                            chunk_data: buffer[..n].to_vec(),
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
        info!(block_id = %block_id, "Received delete_block request");

        let mut file_path_pathbuf = PathBuf::from(&self.settings.data_folder);
        file_path_pathbuf.push(&block_id[..2]);
        file_path_pathbuf.push(&block_id[2..4]);
        let _ = fs::remove_file(&file_path_pathbuf)
            .await
            .map_err(|err| {
                warn!(block_id = %block_id, path = ?file_path_pathbuf, "Could not delete block file: {}", err);
                Status::not_found(format!("Could not delete file {}", err))
            });
        info!(block_id = %block_id, "Block deleted successfully");
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Arc::new(load_config());

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .ok()
                .and_then(|v| tracing_subscriber::EnvFilter::try_new(v).ok())
                .unwrap_or_else(|| tracing_subscriber::EnvFilter::new(settings.log_level.clone())),
        )
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();
    debug!("Creating directory to store files");
    create_dir_all(&settings.data_folder).await?;
    let address = settings.listen_address.parse()?;
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
            match MetadataServiceClient::connect("http://[::1]:9090").await {
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
                            info!(
                                node_id = new_id,
                                address = resp.node_address,
                                "Registered with master, assigned node_id"
                            );
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
                        match MetadataServiceClient::connect("http://[::1]:9090").await {
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
