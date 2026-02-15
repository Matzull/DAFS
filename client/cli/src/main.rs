use bytes::{Bytes, BytesMut};
use clap::{Parser, Subcommand};
use common::dfs::data_node_service_client::DataNodeServiceClient;
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::read_block_response::ResponseData;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    CreateFileRequest, CreateFileResponse, GetFileRequest, ListDirectoryRequest, ReadBlockRequest,
    RenameFileRequest, SetFileStatusRequest, WriteBlockRequest, WriteBlockResponse,
};
use crc32fast::Hasher;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{de, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::OpenOptions;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument};

fn format_file_size(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1024 * KB;
    const GB: i64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn format_timestamp(millis: i64) -> String {
    let secs = millis / 1000;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct InternalSettings {
    pub mpsc_upload_channel_size: u8,
    pub mpsc_download_channel_size: u8,
    pub master_endpoints: Vec<String>,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
    pub log_level: String,
}

pub fn load_config() -> InternalSettings {
    let cli = Cli::parse();
    info!("Loading CLI configuration from '{}'", cli.config_path);

    let has_master_endpoints = !cli.master_endpoints.is_empty();
    let master_endpoints = cli.master_endpoints.clone();

    let mut figment = Figment::new()
        .merge(Serialized::defaults(InternalSettings {
            mpsc_upload_channel_size: 5,
            mpsc_download_channel_size: 5,
            master_endpoints: vec!["http://[::1]:9090".to_string()],
            upload_concurrency: 2,
            download_concurrency: 10,
            log_level: "info".to_string(),
        }))
        .merge(Toml::file(&cli.config_path))
        .merge(Serialized::globals(cli));

    if has_master_endpoints {
        figment = figment.merge(("master_endpoints", master_endpoints));
    }

    figment.extract().expect("Error loading configuration")
}

/// DAFS CLI - Distributed Asynchronous File System client
#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(name = "dafs-cli")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Master node endpoint
    #[arg(short, long, global = true)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    master_endpoints: Vec<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,

    #[command(subcommand)]
    #[serde(skip)]
    command: Option<Commands>,

    #[arg(short, long, default_value = "config.toml", global = true)]
    #[serde(skip)]
    pub config_path: String,
}

#[derive(Subcommand, Debug, Serialize, Deserialize)]
enum Commands {
    /// Upload a file to the distributed file system
    Upload {
        /// Path to the local file to upload
        #[arg(short, long)]
        file: String,

        /// Remote path in the DFS (defaults to the filename)
        #[arg(short, long)]
        remote_path: Option<String>,
    },
    /// Download a file from the distributed file system
    Download {
        /// Remote path in the DFS
        #[arg(short, long)]
        remote_path: String,

        /// Local output file path
        #[arg(short, long)]
        output: String,
    },
    /// List directory contents
    List {
        /// Directory path to list
        #[arg(short, long, default_value = "/")]
        path: String,
    },
    /// Rename a file or move it from old_path to new_path
    Rename {
        #[arg(short, long, default_value = "/")]
        old_path: String,

        #[arg(short, long, default_value = "/")]
        new_path: String,
    },
}

#[instrument(skip(file_handle, data_node_client, settings))]
async fn upload_block(
    block_id: String,
    // data_node_endpoint: String,
    data_node_client: DataNodeServiceClient<tonic::transport::Channel>,
    offset: u64,
    block_size: i64,
    file_handle: Arc<std::fs::File>,
    settings: Arc<InternalSettings>,
) -> Result<WriteBlockResponse, String> {
    debug!(block_id, "Starting block upload");
    let mut hasher = Hasher::new();
    let (tx, rx) = mpsc::channel(settings.mpsc_upload_channel_size as usize);
    let mut client = data_node_client;
    debug!(block_id, "Connected to datanode for upload");
    let response_fut = client.write_block(ReceiverStream::new(rx));
    tx.send(WriteBlockRequest {
        request_data: Some(RequestData::Metadata(common::dfs::BlockMetadata {
            block_id: block_id.clone(),
            total_size: block_size,
        })),
    })
    .await
    .map_err(|e| format!("Failed to send block metadata: {e}"))?;
    let mut bytes_remaining = block_size as usize;
    let mut internal_block_offset = offset;
    while bytes_remaining > 0 {
        let bytes_to_read = std::cmp::min(bytes_remaining, 64 * 1024_usize); //TODO set in config
        let mut buffer = BytesMut::with_capacity(bytes_to_read);
        buffer.resize(bytes_to_read, 0);
        let file = Arc::clone(&file_handle);
        let (bytes_read, buffer) = tokio::task::spawn_blocking(move || {
            file.read_at(&mut buffer[..], internal_block_offset)
                .map(|n| (n, buffer))
        })
        .await
        .map_err(|e| format!("Block read task failed: {e}"))?
        .map_err(|e| format!("Failed to read file at offset {internal_block_offset}: {e}"))?;
        if bytes_read == 0 {
            break;
        }
        let mut final_data = buffer.freeze();
        if bytes_read < bytes_to_read {
            final_data = final_data.slice(0..bytes_read);
        }
        hasher.update(&final_data);
        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::ChunkData(final_data.to_vec())),
        })
        .await
        .map_err(|e| format!("Failed to send chunk data: {e}"))?;

        bytes_remaining -= bytes_read;
        internal_block_offset += bytes_read as u64;
    }
    let checksum = hasher.finalize();
    tx.send(WriteBlockRequest {
        request_data: Some(RequestData::Checksum(checksum.to_string())),
    })
    .await
    .map_err(|e| format!("Failed to send checksum: {e}"))?;
    drop(tx);
    let response = response_fut.await.map_err(|e| {
        error!("Failed to get write block response: {}", e);
        format!("Failed to get write block response: {e}")
    })?;
    let inner = response.into_inner();
    debug!(
        block_id = &block_id,
        bytes_written = inner.bytes_written,
        "Block upload completed successfully"
    );
    Ok(inner)
}

#[instrument(skip(settings))]
async fn upload_file(
    settings: Arc<InternalSettings>,
    file_path: String,
) -> Result<CreateFileResponse, Box<dyn Error>> {
    info!(file_path, "Starting file upload");
    let mut meta_client =
        MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;

    let file_handle = std::fs::File::open(&file_path)?;
    let create_file_response = meta_client
        .create_file(CreateFileRequest {
            path: file_path.clone(),
            size: file_handle.metadata()?.size() as i64, //TODO: file can be larger than 4GB
            user_id: "1".to_string(),
        })
        .await?
        .into_inner();

    let shared_file_handle = Arc::new(file_handle);
    let semaphore = Arc::new(Semaphore::new(settings.upload_concurrency));
    let mut set = JoinSet::new();
    let mut block_size = -1;

    let nodes_to_connect: HashSet<_> = create_file_response
        .blocks
        .iter()
        .map(|b| b.node_address.clone())
        .collect();
    let mut clients = HashMap::new();
    for addr in nodes_to_connect {
        let client = DataNodeServiceClient::connect(addr.clone()).await?;
        clients.insert(addr, client);
    }
    for block_assignment in create_file_response.blocks.clone() {
        if block_size == -1 {
            block_size = block_assignment.block_size;
        }
        let block_offset = block_assignment.order * block_size;
        let block_id = block_assignment.block_id.clone();
        let f_handle = Arc::clone(&shared_file_handle);
        let settings_clone = Arc::clone(&settings);
        let sem = Arc::clone(&semaphore); // Clonamos el Arc fuera
        let client = clients.get(&block_assignment.node_address).unwrap().clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.map_err(|e| e.to_string())?;

            let result = upload_block(
                block_id,
                client,
                block_offset as u64,
                block_assignment.block_size,
                f_handle,
                settings_clone,
            )
            .await;

            result
        });
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| {
            error!(file_path, "Block upload task panicked: {}", e);
            format!("Block upload task panicked: {e}")
        })??;
    }
    info!(
        file_path,
        blocks = create_file_response.blocks.len(),
        "File upload completed successfully"
    );
    meta_client
        .set_file_status(SetFileStatusRequest {
            path: file_path,
            status: "READY".to_string(),
        })
        .await?;
    Ok(create_file_response)
}

#[instrument(skip(file_handle, data_node_client, settings))]
async fn download_block(
    block_id: String,
    data_node_client: DataNodeServiceClient<tonic::transport::Channel>,
    offset: u64,
    file_handle: Arc<std::fs::File>,
    settings: Arc<InternalSettings>,
) -> Result<bool, String> {
    debug!(block_id, "Starting block download");
    let hasher = Hasher::new();
    let mut client = data_node_client;

    let mut current_offset = offset;
    let mut stream = client
        .read_block(ReadBlockRequest {
            block_id: block_id.clone(),
        })
        .await
        .map_err(|e| format!("Couldnt read block {e}"))?
        .into_inner();
    let mut writing_tasks = JoinSet::new();
    while let Some(msg) = stream.next().await {
        let data = msg.map_err(|e| format!("Failed to receive block chunk: {e}"))?;
        if let Some(ResponseData::Checksum(checksum)) = data.response_data {
            let calculated_checksum = hasher.finalize();
            if checksum != calculated_checksum.to_string() {
                error!(
                    block_id,
                    "Checksum mismatch: expected {}, got {}", checksum, calculated_checksum
                );
                return Err(format!(
                    "Checksum mismatch for block {block_id}: expected {checksum}, got {calculated_checksum}"));
            }
            debug!(
                block_id,
                "Block download completed successfully with valid checksum"
            );
            break;
        }
        let chunk_data = match data.response_data {
            Some(ResponseData::ChunkData(data)) => data,
            _ => {
                error!(block_id, "Received unexpected response data type");
                return Err(format!(
                    "Received unexpected response data type for block {block_id}"));
            }
        };
        let data_len = chunk_data.len() as u64;
        let write_offset = current_offset;
        let file = Arc::clone(&file_handle);
        writing_tasks.spawn_blocking(move || file.write_at(&chunk_data, write_offset));
        current_offset += data_len;

        if writing_tasks.len() > settings.download_concurrency {
            writing_tasks.join_next().await;
        }
    }

    while let Some(res) = writing_tasks.join_next().await {
        res.map_err(|err| {
            error!(block_id, "Block write task failed: {}", err);
            format!("Block write task failed: {err}")
        })?
        .map_err(|err| {
            error!(block_id, "Failed to write block data to file: {}", err);
            format!("Failed to write block data to file: {err}")
        })?;
    }
    debug!(block_id, "Block downloaded successfully");
    Ok(true)
}

#[instrument(skip(settings))]
async fn download_file(
    settings: Arc<InternalSettings>,
    file_path: String,
    out_file_path: String,
) -> Result<bool, Box<dyn Error>> {
    info!(file_path, "Starting file download");
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;

    let get_file_response = client
        .get_file_metadata(GetFileRequest {
            path: file_path.clone(),
            user_id: "1".to_string(),
        })
        .await?
        .into_inner();
    let file_handle = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&out_file_path)?;
    file_handle.set_len(get_file_response.size as u64)?;
    let shared_file_handle = Arc::new(file_handle);
    let semaphore = Arc::new(Semaphore::new(settings.download_concurrency));
    let mut set = JoinSet::new();

    let mut block_size = -1;
    let nodes_to_connect: HashSet<_> = get_file_response
        .blocks
        .iter()
        .map(|b| b.node_address.clone())
        .collect();
    let mut clients = HashMap::new();
    for addr in nodes_to_connect {
        let client = DataNodeServiceClient::connect(addr.clone()).await?;
        clients.insert(addr, client);
    }
    for block_assignment in get_file_response.blocks.clone() {
        if block_size == -1 {
            block_size = block_assignment.block_size;
        }
        let permit = semaphore.clone().acquire_owned().await?;
        let block_offset = block_assignment.order * block_size;
        let block_id = block_assignment.block_id.clone();
        let f_handle = Arc::clone(&shared_file_handle);
        let settings_clone = Arc::clone(&settings);
        let client = clients.get(&block_assignment.node_address).unwrap().clone();
        set.spawn(async move {
            let result = download_block(
                block_id,
                client,
                block_offset as u64,
                f_handle,
                settings_clone,
            )
            .await;
            let _ptr = permit;
            result
        });
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| {
            error!(file_path, "Block download task panicked: {}", e);
            format!("Block download task panicked: {e}")
        })??;
    }
    info!(
        file_path,
        out_file_path, "File download completed successfully"
    );
    Ok(true)
}

#[instrument(skip(settings))]
async fn list_directory(
    settings: Arc<InternalSettings>,
    directory: String,
) -> Result<bool, Box<dyn Error>> {
    info!(directory, "Starting listing directory");
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;

    let get_file_response = client
        .list_directory(ListDirectoryRequest {
            path: directory.clone(),
        })
        .await?
        .into_inner();
    let file_count = get_file_response.files.len();
    println!(
        "{:<30} {:<12} {:<15} {:<15} {:<20} {:<20} {:<12}",
        "Filename", "Type", "Size", "Owner", "Created", "Modified", "Status"
    );
    println!("{}", "-".repeat(124));
    for file in get_file_response.files {
        let file_type = if file.is_directory {
            "Directory"
        } else {
            "File"
        };
        let size_str = if file.is_directory {
            "-".to_string()
        } else {
            format_file_size(file.size)
        };
        let created_time = format_timestamp(file.created_at);
        let modified_time = format_timestamp(file.last_modified_at);
        println!(
            "{:<30} {:<12} {:<15} {:<15} {:<20} {:<20} {:<12}",
            file.filename,
            file_type,
            size_str,
            file.owner,
            created_time,
            modified_time,
            file.file_status
        );
    }
    info!(directory, file_count, "Directory listing completed");
    Ok(true)
}

#[instrument(skip(settings))]
async fn rename_file(
    settings: Arc<InternalSettings>,
    old_path: String,
    new_path: String,
) -> Result<bool, Box<dyn Error>> {
    info!(old_path, new_path, "Starting renaming file");
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;

    client
        .rename_file(RenameFileRequest {
            old_path: old_path.clone(),
            new_path: new_path.clone(),
            user_id: "1".to_string(),
        })
        .await?;
    info!(old_path, new_path, "File rename completed successfully");
    Ok(true)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let settings = Arc::new(load_config());

    // Initialize tracing with the specified log level
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| settings.log_level.clone());

    let filter = tracing_subscriber::EnvFilter::new("info")
        .add_directive(format!("cli={log_level}").parse().unwrap());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();
    debug!("CLI configuration loaded: {:?}", settings);
    info!("DAFS CLI starting");
    info!("Master node: {}", settings.master_endpoints.join(", "));

    let result = match cli.command {
        Some(Commands::Upload { file, remote_path }) => {
            let remote = remote_path.unwrap_or_else(|| {
                std::path::Path::new(&file)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(&file)
                    .to_string()
            });
            info!("Uploading {} to {}", file, remote);
            upload_file(Arc::clone(&settings), file).await.map(|resp| {
                info!("File uploaded successfully");
                info!("Blocks created: {}", resp.blocks.len());
            })
        }
        Some(Commands::Download {
            remote_path,
            output,
        }) => {
            info!("Downloading {} to {}", remote_path, output);
            download_file(Arc::clone(&settings), remote_path, output)
                .await
                .map(|_| info!("File downloaded successfully"))
        }
        Some(Commands::List { path }) => list_directory(Arc::clone(&settings), path)
            .await
            .map(|_| ()),
        Some(Commands::Rename { old_path, new_path }) => {
            info!("Renaming/Moving from {} to {}", old_path, new_path);
            rename_file(Arc::clone(&settings), old_path, new_path)
                .await
                .map(|_| info!("File renamed/moved successfully"))
        }
        None => {
            error!("No command provided. Use --help for usage information.");
            std::process::exit(1);
        }
    };

    if let Err(e) = result {
        error!("Error: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests;
