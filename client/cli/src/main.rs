use clap::{Parser, Subcommand};
use common::dfs::data_node_service_client::DataNodeServiceClient;
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    CreateFileRequest, CreateFileResponse, GetFileRequest, ListDirectoryRequest, ReadBlockRequest,
    RenameFileRequest, WriteBlockRequest, WriteBlockResponse,
};
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::OpenOptions;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn};

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

    let mut figment = Figment::new()
        .merge(Serialized::defaults(InternalSettings {
            mpsc_upload_channel_size: 5,
            mpsc_download_channel_size: 5,
            master_endpoints: vec!["http://[::1]:9090".to_string()],
            upload_concurrency: 2,
            download_concurrency: 10,
            log_level: "info".to_string(),
        }))
        .merge(Toml::file(&cli.config_path));

    if !cli.master_endpoints.is_empty() {
        figment = figment.merge(("master_endpoints", cli.master_endpoints));
    }

    figment.extract().expect("Error loading configuration")
}

/// DAFS CLI - Distributed Asynchronous File System client
#[derive(Parser, Debug)]
#[command(name = "dafs-cli")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Master node endpoint
    #[arg(short, long, default_value = "http://[::1]:9090")]
    master_endpoints: Vec<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,
}

#[derive(Subcommand, Debug)]
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

#[instrument(skip(file_handle, data_node_endpoint, settings))]
async fn upload_block(
    block_id: String,
    data_node_endpoint: String,
    offset: u64,
    block_size: i64,
    file_handle: Arc<std::fs::File>,
    settings: Arc<InternalSettings>,
) -> Result<WriteBlockResponse, String> {
    info!(block_id, "Starting block upload");
    let (tx, rx) = mpsc::channel(settings.mpsc_upload_channel_size as usize);
    let mut client = DataNodeServiceClient::connect(data_node_endpoint)
        .await
        .map_err(|e| format!("Cannot connect to datanode for upload: {}", e))?;
    let response_fut = client.write_block(ReceiverStream::new(rx));
    tx.send(WriteBlockRequest {
        request_data: Some(RequestData::Metadata(common::dfs::BlockMetadata {
            block_id: block_id.clone(),
            total_size: block_size,
        })),
    })
    .await
    .map_err(|e| format!("Failed to send block metadata: {}", e))?;
    let mut bytes_remaining = block_size as usize;
    let mut internal_block_offset = offset;
    while bytes_remaining > 0 {
        let bytes_to_read = std::cmp::min(bytes_remaining, 64 * 1024); //TODO check block size with master
        let mut sending_buffer = vec![0u8; bytes_to_read];
        let file = Arc::clone(&file_handle);
        let reading = tokio::task::spawn_blocking(move || {
            file.read_at(&mut sending_buffer, internal_block_offset)
                .map(|n| (n, sending_buffer))
        })
        .await
        .map_err(|e| format!("Block read task failed: {}", e))?
        .map_err(|e| {
            format!(
                "Failed to read file at offset {}: {}",
                internal_block_offset, e
            )
        })?;
        let (bytes_read, sending_buffer) = reading;
        if bytes_read == 0 {
            break;
        }

        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::ChunkData(sending_buffer)),
        })
        .await
        .map_err(|e| format!("Failed to send chunk data: {}", e))?;

        bytes_remaining -= bytes_read;
        internal_block_offset += bytes_read as u64;
    }
    drop(tx);
    let response = response_fut.await.map_err(|e| {
        error!("Failed to get write block response: {}", e);
        format!("Failed to get write block response: {}", e)
    })?;
    let inner = response.into_inner();
    info!(
        block_id = &block_id,
        bytes_written = inner.bytes_written,
        "Block upload completed successfully"
    );
    Ok(inner)
}

#[instrument(skip(file_handle, data_node_endpoint, settings))]
async fn download_block(
    block_id: String,
    data_node_endpoint: String,
    offset: u64,
    file_handle: Arc<std::fs::File>,
    settings: Arc<InternalSettings>,
) -> Result<bool, String> {
    info!(block_id, "Starting block download");
    let mut client = DataNodeServiceClient::connect(data_node_endpoint)
        .await
        .map_err(|e| format!("Cannot connect to datanode {}", e.to_string()))?;

    let mut current_offset = offset;
    let mut stream = client
        .read_block(ReadBlockRequest {
            block_id: block_id.clone(),
        })
        .await
        .map_err(|e| format!("Couldnt read block {}", e.to_string()))?
        .into_inner();
    let mut writing_tasks = JoinSet::new();
    while let Some(msg) = stream.next().await {
        let data = msg
            .map_err(|e| format!("Failed to receive block chunk: {}", e))?
            .chunk_data;
        let data_len = data.len() as u64;
        let write_offset = current_offset;
        let file = Arc::clone(&file_handle);
        writing_tasks.spawn_blocking(move || file.write_at(&data, write_offset));
        current_offset += data_len;

        if writing_tasks.len() > settings.download_concurrency {
            writing_tasks.join_next().await;
        }
    }

    while let Some(res) = writing_tasks.join_next().await {
        res.map_err(|err| {
            error!(block_id, "Block write task failed: {}", err);
            format!("Block write task failed: {}", err)
        })?
        .map_err(|err| {
            error!(block_id, "Failed to write block data to file: {}", err);
            format!("Failed to write block data to file: {}", err)
        })?;
    }
    info!(block_id, "Block downloaded successfully");
    Ok(true)
}

#[instrument(skip(settings))]
async fn upload_file(
    settings: Arc<InternalSettings>,
    file_path: String,
) -> Result<CreateFileResponse, Box<dyn Error>> {
    info!(file_path, "Starting file upload");
    let mut client = MetadataServiceClient::connect(settings.master_endpoints[0].clone()).await?;

    let file_handle = std::fs::File::open(&file_path)?;
    let create_file_response = client
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
    for block_assignment in create_file_response.blocks.clone() {
        if block_size == -1 {
            block_size = block_assignment.block_size;
        }
        let block_offset = block_assignment.order * block_size;
        let block_id = block_assignment.block_id.clone();
        let f_handle = Arc::clone(&shared_file_handle);
        let settings_clone = Arc::clone(&settings);
        let sem = Arc::clone(&semaphore); // Clonamos el Arc fuera

        set.spawn(async move {
            let _permit = sem.acquire_owned().await.map_err(|e| e.to_string())?;

            let result = upload_block(
                block_id,
                block_assignment.node_address.clone(),
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
            format!("Block upload task panicked: {}", e)
        })??;
    }
    info!(
        file_path,
        blocks = create_file_response.blocks.len(),
        "File upload completed successfully"
    );
    Ok(create_file_response)
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
        .open(&out_file_path)?;
    file_handle.set_len(get_file_response.size as u64)?;
    let shared_file_handle = Arc::new(file_handle);
    let semaphore = Arc::new(Semaphore::new(settings.download_concurrency));
    let mut set = JoinSet::new();

    let mut block_size = -1;
    for block_assignment in get_file_response.blocks.clone() {
        if block_size == -1 {
            block_size = block_assignment.block_size;
        }
        let permit = semaphore.clone().acquire_owned().await?;
        let block_offset = block_assignment.order * block_size;
        let block_id = block_assignment.block_id.clone();
        let f_handle = Arc::clone(&shared_file_handle);
        let settings_clone = Arc::clone(&settings);

        set.spawn(async move {
            let result = download_block(
                block_id,
                block_assignment.node_address.clone(),
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
            format!("Block download task panicked: {}", e)
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
    for file in get_file_response.files {
        println!(
            "Dir: {} ---- Size: {} bytes ---- {}",
            file.is_directory, file.size, file.filename
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
    let settings = Arc::new(load_config());

    // Initialize tracing with the specified log level
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| settings.log_level.clone());

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_level))
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();

    info!("DAFS CLI starting");
    info!("Master node: {}", settings.master_endpoints.join(", "));

    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Upload { file, remote_path } => {
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
        Commands::Download {
            remote_path,
            output,
        } => {
            info!("Downloading {} to {}", remote_path, output);
            download_file(Arc::clone(&settings), remote_path, output)
                .await
                .map(|_| info!("File downloaded successfully"))
        }
        Commands::List { path } => list_directory(Arc::clone(&settings), path)
            .await
            .map(|_| ()),

        Commands::Rename { old_path, new_path } => {
            info!("Renaming/Moving from {} to {}", old_path, new_path);
            rename_file(Arc::clone(&settings), old_path, new_path)
                .await
                .map(|_| info!("File renamed/moved successfully"))
        }
    };

    if let Err(e) = result {
        error!("Error: {}", e);
        std::process::exit(1);
    }
}
