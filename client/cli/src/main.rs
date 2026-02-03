use clap::{Parser, Subcommand};
use common::dfs::data_node_service_client::DataNodeServiceClient;
use common::dfs::metadata_service_client::MetadataServiceClient;
use common::dfs::write_block_request::RequestData;
use common::dfs::{
    CreateFileRequest, CreateFileResponse, GetFileRequest, ListDirectoryRequest,
    ListDirectoryResponse, ReadBlockRequest, ReadBlockResponse, WriteBlockRequest,
    WriteBlockResponse,
};
use std::error::Error;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::mem::offset_of;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::current;
use tokio::fs::{create_dir_all, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, instrument, warn};

/// DAFS CLI - Distributed Asynchronous File System client
#[derive(Parser, Debug)]
#[command(name = "dafs-cli")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Master node endpoint
    #[arg(short, long, default_value = "http://[::1]:9090")]
    master: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
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
}

#[instrument(skip(file_handle, data_node_endpoint))]
async fn upload_block(
    block_id: String,
    data_node_endpoint: String,
    offset: u64,
    block_size: i64,
    file_handle: Arc<std::fs::File>,
) -> Result<WriteBlockResponse, String> {
    info!(block_id, "Starting block upload");
    let (tx, rx) = mpsc::channel(5);
    let mut client = DataNodeServiceClient::connect(data_node_endpoint)
        .await
        .map_err(|e| e.to_string())?;
    let response_fut = client.write_block(ReceiverStream::new(rx));
    tx.send(WriteBlockRequest {
        request_data: Some(RequestData::Metadata(common::dfs::BlockMetadata {
            block_id,
            total_size: block_size,
        })),
    })
    .await
    .map_err(|e| e.to_string())?;
    let mut bytes_remaining = block_size as usize;
    let mut internal_block_offset = offset;
    while bytes_remaining > 0 {
        let bytes_to_read = std::cmp::min(bytes_remaining, 64 * 1024);
        let mut sending_buffer = vec![0u8; bytes_to_read];
        let file = Arc::clone(&file_handle);
        let reading = tokio::task::spawn_blocking(move || {
            file.read_at(&mut sending_buffer, internal_block_offset)
                .map(|n| (n, sending_buffer))
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        let (bytes_read, sending_buffer) = reading;
        if bytes_read == 0 {
            break;
        }

        tx.send(WriteBlockRequest {
            request_data: Some(RequestData::ChunkData(sending_buffer)),
        })
        .await
        .map_err(|e| e.to_string())?;

        bytes_remaining -= bytes_read;
        internal_block_offset += bytes_read as u64;
    }
    drop(tx);
    let response = response_fut.await.map_err(|e| e.to_string())?;
    Ok(response.into_inner())
}

#[instrument(skip(file_handle, data_node_endpoint))]
async fn download_block(
    block_id: String,
    data_node_endpoint: String,
    offset: u64,
    file_handle: Arc<std::fs::File>,
) -> Result<bool, String> {
    info!(block_id, "Starting block download");
    let mut client = DataNodeServiceClient::connect(data_node_endpoint)
        .await
        .map_err(|e| e.to_string())?;

    let mut current_offset = offset;
    let mut stream = client
        .read_block(ReadBlockRequest {
            block_id: block_id.clone(),
        })
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    let mut writing_tasks = JoinSet::new();
    while let Some(msg) = stream.next().await {
        let data = msg.map_err(|e| e.to_string())?.chunk_data;
        let data_len = data.len() as u64;
        let write_offset = current_offset;
        let file = Arc::clone(&file_handle);
        writing_tasks.spawn_blocking(move || file.write_at(&data, write_offset));
        current_offset += data_len;

        if writing_tasks.len() > 10 {
            writing_tasks.join_next().await;
        }
    }

    while let Some(res) = writing_tasks.join_next().await {
        res.map_err(|err| err.to_string())?
            .map_err(|err| err.to_string())?;
    }
    info!("Block was downloaded correctly");
    Ok(true)
}

#[instrument]
async fn upload_file(
    master_node_endpoint: String,
    file_path: String,
) -> Result<CreateFileResponse, Box<dyn Error>> {
    info!(file_path, "Starting file upload");
    let mut client = MetadataServiceClient::connect(master_node_endpoint).await?;

    let file_handle = std::fs::File::open(&file_path)?;
    let create_file_response = client
        .create_file(CreateFileRequest {
            path: file_path.clone(),
            size: file_handle.metadata()?.size() as i64, //TODO: file can be larger than 4GB
            user_id: "1".to_string(),
        })
        .await?
        .into_inner();
    let block_assignments = &create_file_response.blocks;

    let shared_file_handle = Arc::new(file_handle);
    let mut set = JoinSet::new();
    let mut block_size = -1;
    for block_assignment in block_assignments.iter() {
        if block_size == -1 {
            block_size = block_assignment.block_size;
        }
        let block_offset = block_assignment.order * block_size;
        let block_id = block_assignment.block_id.clone();
        set.spawn(upload_block(
            block_id,
            block_assignment.node_address.clone(),
            block_offset as u64,
            block_assignment.block_size,
            Arc::clone(&shared_file_handle),
        ));
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| e.to_string())??;
    }
    Ok(create_file_response)
}

#[instrument]
async fn download_file(
    master_node_endpoint: String,
    file_path: String,
    out_file_path: String,
) -> Result<bool, Box<dyn Error>> {
    info!(file_path, "Starting file download");
    let mut client = MetadataServiceClient::connect(master_node_endpoint).await?;

    let get_file_response = client
        .get_file_metadata(GetFileRequest {
            path: file_path.clone(),
            user_id: "1".to_string(),
        })
        .await?
        .into_inner();
    let block_assignments = &get_file_response.blocks;
    let file_handle = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&out_file_path)?;
    file_handle.set_len(get_file_response.size as u64)?;
    let shared_file_handle = Arc::new(file_handle);
    let mut set = JoinSet::new();

    let mut block_size = -1;
    for block_assignment in block_assignments.iter() {
        if block_size == -1 {
            block_size = block_assignment.block_size;
        }
        let block_offset = block_assignment.order * block_size;
        let block_id = block_assignment.block_id.clone();
        set.spawn(download_block(
            block_id,
            block_assignment.node_address.clone(),
            block_offset as u64,
            Arc::clone(&shared_file_handle),
        ));
    }

    while let Some(res) = set.join_next().await {
        res.map_err(|e| e.to_string())??;
    }
    Ok(true)
}

#[instrument]
async fn list_directory(
    master_node_endpoint: String,
    directory: String,
) -> Result<bool, Box<dyn Error>> {
    info!(directory, "Starting listing directory");
    let mut client = MetadataServiceClient::connect(master_node_endpoint).await?;

    let get_file_response = client
        .list_directory(ListDirectoryRequest {
            path: directory.clone(),
        })
        .await?
        .into_inner();
    for file in get_file_response.files {
        println!(
            "Dir: {} ---- Size: {} bytes ---- {}",
            file.is_directory, file.size, file.filename
        );
    }
    Ok(true)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize tracing with the specified log level
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| cli.log_level.clone());

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_level))
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();

    info!("DAFS CLI starting");
    info!("Master node: {}", cli.master);

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
            upload_file(cli.master, file).await.map(|resp| {
                info!("File uploaded successfully");
                info!("Blocks created: {}", resp.blocks.len());
            })
        }
        Commands::Download {
            remote_path,
            output,
        } => {
            info!("Downloading {} to {}", remote_path, output);
            download_file(cli.master, remote_path, output)
                .await
                .map(|_| info!("File downloaded successfully"))
        }
        Commands::List { path } => list_directory(cli.master, path).await.map(|_| ()),
    };

    if let Err(e) = result {
        error!("Error: {}", e);
        std::process::exit(1);
    }
}
