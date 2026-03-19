use clap::{Parser, Subcommand};
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use fscore::DafsSettings;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info};

// ─── Display helpers ──────────────────────────────────────────────────────────

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

// ─── Config ───────────────────────────────────────────────────────────────────

/// Loads DafsSettings by merging defaults → config.toml → CLI flags.
pub fn load_config() -> DafsSettings {
    let cli = Cli::parse();
    info!("Loading CLI configuration from '{}'", cli.config_path);

    let has_master_endpoints = !cli.master_endpoints.is_empty();
    let master_endpoints = cli.master_endpoints.clone();

    let mut figment = Figment::new()
        .merge(Serialized::defaults(DafsSettings::default()))
        .merge(Toml::file(&cli.config_path))
        .merge(Serialized::globals(cli));

    if has_master_endpoints {
        figment = figment.merge(("master_endpoints", master_endpoints));
    }

    figment.extract().expect("Error loading configuration")
}

// ─── CLI argument structs ─────────────────────────────────────────────────────

/// DAFS CLI – Distributed Asynchronous File System client
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

    /// Default user id sent to metadata service
    #[arg(long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    default_user_id: Option<String>,

    /// Upload block read chunk size in bytes
    #[arg(long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    upload_chunk_size: Option<usize>,

    /// Path to persisted access token
    #[arg(long, global = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    access_token_path: Option<String>,

    #[command(subcommand)]
    #[serde(skip)]
    command: Option<Commands>,

    #[arg(short, long, default_value = "config.toml", global = true)]
    #[serde(skip)]
    pub config_path: String,
}

#[derive(Subcommand, Debug, Serialize, Deserialize)]
enum Commands {
    /// Register a new user account
    Register {
        #[arg(long)]
        username: String,
        #[arg(long)]
        password: String,
    },
    /// Login and store access token locally
    Login {
        #[arg(long)]
        username: String,
        #[arg(long)]
        password: String,
    },
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
    /// Delete a file from the distributed file system
    Delete {
        #[arg(short, long)]
        file_path: Option<String>,
    },
    /// List directory contents
    List {
        /// Directory path to list
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Rename or move a file
    Rename {
        #[arg(short, long)]
        old_path: Option<String>,
        #[arg(short, long)]
        new_path: Option<String>,
    },
}

// ─── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let settings = Arc::new(load_config());

    let _guard = common::init_tracing("cli", &settings.log_level);
    debug!("CLI configuration loaded: {:?}", settings);
    info!("DAFS CLI starting");
    info!("Master node: {}", settings.master_endpoints.join(", "));

    if let Err(e) = run(cli, settings).await {
        error!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run(
    cli: Cli,
    settings: Arc<DafsSettings>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match cli.command {
        Some(Commands::Register { username, password }) => {
            fscore::register_user(Arc::clone(&settings), username, password).await?;
            info!("User registered successfully");
        }
        Some(Commands::Login { username, password }) => {
            fscore::login_user(Arc::clone(&settings), username, password).await?;
            info!("Login successful");
        }
        Some(Commands::Upload { file, remote_path }) => {
            let token = fscore::read_access_token(&settings.access_token_path)?;
            let remote = remote_path.unwrap_or_else(|| {
                std::path::Path::new(&file)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&file)
                    .to_string()
            });
            info!("Uploading {} to {}", file, remote);
            let resp = fscore::upload_file(Arc::clone(&settings), file, remote, &token).await?;
            info!("File uploaded successfully");
            info!("Blocks created: {}", resp.blocks.len());
        }
        Some(Commands::Download {
            remote_path,
            output,
        }) => {
            let token = fscore::read_access_token(&settings.access_token_path)?;
            info!("Downloading {} to {}", remote_path, output);
            fscore::download_file(Arc::clone(&settings), remote_path, output, &token).await?;
            info!("File downloaded successfully");
        }
        Some(Commands::List { path }) => {
            let token = fscore::read_access_token(&settings.access_token_path)?;
            let dir = path.unwrap_or_else(|| settings.default_list_path.clone());
            let response =
                fscore::list_directory(Arc::clone(&settings), dir.clone(), &token).await?;
            let file_count = response.files.len();
            println!(
                "{:<30} {:<12} {:<15} {:<15} {:<20} {:<20} {:<12}",
                "Filename", "Type", "Size", "Owner", "Created", "Modified", "Status"
            );
            println!("{}", "-".repeat(124));
            for file in response.files {
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
                println!(
                    "{:<30} {:<12} {:<15} {:<15} {:<20} {:<20} {:<12}",
                    file.filename,
                    file_type,
                    size_str,
                    file.owner,
                    format_timestamp(file.created_at),
                    format_timestamp(file.last_modified_at),
                    file.file_status
                );
            }
            info!(file_count, directory = dir, "Directory listing completed");
        }
        Some(Commands::Rename { old_path, new_path }) => {
            let token = fscore::read_access_token(&settings.access_token_path)?;
            let old = old_path.unwrap_or_else(|| settings.default_rename_old_path.clone());
            let new = new_path.unwrap_or_else(|| settings.default_rename_new_path.clone());
            info!("Renaming/Moving from {} to {}", old, new);
            fscore::rename_file(Arc::clone(&settings), old, new, &token).await?;
            info!("File renamed/moved successfully");
        }
        Some(Commands::Delete { file_path }) => {
            let token = fscore::read_access_token(&settings.access_token_path)?;
            let path = file_path.unwrap_or_else(|| settings.default_rename_old_path.clone());
            fscore::delete_file(Arc::clone(&settings), path, &token).await?;
            info!("File deleted successfully");
        }
        None => {
            error!("No command provided. Use --help for usage information.");
            std::process::exit(1);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
