use bimap::BiMap;
use clap::{Parser, Subcommand};
use core::error;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use fscore::DafsSettings;
use fuser::{
    mount2, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, INodeNo, MountOption,
    ReplyAttr, ReplyDirectory, ReplyEntry, Request,
};
use libc::{EIO, ENOENT, ENOTDIR};
use serde::{de, Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, UNIX_EPOCH};
use std::{ffi::OsStr, time::SystemTime};
use tokio::{runtime::Runtime, sync::RwLock};
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

struct DAFSfs {
    runtime: Arc<tokio::runtime::Runtime>,
    settings: Arc<fscore::DafsSettings>,
    token: RwLock<String>,
    inode_map: RwLock<BiMap<u64, String>>,
    next_inode: AtomicU64,
    ttl: Duration,
}

impl Filesystem for DAFSfs {
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        info!(ino = ino.0, "Getting attr");
        debug!(ino = ino.0, "Looking up path for inode");
        if ino == fuser::INodeNo(1) {
            reply.attr(&self.ttl, &self.root_attr());
            return;
        }

        let path = {
            let map = self.inode_map.blocking_read();
            match map.get_by_left(&ino.0) {
                Some(p) => p.to_string(),
                None => {
                    reply.error(Errno::from_i32(ENOENT));
                    return;
                }
            }
        };
        let token = self.token.blocking_read().clone();

        match self.runtime.block_on(fscore::get_file_metadata(
            self.settings.clone(),
            path,
            &token,
        )) {
            Ok(file_metadata) => {
                reply.attr(
                    &self.ttl,
                    &self.file_attr(
                        ino,
                        file_metadata.size,
                        file_metadata.created_at,
                        FileType::RegularFile,
                    ),
                );
            }
            Err(err) => {
                if self.is_not_found_error(&err.to_string()) {
                    reply.error(Errno::from_i32(ENOENT));
                } else {
                    reply.error(Errno::from_i32(EIO));
                }
            }
        }
    }

    // 2. Permitir que el OS encuentre "hola.txt" dentro de la raíz
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        info!(
            parent = parent.0,
            name = name.to_string_lossy().to_string(),
            "Looking up entry"
        );
        let parent_path = {
            let map = self.inode_map.blocking_read();
            match map.get_by_left(&parent.0) {
                Some(p) => p.to_string(),
                None => {
                    reply.error(Errno::from_i32(ENOENT));
                    return;
                }
            }
        };
        info!(
            parent_path = parent_path,
            name = name.to_string_lossy().to_string(),
            "Resolved parent path"
        );
        let full_path = self.join_path(&parent_path, name);
        let token = self.token.blocking_read().clone();
        let file_metadata = self.runtime.block_on(fscore::get_file_metadata(
            self.settings.clone(),
            full_path.clone(),
            &token,
        ));
        info!(full_path = full_path, "Got file metadata");
        let file_metadata = match file_metadata {
            Ok(m) => m,
            Err(err) => {
                if self.is_not_found_error(&err.to_string()) {
                    reply.error(Errno::from_i32(ENOENT));
                } else {
                    reply.error(Errno::from_i32(EIO));
                }
                return;
            }
        };
        info!(
            full_path = full_path,
            size = file_metadata.size,
            "File metadata retrieved successfully"
        );
        let ino = {
            let mut map = self.inode_map.blocking_write();
            match map.get_by_right(&full_path) {
                Some(ino) => *ino,
                None => {
                    let new_ino = self.next_inode.fetch_add(1, Ordering::SeqCst);
                    map.insert(new_ino, full_path.clone());
                    new_ino
                }
            }
        };
        info!(full_path = full_path, ino = ino, "Assigned inode for entry");
        reply.entry(
            &self.ttl,
            &self.file_attr(
                fuser::INodeNo(ino),
                file_metadata.size,
                file_metadata.created_at,
                FileType::RegularFile,
            ),
            fuser::Generation(0),
        );
    }

    // 3. El corazón del `ls`: listar las entradas del directorio
    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let path = {
            let map = self.inode_map.blocking_read();
            match map.get_by_left(&ino.0) {
                Some(path) => path.to_string(),
                None => {
                    reply.error(Errno::from_i32(ENOENT));
                    return;
                }
            }
        };
        info!(ino = ino.0, path = path, "Reading directory");
        //Check if dir or file todo
        let token = self.token.blocking_read().clone();
        let list_directory_response = self.runtime.block_on(fscore::list_directory(
            self.settings.clone(),
            path.clone(),
            &token,
        ));
        let list_directory_response = match list_directory_response {
            Ok(m) => m,
            Err(err) => {
                if self.is_not_found_error(&err.to_string()) {
                    error!(path = path, "Directory not found");
                    reply.error(Errno::from_i32(ENOENT));
                } else if self.is_not_directory_error(&err.to_string()) {
                    error!(path = path, "Not a directory");
                    reply.error(Errno::from_i32(ENOTDIR));
                } else {
                    error!(path = path, "Error listing directory: {}", err);
                    reply.error(Errno::from_i32(EIO));
                }
                return;
            }
        };
        info!(
            path = path,
            file_count = list_directory_response.files.len(),
            "Directory listed successfully"
        );
        let parent_path = self.parent_path(&path);
        let parent_ino = {
            let map = self.inode_map.blocking_read();
            map.get_by_right(&parent_path).copied().unwrap_or(1)
        };

        let mut files = list_directory_response.files;
        files.sort_by(|a, b| a.filename.cmp(&b.filename));
        info!(
            path = path,
            file_count = files.len(),
            "Sorted directory entries"
        );
        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (
                fuser::INodeNo(parent_ino),
                FileType::Directory,
                "..".to_string(),
            ),
        ];
        info!(path = path, "Added . and .. entries");
        let mut inode_map = self.inode_map.blocking_write();
        for file in files {
            let file_path = self.join_path(&path, OsStr::new(&file.filename));
            let file_ino = match inode_map.get_by_right(&file_path) {
                Some(file_ino) => *file_ino,
                None => {
                    let new_ino = self.next_inode.fetch_add(1, Ordering::SeqCst);
                    inode_map.insert(new_ino, file_path.clone());
                    new_ino
                }
            };
            entries.push((
                fuser::INodeNo(file_ino),
                if file.is_directory {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                file.filename,
            ));
        }
        info!(
            path = path,
            entry_count = entries.len(),
            "Prepared directory entries for reply"
        );

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // El offset ayuda al kernel a saber por dónde va la lectura
            info!(ino = entry.0 .0, name = entry.2, "Adding directory entry");
            if reply.add(entry.0, (i + 1) as u64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }
}

impl DAFSfs {
    fn intitialize(&self) {
        self.inode_map.blocking_write().insert(1, "/".to_string());
        self.runtime
            .block_on(fscore::login_user(
                self.settings.clone(),
                "pepe".to_string(),
                "pepe".to_string(),
            ))
            .unwrap();
        let token = fscore::read_access_token(&self.settings.access_token_path).unwrap();
        *self.token.blocking_write() = token;
    }

    fn join_path(&self, parent_path: &str, name: &OsStr) -> String {
        let child = name.to_string_lossy();
        if parent_path == "/" {
            format!("/{}", child)
        } else {
            format!("{}/{}", parent_path.trim_end_matches('/'), child)
        }
    }

    fn root_attr(&self) -> FileAttr {
        let now = SystemTime::now();
        FileAttr {
            ino: fuser::INodeNo(1),
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: self.current_uid(),
            gid: self.current_gid(),
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn file_attr(&self, ino: INodeNo, size: i64, created_at_ms: i64, kind: FileType) -> FileAttr {
        let timestamp = UNIX_EPOCH + Duration::from_millis(created_at_ms.max(0) as u64);
        FileAttr {
            ino,
            size: size.max(0) as u64,
            blocks: ((size.max(0) as u64) + 511) / 512,
            atime: timestamp,
            mtime: timestamp,
            ctime: timestamp,
            crtime: timestamp,
            kind,
            perm: if kind == FileType::Directory {
                0o755
            } else {
                0o644
            },
            nlink: if kind == FileType::Directory { 2 } else { 1 },
            uid: self.current_uid(),
            gid: self.current_gid(),
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn is_not_found_error(&self, msg: &str) -> bool {
        let lower = msg.to_lowercase();
        lower.contains("notfound") || lower.contains("not found")
    }

    fn is_not_directory_error(&self, msg: &str) -> bool {
        let lower = msg.to_lowercase();
        lower.contains("not a directory") || lower.contains("enotdir")
    }

    fn parent_path(&self, path: &str) -> String {
        if path == "/" {
            return "/".to_string();
        }

        let trimmed = path.trim_end_matches('/');
        if let Some(idx) = trimmed.rfind('/') {
            if idx == 0 {
                "/".to_string()
            } else {
                trimmed[..idx].to_string()
            }
        } else {
            "/".to_string()
        }
    }

    fn current_uid(&self) -> u32 {
        unsafe { libc::geteuid() as u32 }
    }

    fn current_gid(&self) -> u32 {
        unsafe { libc::getegid() as u32 }
    }
}

// ─── Entry point ──────────────────────────────────────────────────────────────
fn main() {
    let cli = Cli::parse();
    let settings = Arc::new(load_config());

    common::init_tracing("cli", "DEBUG");
    debug!("CLI configuration loaded: {:?}", settings);
    info!("DAFS CLI starting");
    info!("Master node: {}", settings.master_endpoints.join(", "));
    let mut config = Config::default();
    config.mount_options = vec![MountOption::RO, MountOption::AutoUnmount];
    config.acl = fuser::SessionACL::All;
    let fs = DAFSfs {
        inode_map: RwLock::new(BiMap::new()),
        runtime: Arc::new(Runtime::new().unwrap()),
        next_inode: AtomicU64::new(2),
        token: RwLock::new(String::new()),
        ttl: Duration::from_secs(2),
        settings,
    };
    fs.intitialize();
    mount2(fs, "/tmp/DAFS", &config).expect("Failed to mount DAFS");
    // if let Err(e) = run(cli, settings).await {
    //     error!("Error: {}", e);
    //     std::process::exit(1);
    // }
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
