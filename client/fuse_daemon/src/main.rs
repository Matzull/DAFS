use bimap::BiMap;
use clap::{Parser, Subcommand};
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use fscore::DafsSettings;
use fuser::{
    mount2, AccessFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    INodeNo, IoctlFlags, MountOption, ReplyAttr, ReplyDirectory, ReplyEntry, ReplyIoctl,
    ReplyXattr, Request,
};
use libc::{EEXIST, EIO, ENOENT, ENOTDIR};
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use std::{ffi::OsStr, time::SystemTime};
use tokio::{runtime::Runtime, sync::RwLock};
use tracing::{debug, error};

const MOUNTPOINT: &str = "/tmp/DAFS";

fn ensure_mountpoint(path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let p = Path::new(path);
    if !p.exists() {
        std::fs::create_dir_all(p)?;
        debug!(path, "Created missing mountpoint directory");
    }
    Ok(())
}

fn is_likely_mounted(path: &str) -> bool {
    match std::fs::read_to_string("/proc/self/mountinfo") {
        Ok(mountinfo) => mountinfo.lines().any(|line| line.contains(path)),
        Err(_) => false,
    }
}

fn try_unmount(path: &str) {
    let c_path = match CString::new(path) {
        Ok(v) => v,
        Err(err) => {
            error!(path, error = %err, "Invalid mount path");
            return;
        }
    };

    // First attempt: direct lazy unmount.
    let umount_rc = unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_DETACH) };
    if umount_rc == 0 {
        debug!(path, "Unmounted mountpoint using umount2");
        return;
    }

    // Fallback for user-space FUSE unmount.
    let fusermount3 = Command::new("fusermount3").arg("-u").arg(path).status();
    match fusermount3 {
        Ok(status) if status.success() => {
            debug!(path, "Unmounted mountpoint using fusermount3");
            return;
        }
        _ => {}
    }

    let fusermount = Command::new("fusermount").arg("-u").arg(path).status();
    match fusermount {
        Ok(status) if status.success() => {
            debug!(path, "Unmounted mountpoint using fusermount");
        }
        Ok(status) => {
            debug!(
                path,
                code = status.code(),
                "fusermount returned non-zero status"
            );
        }
        Err(err) => {
            debug!(path, error = %err, "fusermount command unavailable or failed to run");
        }
    }
}

// ─── Config ───────────────────────────────────────────────────────────────────

/// Loads DafsSettings by merging defaults → config.toml → CLI flags.
pub fn load_config() -> DafsSettings {
    let cli = Cli::parse();
    debug!("Loading CLI configuration from '{}'", cli.config_path);

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

struct DAFSEntry {
    dirty: bool,
    remote_path: String,
    size: u64,
    buffer: Vec<u8>,
    base_offset: u64,
    next_expected_offset: u64,
}
struct DAFSState {
    files: HashMap<FileHandle, DAFSEntry>,
}

struct DAFSfs {
    runtime: Arc<tokio::runtime::Runtime>,
    settings: Arc<fscore::DafsSettings>,
    token: RwLock<Arc<String>>,
    inode_map: RwLock<BiMap<u64, String>>,
    next_inode: AtomicU64,
    ttl: Duration,
    state: Arc<RwLock<DAFSState>>,
    directories: RwLock<HashSet<String>>,  // Track directory paths locally
}

impl Filesystem for DAFSfs {
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        debug!(ino = ino.0, "Looking up path for inode");
        if ino == fuser::INodeNo(1) {
            reply.attr(&self.ttl, &self.root_attr());
            return;
        }

        let path = match self.get_path_by_inode(ino) {
            Some(p) => p,
            None => {
                error!(ino = ino.0, "Inode not found in map");
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };
        
        // Check if this is a directory
        if self.directories.blocking_read().contains(&path) {
            debug!(path = path, ino = ino.0, "Entry is a directory");
            reply.attr(
                &self.ttl,
                &self.file_attr(ino, 0, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64, FileType::Directory, 0o755),
            );
            return;
        }
        
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
                        file_metadata.mode,
                    ),
                );
            }
            Err(err) => {
                reply.error(self.map_error(&err));
            }
        }
    }

    // 2. Permitir que el OS encuentre "hola.txt" dentro de la raíz
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        debug!(
            parent = parent.0,
            name = name.to_string_lossy().to_string(),
            "Looking up entry"
        );
        let parent_path = match self.get_path_by_inode(parent) {
            Some(p) => p,
            None => {
                error!(ino = parent.0, "Inode not found in map");
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };
        debug!(
            parent_path = parent_path,
            name = name.to_string_lossy().to_string(),
            "Resolved parent path"
        );
        let full_path = self.join_path(&parent_path, name);
        
        // Check if this is a directory first
        if self.directories.blocking_read().contains(&full_path) {
            debug!(full_path = full_path, "Entry is a directory");
            let ino = self.get_new_inode(full_path.clone());
            reply.entry(
                &self.ttl,
                &self.file_attr(ino, 0, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64, FileType::Directory, 0o755),
                fuser::Generation(0),
            );
            return;
        }
        
        let token = self.token.blocking_read().clone();
        let file_metadata = self.runtime.block_on(fscore::get_file_metadata(
            self.settings.clone(),
            full_path.clone(),
            &token,
        ));
        debug!(full_path = full_path, "Got file metadata");
        let file_metadata = match file_metadata {
            Ok(m) => m,
            Err(err) => {
                reply.error(self.map_error(&err));
                return;
            }
        };
        debug!(
            full_path = full_path,
            size = file_metadata.size,
            "File metadata retrieved successfully"
        );
        let ino = self.get_new_inode(full_path.clone());
        debug!(
            full_path = full_path,
            ino = ino.0,
            "Assigned inode for entry"
        );
        reply.entry(
            &self.ttl,
            &self.file_attr(
                ino,
                file_metadata.size,
                file_metadata.created_at,
                FileType::RegularFile,
                file_metadata.mode,
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
        let path = match self.get_path_by_inode(ino) {
            Some(p) => p,
            None => {
                error!(ino = ino.0, "Inode not found in map");
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };
        debug!(ino = ino.0, path = path, "Reading directory");
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
                if self.is_not_directory_error(&err.to_string()) {
                    error!(path = path, "Not a directory");
                }
                reply.error(self.map_error(&err));
                return;
            }
        };
        debug!(
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
        debug!(
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
        debug!(path = path, "Added . and .. entries");
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
        debug!(
            path = path,
            entry_count = entries.len(),
            "Prepared directory entries for reply"
        );

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // El offset ayuda al kernel a saber por dónde va la lectura
            debug!(ino = entry.0 .0, name = entry.2, "Adding directory entry");
            if reply.add(entry.0, (i + 1) as u64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        debug!(
            parent = parent.0,
            name = name.to_string_lossy().to_string(),
            "Looking up entry"
        );
        let parent_path = match self.get_path_by_inode(parent) {
            Some(p) => p,
            None => {
                error!(ino = parent.0, "Inode not found in map");
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };
        debug!(
            parent_path = parent_path,
            name = name.to_string_lossy().to_string(),
            "Resolved parent path"
        );
        let new_file_path = self.join_path(&parent_path, name);

        //Check if inode already exists
        if self.get_inode_by_path(&new_file_path).is_some() {
            error!(path = new_file_path, "File already exists");
            reply.error(Errno::from_i32(EEXIST));
            return;
        }

        let token = self.token.blocking_read().clone();

        match self.runtime.block_on(fscore::create_file(
            self.settings.clone(),
            new_file_path.clone(),
            &token,
            0,
        )) {
            Ok(_) => {
                let ino = self.get_new_inode(new_file_path.clone());
                // let _ = self.runtime.block_on(fscore::set_file_status_ready(
                //     self.settings.clone(),
                //     new_file_path.clone(),
                //     &token,
                // ));
                let fh = FileHandle(ino.0);
                self.state.blocking_write().files.insert(
                    fh,
                    DAFSEntry {
                        dirty: true,
                        remote_path: new_file_path,
                        size: 0,
                        buffer: Vec::new(),
                        base_offset: 0,
                        next_expected_offset: 0,
                    },
                );
                reply.created(
                    &self.ttl,
                    &self.file_attr(ino, 0, 0, FileType::RegularFile, 0o644),
                    fuser::Generation(0),
                    fh,
                    FopenFlags::empty(),
                );
            }
            Err(err) => {
                reply.error(self.map_error(&err));
            }
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: fuser::OpenFlags, reply: fuser::ReplyOpen) {
        debug!(ino = ino.0, flags = ?flags, "Opening file");
        let path = match self.get_path_by_inode(ino) {
            Some(p) => p,
            None => {
                error!(ino = ino.0, "Inode not found in map");
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };
        let token = self.token.blocking_read().clone();
        let file_meta = match self.runtime.block_on(fscore::get_file_metadata(
            self.settings.clone(),
            path.clone(),
            &token,
        )) {
            Ok(file_meta) => file_meta,
            Err(err) => {
                reply.error(self.map_error(&err));
                return;
            }
        };

        let is_truncate = (flags.0 & libc::O_TRUNC) != 0;
        if is_truncate {
            if let Err(err) = self.resize_remote_file(&path, 0, &token) {
                error!(path = path, error = %err, "Failed to truncate file on open");
                reply.error(self.map_error(&err));
                return;
            }
        }

        // O_APPEND => append writes at end. Otherwise we treat writes as overwrite from 0.
        let is_append = (flags.0 & libc::O_APPEND) != 0;
        let effective_size = if is_truncate {
            0
        } else {
            file_meta.size.max(0) as u64
        };
        let base_offset = if is_append { effective_size } else { 0 };
        self.state.blocking_write().files.insert(
            FileHandle(ino.0),
            DAFSEntry {
                dirty: false,
                remote_path: path,
                size: effective_size,
                buffer: Vec::new(),
                base_offset,
                next_expected_offset: base_offset,
            },
        );

        reply.opened(FileHandle(ino.0), FopenFlags::empty());
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        debug!(
            ino = ino.0,
            fh = fh.0,
            offset,
            len = data.len(),
            "Write request"
        );

        let (needs_realign_flush, flush_threshold) = {
            let state_lock = self.state.blocking_read();
            let entry = match state_lock.files.get(&fh) {
                Some(e) => e,
                None => {
                    error!("File handle not found");
                    reply.error(Errno::from_i32(EIO));
                    return;
                }
            };

            let needs_flush = offset != entry.next_expected_offset && !entry.buffer.is_empty();
            let thresh = (self.settings.upload_chunk_size.saturating_mul(64))
                .clamp(1024 * 1024, 8 * 1024 * 1024);
            (needs_flush, thresh)
        };

        // Some workloads (e.g. git index-pack) issue writes at varying offsets.
        // Flush the previous contiguous segment and start buffering at the new offset.
        if needs_realign_flush {
            if let Err(err) = self.flush_pending_buffer(fh, 1) {
                error!(fh = fh.0, error = %err, "Failed to flush before write realignment");
                reply.error(Errno::from_i32(EIO));
                return;
            }
        }

        let buffered_len = {
            let mut state_lock = self.state.blocking_write();
            let entry = match state_lock.files.get_mut(&fh) {
                Some(e) => e,
                None => {
                    error!("File handle not found after realignment");
                    reply.error(Errno::from_i32(EIO));
                    return;
                }
            };

            if offset != entry.next_expected_offset {
                debug!(
                    fh = fh.0,
                    old_next_expected = entry.next_expected_offset,
                    new_offset = offset,
                    "Realigning buffered write segment"
                );
                entry.base_offset = offset;
                entry.next_expected_offset = offset;
                entry.buffer.clear();
            }

            entry.buffer.extend_from_slice(data);
            entry.next_expected_offset = entry.next_expected_offset.saturating_add(data.len() as u64);
            entry.size = entry.size.max(entry.next_expected_offset);
            entry.dirty = true;

            let buffered = entry.buffer.len();
            debug!(
                fh = fh.0,
                offset,
                len = data.len(),
                buffered = buffered,
                "Write buffered"
            );
            buffered
        };

        let should_flush = buffered_len >= flush_threshold;
        if should_flush {
            match self.flush_pending_buffer(fh, flush_threshold) {
                Ok(()) => {
                    debug!(fh = fh.0, threshold = flush_threshold, "Buffered chunk upload completed after write");
                }
                Err(err) => {
                    error!(fh = fh.0, error = %err, "Failed buffered chunk upload during write");
                    reply.error(Errno::from_i32(EIO));
                    return;
                }
            }
        }

        reply.written(data.len() as u32);
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyData,
    ) {
        let path = match self.get_path_by_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };
        let token = self.token.blocking_read().clone();
        let settings = self.settings.clone();
        let read_result = self.runtime.block_on(fscore::read_file_range(
            settings,
            path,
            offset,
            size as u64,
            &token,
        ));
        match read_result {
            Ok(data) => reply.data(&data),
            Err(_) => reply.error(Errno::from_i32(EIO)),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(ino = ino.0, fh = fh.0, "Flush request");
        match self.flush_pending_buffer(fh, 1) {
            Ok(()) => reply.ok(),
            Err(err) => {
                error!(fh = fh.0, error = %err, "Failed to flush buffered data");
                reply.error(Errno::from_i32(EIO));
            }
        }
    }

    fn fsync(
            &self,
            req: &Request,
            ino: INodeNo,
            fh: FileHandle,
            datasync: bool,
            reply: fuser::ReplyEmpty,
        ) {
        debug!(ino = ino.0, fh = fh.0, datasync, "Fsync request");
        self.flush(req, ino, fh, fuser::LockOwner(0), reply);
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(ino = ino.0, fh = fh.0, "Release request");

        // Ensure buffered tail data is persisted even if kernel doesn't issue flush first.
        if let Err(err) = self.flush_pending_buffer(fh, 1) {
            error!(fh = fh.0, error = %err, "Failed to flush buffered data on release");
            reply.error(Errno::from_i32(EIO));
            return;
        }

        let mut state_lock = self.state.blocking_write();
        if let Some(_entry) = state_lock.files.remove(&fh) {
            debug!(fh = fh.0, "File handle released and cleaned up");
        }

        reply.ok();
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        debug!(
            ino = ino.0,
            mode = mode,
            uid = uid,
            gid = gid,
            size = size,
            atime = ?atime,
            mtime = ?mtime,
            ctime = ?ctime,
            crtime = ?crtime,
            chgtime = ?chgtime,
            bkuptime = ?bkuptime,
            flags = ?flags,
            "Setattr request"
        );
        let path = match self.get_path_by_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };
        let token = self.token.blocking_read().clone();
        if let Some(new_size) = size {
            match self.resize_remote_file(&path, new_size, &token) {
                Ok(_) => {
                    // Reset open buffered state for this path after resize.
                    let mut state = self.state.blocking_write();
                    for entry in state.files.values_mut() {
                        if entry.remote_path == path {
                            entry.dirty = false;
                            entry.size = new_size;
                            entry.buffer.clear();
                            if entry.base_offset > new_size {
                                entry.base_offset = new_size;
                            }
                            if entry.next_expected_offset > new_size {
                                entry.next_expected_offset = new_size;
                            }
                        }
                    }

                    match self.runtime.block_on(fscore::get_file_metadata(
                        self.settings.clone(),
                        path,
                        &token,
                    )) {
                        Ok(meta) => reply.attr(
                            &self.ttl,
                            &self.file_attr(ino, meta.size, meta.created_at, FileType::RegularFile, meta.mode),
                        ),
                        Err(_) => reply.attr(
                            &self.ttl,
                            &self.file_attr(ino, new_size as i64, 0, FileType::RegularFile, 0o644),
                        ),
                    }
                }
                Err(err) => {
                    error!(path = path, size = new_size, error = %err, "Failed to resize file on setattr");
                    reply.error(self.map_error(&err));
                }
            }
        } else {
            // Handle chmod: persist mode to backend
            if let Some(new_mode) = mode {
                match self.runtime.block_on(fscore::set_file_permissions(
                    self.settings.clone(),
                    path.clone(),
                    new_mode,
                    &token,
                )) {
                    Ok(_) => {
                        debug!(path = path, mode = new_mode, "chmod succeeded");
                    }
                    Err(err) => {
                        error!(path = path, mode = new_mode, error = %err, "chmod failed");
                        reply.error(self.map_error(&err));
                        return;
                    }
                }
            }

            // chown/utimens are accepted as no-ops so tools like mv/cp
            // can preserve metadata without failing the operation.
            if self.directories.blocking_read().contains(&path) {
                reply.attr(
                    &self.ttl,
                    &self.file_attr(
                        ino,
                        0,
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_else(|_| Duration::from_secs(0))
                            .as_millis() as i64,
                        FileType::Directory,
                        0o755,
                    ),
                );
                return;
            }

            match self.runtime.block_on(fscore::get_file_metadata(
                self.settings.clone(),
                path,
                &token,
            )) {
                Ok(meta) => reply.attr(
                    &self.ttl,
                    &self.file_attr(ino, meta.size, meta.created_at, FileType::RegularFile, meta.mode),
                ),
                Err(err) => {
                    error!(ino = ino.0, error = %err, "Setattr metadata refresh failed");
                    reply.error(self.map_error(&err));
                }
            }
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            parent = parent.0,
            new_parent = newparent.0,
            current_name = name.to_str(),
            new_name = newname.to_str(),
            "Rename file"
        );

        let parent_path = match self.get_path_by_inode(parent) {
            Some(p) => p,
            None => {
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };
        let new_parent_path = match self.get_path_by_inode(newparent) {
            Some(p) => p,
            None => {
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };

        let old_path = self.join_path(&parent_path, name);
        let new_path = self.join_path(&new_parent_path, newname);
        let token = self.token.blocking_read().clone();

        match self.runtime.block_on(fscore::rename_file(
            self.settings.clone(),
            old_path.clone(),
            new_path.clone(),
            &token,
        )) {
            Ok(_) => {
                let mut inode_map = self.inode_map.blocking_write();
                if let Some(ino) = inode_map.get_by_right(&old_path).copied() {
                    inode_map.remove_by_left(&ino);
                    inode_map.insert(ino, new_path);
                }
                reply.ok();
            }
            Err(err) => {
                error!(old_path = old_path, new_path = new_path, error = %err, "Rename failed");
                reply.error(self.map_error(&err));
            }
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        debug!(
            parent = parent.0,
            name = name.to_str(),
            "Deleting file"
        );

        let parent_path = match self.get_path_by_inode(parent) {
            Some(p) => p,
            None => {
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };

        let file_path = self.join_path(&parent_path, name);

        let token = self.token.blocking_read().clone();

        match self.runtime.block_on(fscore::delete_file(
            self.settings.clone(),
            file_path.clone(),
            &token,
        )) {
            Ok(_) => {
                let mut inode_map = self.inode_map.blocking_write();
                if let Some(ino) = inode_map.get_by_right(&file_path).copied() {
                    inode_map.remove_by_left(&ino);
                }
                reply.ok();
            }
            Err(err) => {
                error!(path = file_path, error = %err, "Delete failed");
                reply.error(self.map_error(&err));
            }
        }
    }

    fn mkdir(
            &self,
            _req: &Request,
            parent: INodeNo,
            name: &OsStr,
            _mode: u32,
            _umask: u32,
            reply: ReplyEntry,
        ) {
        debug!(parent = parent.0, name = name.to_str(), "mkdir request");
        
        let parent_path = match self.get_path_by_inode(parent) {
            Some(p) => p,
            None => {
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };

        let dir_path = self.join_path(&parent_path, name);
        let token = self.token.blocking_read().clone();

        // Call backend to create directory
        match self.runtime.block_on(fscore::create_directory(
            self.settings.clone(),
            dir_path.clone(),
            &token,
        )) {
            Ok(()) => {
                // Add to local cache
                self.directories.blocking_write().insert(dir_path.clone());
                
                // Create and return inode
                let ino = self.get_new_inode(dir_path.clone());
                let now = SystemTime::now();
                let attr = FileAttr {
                    ino,
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
                };
                debug!(path = dir_path, ino = ino.0, "Directory created successfully");
                reply.entry(&self.ttl, &attr, fuser::Generation(0));
            }
            Err(err) => {
                error!(path = dir_path, error = %err, "Failed to create directory");
                reply.error(self.map_error(&err));
            }
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        debug!(parent = parent.0, name = name.to_str(), "rmdir request");
        
        let parent_path = match self.get_path_by_inode(parent) {
            Some(p) => p,
            None => {
                reply.error(Errno::from_i32(ENOENT));
                return;
            }
        };

        let dir_path = self.join_path(&parent_path, name);
        let token = self.token.blocking_read().clone();

        // Call backend to delete directory
        match self.runtime.block_on(fscore::delete_directory(
            self.settings.clone(),
            dir_path.clone(),
            &token,
        )) {
            Ok(()) => {
                // Remove from local cache
                self.directories.blocking_write().remove(&dir_path);
                
                // Remove inode mapping
                let mut inode_map = self.inode_map.blocking_write();
                if let Some(ino) = inode_map.get_by_right(&dir_path).copied() {
                    inode_map.remove_by_left(&ino);
                }
                
                debug!(path = dir_path, "Directory removed successfully");
                reply.ok();
            }
            Err(err) => {
                error!(path = dir_path, error = %err, "Failed to delete directory");
                reply.error(self.map_error(&err));
            }
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: fuser::ReplyStatfs) {
        debug!("statfs request");
        
        let token = self.token.blocking_read().clone();
        
        // Query backend for real filesystem statistics
        match self.runtime.block_on(fscore::get_system_stats(
            self.settings.clone(),
            &token,
        )) {
            Ok(stats) => {
                debug!(
                    blocks_total = stats.total_blocks,
                    blocks_free = stats.free_blocks,
                    inodes_total = stats.total_inodes,
                    inodes_free = stats.free_inodes,
                    "Returning filesystem statistics from backend"
                );
                
                reply.statfs(
                    stats.total_blocks as u64,       // blocks
                    stats.free_blocks as u64,        // bfree (free blocks)
                    stats.free_blocks as u64,        // bavail (available blocks to non-root)
                    stats.total_inodes as u64,       // files (total inodes)
                    stats.free_inodes as u64,        // ffree (free inodes)
                    stats.block_size as u32,         // bsize (block size)
                    512,                             // namelen (max filename length)
                    stats.block_size as u32,         // frsize (fragment size)
                );
            }
            Err(err) => {
                error!(error = %err, "Failed to get system stats, using fallback");
                // Fallback to conservative defaults
                reply.statfs(
                    1048576,  // 4 GB (1M blocks × 4KB)
                    1000000,  // 4 GB free
                    1000000,  // 4 GB available
                    10000,    // 10K inodes
                    10000,    // 10K free inodes
                    4096,     // 4KB block size
                    512,      // max filename
                    4096,     // fragment size
                );
            }
        }
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        debug!(
            parent = parent.0,
            link_name = link_name.to_string_lossy().to_string(),
            target = target.display().to_string(),
            "Symlink not supported"
        );
        // Git probes symlink support; return explicit unsupported instead of default warning path.
        reply.error(Errno::from_i32(libc::ENOTSUP));
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        // We currently do not persist extended attributes.
        let exists = ino == fuser::INodeNo(1) || self.get_path_by_inode(ino).is_some();
        if !exists {
            reply.error(Errno::from_i32(ENOENT));
            return;
        }
        debug!(ino = ino.0, name = name.to_string_lossy().to_string(), "setxattr unsupported");
        reply.error(Errno::from_i32(libc::ENOTSUP));
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, _size: u32, reply: ReplyXattr) {
        // Report missing xattrs with ENODATA instead of ENOSYS to avoid noisy fallbacks.
        let exists = ino == fuser::INodeNo(1) || self.get_path_by_inode(ino).is_some();
        if !exists {
            reply.error(Errno::from_i32(ENOENT));
            return;
        }
        debug!(ino = ino.0, name = name.to_string_lossy().to_string(), "xattr not present");
        reply.error(Errno::from_i32(libc::ENODATA));
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let exists = ino == fuser::INodeNo(1) || self.get_path_by_inode(ino).is_some();
        if !exists {
            reply.error(Errno::from_i32(ENOENT));
            return;
        }

        // No xattrs are currently stored: return an empty xattr list.
        if size == 0 {
            reply.size(0);
        } else {
            reply.data(&[]);
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let exists = ino == fuser::INodeNo(1) || self.get_path_by_inode(ino).is_some();
        if !exists {
            reply.error(Errno::from_i32(ENOENT));
            return;
        }
        debug!(ino = ino.0, name = name.to_string_lossy().to_string(), "removexattr unsupported");
        reply.error(Errno::from_i32(libc::ENOTSUP));
    }

    fn access(&self, _req: &Request, ino: INodeNo, _mask: AccessFlags, reply: fuser::ReplyEmpty) {
        // Minimal access policy: if the inode exists, allow access.
        let exists = ino == fuser::INodeNo(1) || self.get_path_by_inode(ino).is_some();
        if exists {
            reply.ok();
        } else {
            reply.error(Errno::from_i32(ENOENT));
        }
    }

    fn ioctl(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: IoctlFlags,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: ReplyIoctl,
    ) {
        debug!(
            ino = ino.0,
            fh = fh.0,
            flags = %flags,
            cmd,
            in_len = in_data.len(),
            out_size,
            "ioctl unsupported"
        );
        // Not a tty / unsupported ioctl for this FS.
        reply.error(Errno::from_i32(libc::ENOTTY));
    }

}

impl DAFSfs {
    fn flush_pending_buffer(
        &self,
        fh: FileHandle,
        min_bytes_to_flush: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (remote_path, base_offset, mut pending) = {
            let mut state_lock = self.state.blocking_write();
            let entry = match state_lock.files.get_mut(&fh) {
                Some(e) => e,
                None => return Ok(()),
            };

            if !entry.dirty {
                return Ok(());
            }

            if entry.buffer.is_empty() {
                entry.dirty = false;
                return Ok(());
            }

            if entry.buffer.len() < min_bytes_to_flush {
                return Ok(());
            }

            (
                entry.remote_path.clone(),
                entry.base_offset,
                std::mem::take(&mut entry.buffer),
            )
        };

        let token = self.token.blocking_read().clone();
        let to_upload = std::mem::take(&mut pending);
        let uploaded = to_upload.len() as u64;

        debug!(
            fh = fh.0,
            path = &remote_path,
            base_offset,
            size = to_upload.len(),
            "Uploading buffered range"
        );

        if let Err(err) = self.runtime.block_on(fscore::write_file_range(
            self.settings.clone(),
            remote_path.clone(),
            base_offset,
            to_upload.clone(),
            &token,
        )) {
            let mut state_lock = self.state.blocking_write();
            if let Some(entry) = state_lock.files.get_mut(&fh) {
                if entry.buffer.is_empty() {
                    entry.buffer = to_upload;
                } else {
                    let mut merged = to_upload;
                    merged.extend_from_slice(&entry.buffer);
                    entry.buffer = merged;
                }
                entry.base_offset = base_offset;
                entry.dirty = true;
            }
            return Err(err);
        }

        let mut state_lock = self.state.blocking_write();
        if let Some(entry) = state_lock.files.get_mut(&fh) {
            entry.base_offset = base_offset.saturating_add(uploaded);
            entry.dirty = !entry.buffer.is_empty();
        }

        Ok(())
    }

    fn resize_remote_file(
        &self,
        path: &str,
        new_size: u64,
        token: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let meta = self.runtime.block_on(fscore::get_file_metadata(
            self.settings.clone(),
            path.to_string(),
            token,
        ))?;
        let current_size = meta.size.max(0) as u64;

        if new_size == current_size {
            return Ok(());
        }

        if new_size == 0 {
            return self.truncate_remote_file(path, token);
        }

        if new_size < current_size {
            // Keep prefix bytes [0, new_size), rebuild file, then write prefix back.
            let preserved = self.runtime.block_on(fscore::read_file_range(
                self.settings.clone(),
                path.to_string(),
                0,
                new_size,
                token,
            ))?;
            self.truncate_remote_file(path, token)?;
            if !preserved.is_empty() {
                self.runtime.block_on(fscore::write_file_range(
                    self.settings.clone(),
                    path.to_string(),
                    0,
                    preserved,
                    token,
                ))?;
            }
            return Ok(());
        }

        // Extend with zero-filled chunks.
        let mut remaining = new_size - current_size;
        let mut write_offset = current_size;
        const ZERO_CHUNK_SIZE: usize = 1024 * 1024;
        while remaining > 0 {
            let chunk_len = std::cmp::min(remaining, ZERO_CHUNK_SIZE as u64) as usize;
            self.runtime.block_on(fscore::write_file_range(
                self.settings.clone(),
                path.to_string(),
                write_offset,
                vec![0u8; chunk_len],
                token,
            ))?;
            write_offset += chunk_len as u64;
            remaining -= chunk_len as u64;
        }

        Ok(())
    }

    fn truncate_remote_file(
        &self,
        path: &str,
        token: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let delete_result = self.runtime.block_on(fscore::delete_file(
            self.settings.clone(),
            path.to_string(),
            token,
        ));

        if let Err(err) = delete_result {
            // If file is already absent, continue with create(0).
            let not_found = err
                .downcast_ref::<tonic::Status>()
                .is_some_and(|status| status.code() == tonic::Code::NotFound);
            if !not_found {
                return Err(err);
            }
        }

        self.runtime.block_on(fscore::create_file(
            self.settings.clone(),
            path.to_string(),
            token,
            0,
        ))?;

        // Best effort: mark empty file as ready.
        let _ = self.runtime.block_on(fscore::set_file_status_ready(
            self.settings.clone(),
            path.to_string(),
            token,
        ));

        Ok(())
    }

    fn map_error(&self, err: &Box<dyn std::error::Error + Send + Sync>) -> Errno {
        if let Some(status) = err.downcast_ref::<tonic::Status>() {
            match status.code() {
                tonic::Code::NotFound => Errno::from_i32(ENOENT),
                tonic::Code::AlreadyExists => Errno::from_i32(EEXIST),
                tonic::Code::FailedPrecondition => Errno::from_i32(ENOTDIR),
                tonic::Code::PermissionDenied => Errno::from_i32(libc::EACCES),
                _ => Errno::from_i32(EIO),
            }
        } else {
            Errno::from_i32(EIO)
        }
    }

    fn intitialize(&self) {
        self.inode_map.blocking_write().insert(1, "/".to_string());
        // Register user, ignoring error if already exists
        match self.runtime.block_on(fscore::register_user(
            self.settings.clone(),
            "pepe".to_string(),
            "pepe".to_string(),
        )) {
            Ok(_) => {},
            Err(e) if e.to_string().contains("already exists") => {
                debug!("User pepe already registered");
            }
            Err(e) => panic!("Failed to register user: {}", e),
        }
        let token = fscore::read_access_token(&self.settings.access_token_path).unwrap();
        *self.token.blocking_write() = Arc::new(token);
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

    fn file_attr(&self, ino: INodeNo, size: i64, created_at_ms: i64, kind: FileType, mode: u32) -> FileAttr {
        let timestamp = UNIX_EPOCH + Duration::from_millis(created_at_ms.max(0) as u64);
        FileAttr {
            ino,
            size: size.max(0) as u64,
            blocks: (size.max(0) as u64).div_ceil(512),
            atime: timestamp,
            mtime: timestamp,
            ctime: timestamp,
            crtime: timestamp,
            kind,
            perm: mode as u16,
            nlink: if kind == FileType::Directory { 2 } else { 1 },
            uid: self.current_uid(),
            gid: self.current_gid(),
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
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

    fn get_new_inode(&self, file_path: String) -> INodeNo {
        let ino = {
            let mut map = self.inode_map.blocking_write();
            match map.get_by_right(&file_path) {
                Some(ino) => *ino,
                None => {
                    let new_ino = self.next_inode.fetch_add(1, Ordering::SeqCst);
                    map.insert(new_ino, file_path.clone());
                    new_ino
                }
            }
        };
        fuser::INodeNo(ino)
    }

    fn get_inode_by_path(&self, file_path: &str) -> Option<INodeNo> {
        let map = self.inode_map.blocking_read();
        map.get_by_right(file_path).copied().map(INodeNo)
    }

    fn get_path_by_inode(&self, ino: INodeNo) -> Option<String> {
        let map = self.inode_map.blocking_read();
        map.get_by_left(&ino.0).cloned()
    }
}

// ─── Entry point ──────────────────────────────────────────────────────────────
fn main() {
    let settings = Arc::new(load_config());

    let _guard = common::init_tracing("fuse", &settings.log_level);
    debug!("CLI configuration loaded: {:?}", settings);
    debug!("DAFS CLI starting");
    debug!("Master node: {}", settings.master_endpoints.join(", "));
    let mut config = Config::default();
    config.mount_options = vec![MountOption::RW, MountOption::AutoUnmount];
    config.acl = fuser::SessionACL::All;
    let mut initial_dirs = HashSet::new();
    initial_dirs.insert("/".to_string());  // Root is always a directory
    
    let fs = DAFSfs {
        inode_map: RwLock::new(BiMap::new()),
        runtime: Arc::new(Runtime::new().unwrap()),
        next_inode: AtomicU64::new(2),
        token: RwLock::new(Arc::new(String::new())),
        ttl: Duration::from_secs(2),
        settings,
        state: Arc::new(RwLock::new(DAFSState {
            files: HashMap::new(),
        })),
        directories: RwLock::new(initial_dirs),
    };

    if let Err(err) = ensure_mountpoint(MOUNTPOINT) {
        panic!("Failed to create mountpoint '{}': {err}", MOUNTPOINT);
    }

    if is_likely_mounted(MOUNTPOINT) {
        debug!(
            path = MOUNTPOINT,
            "Detected existing mount; trying to unmount stale mount before start"
        );
        try_unmount(MOUNTPOINT);
    }

    let mountpoint_for_signal = MOUNTPOINT.to_string();
    if let Err(err) = ctrlc::set_handler(move || {
        debug!(
            path = mountpoint_for_signal,
            "Signal received, attempting unmount"
        );
        try_unmount(&mountpoint_for_signal);
        std::process::exit(130);
    }) {
        error!(error = %err, "Failed to install Ctrl+C handler");
    }

    fs.intitialize();
    mount2(fs, MOUNTPOINT, &config).expect("Failed to mount DAFS");
}
