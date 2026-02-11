# DAFS Improvements

This document lists concrete improvements for the distributed file system, organized by priority and area.

---

## 1. Metadata Persistence (Master)

The master holds all file and block metadata in memory (`RwLock<HashMap<...>>`). If the master process restarts, every file reference is lost while the blocks remain orphaned on datanodes.

**What to do:**
- Persist the `files` and `available_datanodes` maps to disk on every mutation (create, delete, rename).
- On startup, reload state from disk before accepting RPCs.
- Consider a write-ahead log (WAL) approach: append each operation to a log file, then periodically compact it into a snapshot. This avoids rewriting the entire state on every change.
- A simpler first step is serializing the `HashMap` to a JSON or bincode file after each write operation, and loading it on boot.

**Files involved:** `services/master/src/main.rs` (the `MasterState` struct and every RPC handler that mutates `files`).

---

## 2. Dead DataNode Detection

DataNodes send heartbeats every 3 seconds. The master stores `last_heartbeat` but never checks it. A crashed datanode stays in the registry forever, and the master will keep assigning blocks to it.

**What to do:**
- Add a background task on the master that runs periodically (e.g., every 10 seconds).
- Iterate over `available_datanodes` and compare `last_heartbeat` against the current time.
- If a datanode hasn't sent a heartbeat in a configurable threshold (e.g., 30 seconds), mark it as unavailable or remove it from the map.
- Stop assigning new blocks to unavailable datanodes.
- Log a warning when a datanode is removed so operators notice.

**Files involved:** `services/master/src/main.rs` (heartbeat handler and a new background task).

---

## 3. Data Integrity with Checksums

The `FileMetadata` struct has a `checksum` field hardcoded to `"TO_IMPLEMENT"`. There is no verification that a block written to a datanode is the same data that gets read back.

**What to do:**
- Compute a checksum (e.g., CRC32 or SHA-256) for each block on the client side before uploading.
- Send the checksum along with the block metadata in the `WriteBlock` RPC stream.
- Have the datanode verify the checksum after receiving the full block. If it doesn't match, return an error instead of acknowledging success.
- Store the per-block checksum in `BlockMetadata` on the master.
- On download, the client should compute the checksum of the received data and compare it against the expected value.
- Also consider a whole-file checksum stored in `FileMetadata`.

**Files involved:** `proto/dfs.proto` (add checksum fields), `client/cli/src/main.rs` (compute checksums), `services/datanode/src/main.rs` (verify checksums), `services/master/src/main.rs` (store checksums).

---

## 4. Client-Side Retry and Replica Failover

When a block download fails, the client does not attempt to read from an alternative replica. It also does not retry uploads. Since each block is replicated, the infrastructure for failover already exists but is unused.

**What to do:**
- `BlockMetadata.locations` already contains multiple datanode IDs. On download, if the first datanode fails or times out, try the next location in the list.
- Add a configurable retry count and delay for both uploads and downloads.
- For uploads, if a write to one datanode fails, retry or pick another available node (would require a new RPC to the master to get an alternative assignment).

**Files involved:** `client/cli/src/main.rs` (download_block and upload_block functions).

---

## 5. File Status Transitions

There is a `FileStatus` enum (`Uploading`, `Ready`, `Deleted`) but the transitions are not fully enforced:
- A file is set to `Ready` on creation before any blocks are actually uploaded.
- A client could read a file that is still being uploaded.
- There is no confirmation from the client that an upload completed.

**What to do:**
- Set the initial status to `Uploading` when `CreateFile` is called.
- Add a `ConfirmUpload` RPC (or similar) that the client calls after all blocks have been successfully written. Only then transition to `Ready`.
- Block `GetFileMetadata` or `ReadBlock` requests for files that are still in `Uploading` status, or return a clear status indicator so the client can decide.
- On timeout (e.g., if the client crashes mid-upload), have a background task clean up stale `Uploading` files.

**Files involved:** `proto/dfs.proto` (new RPC), `services/master/src/main.rs` (status management), `client/cli/src/main.rs` (call ConfirmUpload after successful upload).

---

## 6. Garbage Collection Reliability

Deletion is fire-and-forget: the master sends delete requests to datanodes asynchronously and does not track whether they succeed. If a datanode is down during deletion, the blocks are orphaned permanently.

**What to do:**
- Track pending deletions in a persistent queue (on disk).
- After sending a delete request, only remove it from the queue when the datanode confirms success.
- Periodically retry failed deletions.
- Consider adding a datanode-side "block report" mechanism: datanodes periodically tell the master which blocks they hold, and the master can identify orphaned blocks that should be deleted.

**Files involved:** `services/master/src/main.rs` (garbage collector background task).

---

## 7. Block Placement Strategy

The current placement is a simple modular round-robin: `datanodes[(block_idx + replica) % datanodes.len()]`. This does not account for:
- Disk space on each datanode.
- Network locality or rack awareness.
- Two replicas of the same block ending up on adjacent nodes (though the current formula does prevent same-node placement for different replicas of the same block).

**What to do:**
- Use the `free_space` and `used_space` fields from the heartbeat (currently hardcoded to `4` on the datanode side) with real values.
- Prefer placing blocks on datanodes with more free space.
- Ensure replicas of the same block are placed on different failure domains (if you ever run this across multiple machines/racks).

**Files involved:** `services/datanode/src/main.rs` (report real disk stats in heartbeat), `services/master/src/main.rs` (block assignment logic in `create_file`).

---

## 8. Re-Replication on DataNode Loss

If a datanode dies and is detected (see improvement #2), the blocks it held now have fewer replicas than the configured `replication_factor`. There is no mechanism to restore the desired replication level.

**What to do:**
- When a datanode is marked dead, iterate over all files and find blocks that had a replica on that node.
- For each under-replicated block, pick another live datanode and issue a block copy.
- This requires a new RPC or mechanism: either a datanode-to-datanode copy, or the master reading from one datanode and writing to another.
- A simpler approach: have the master instruct a healthy datanode that holds a copy to stream the block to a new target datanode.

**Files involved:** `proto/dfs.proto` (potentially a new `CopyBlock` or `ReplicateBlock` RPC), `services/master/src/main.rs`, `services/datanode/src/main.rs`.

---

## 9. Connection Management in the Client

The client creates a new gRPC connection to each datanode for every block transfer. For files with many blocks on the same datanode, this is wasteful.

**What to do:**
- Maintain a connection pool or cache: `HashMap<String, DataNodeServiceClient>` keyed by datanode address.
- Reuse connections across block transfers to the same datanode.
- Add connection timeout and cleanup for stale connections.

**Files involved:** `client/cli/src/main.rs` (upload and download functions).

---

## 10. Real Heartbeat Metrics

The datanode heartbeat sends hardcoded dummy values:

```rust
cpu_usage: 1.0,
free_space: 4,
used_space: 4,
```

**What to do:**
- Report actual disk usage of the `data_folder` (e.g., using `std::fs::metadata` or a system crate like `sysinfo`).
- Report actual CPU usage if it's relevant to placement decisions.
- The master can then use these metrics for smarter block placement and capacity alerting.

**Files involved:** `services/datanode/src/main.rs` (heartbeat sender).

---

## 11. Error Propagation in the Client

The client uses `Box<dyn Error>` broadly and prints errors with `eprintln!`. Several error paths silently continue or provide vague messages.

**What to do:**
- Define a custom error enum (e.g., `DafsError`) with variants for connection failures, block transfer failures, metadata errors, etc.
- Use `thiserror` or similar to implement `Display` and `From` conversions.
- Provide actionable error messages to the user (e.g., "Block abc123 failed to download from datanode 2 at 192.168.1.5:8080, retrying with datanode 3").
- Ensure partial failures are clearly reported (e.g., "3 of 10 blocks failed to upload").

**Files involved:** `client/cli/src/main.rs`.

---

## 12. Concurrent Upload Safety

Multiple clients can upload to the same file path simultaneously. The master does not lock a file path during upload, which could lead to corrupted metadata where blocks from different uploads are interleaved.

**What to do:**
- Acquire an exclusive lock (or use compare-and-swap) on the file path during `CreateFile`.
- Only release it when the upload is confirmed (ties into improvement #5) or times out.
- Return `ALREADY_EXISTS` or `RESOURCE_EXHAUSTED` if another upload is in progress for the same path.

**Files involved:** `services/master/src/main.rs` (CreateFile handler).

---

## 13. Directory Support

`ListDirectory` filters file paths by prefix, which simulates directories. But there is no concept of creating, deleting, or renaming directories. A file at `/a/b/c.txt` does not require `/a/b/` to exist first.

**What to do:**
- Decide whether to keep the flat-namespace approach (paths are just strings) or add real directory metadata.
- If flat: at minimum, validate path formatting and prevent paths like `//foo` or trailing slashes.
- If real directories: add `CreateDirectory`, `DeleteDirectory` RPCs and track directory entries in metadata.

**Files involved:** `proto/dfs.proto`, `services/master/src/main.rs`.

---

## 14. Graceful Shutdown

Neither the master nor the datanodes handle shutdown signals gracefully. A `SIGTERM` will kill the process mid-operation, potentially leaving a block half-written or a file in `Uploading` state.

**What to do:**
- Listen for `SIGTERM`/`SIGINT` using `tokio::signal`.
- On signal: stop accepting new RPCs, wait for in-flight operations to complete (with a timeout), flush any pending state to disk, then exit.
- On the datanode: ensure any in-progress `WriteBlock` is either completed or the partial file is cleaned up.

**Files involved:** `services/master/src/main.rs`, `services/datanode/src/main.rs`.

---

## 15. Security

There is no authentication or authorization. Any client that can reach the master can read, write, or delete any file. gRPC connections are unencrypted.

**What to do (incremental steps):**
1. **TLS**: Enable TLS on all gRPC connections (`tonic` supports this). This encrypts traffic and prevents eavesdropping.
2. **Token-based auth**: Add a shared secret or JWT token to gRPC metadata. The master validates it before processing requests.
3. **Per-file permissions**: Use the existing `owner` field with real user IDs. Add read/write permission checks based on the authenticated user.
4. **DataNode authentication**: DataNodes should authenticate to the master (and vice versa) to prevent rogue nodes from joining the cluster.

**Files involved:** All three components plus `proto/dfs.proto`.

---

## Summary Table

| # | Improvement | Severity | Complexity |
|---|-------------|----------|------------|
| 1 | Metadata persistence | Critical | Medium |
| 2 | Dead datanode detection | Critical | Low |
| 3 | Data integrity (checksums) | High | Medium |
| 4 | Client retry / failover | High | Medium |
| 5 | File status transitions | High | Low |
| 6 | Garbage collection reliability | Medium | Medium |
| 7 | Block placement strategy | Medium | Low |
| 8 | Re-replication | Medium | High |
| 9 | Client connection pooling | Low | Low |
| 10 | Real heartbeat metrics | Low | Low |
| 11 | Client error handling | Low | Low |
| 12 | Concurrent upload safety | Medium | Low |
| 13 | Directory support | Low | Medium |
| 14 | Graceful shutdown | Medium | Medium |
| 15 | Security (TLS, auth, authz) | High | High |
