//! Multi-process NetCDF (MPIO) reader client.
//!
//! This module provides an async, in-process client that farms out NetCDF decoding
//! work to a small pool of external worker processes (`beacon-arrow-netcdf-mpio`).
//! The main reason to do this is isolation and parallelism: NetCDF/HDF5 stacks can
//! be heavy and may not always be perfectly async-friendly; running reads in separate
//! processes gives us better fault isolation and lets us scale reads across CPU cores.
//!
//! **Top-level architecture**
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────────┐
//! │                       Calling code (Tokio)                    │
//! │                                                               │
//! │  read_schema(...) / read_file_as_batch(...)                    │
//! │            │                                                  │
//! │            ▼                                                  │
//! │      ┌───────────┐         mpsc (requests)                    │
//! │      │  MpioPool  │ ─────────────────────────────────────┐    │
//! │      └───────────┘                                      │    │
//! │            │                                             ▼    │
//! │            │                                  ┌────────────────┐
//! │            │                                  │ worker_dispatcher│
//! │            │                                  │  - FIFO queue    │
//! │            │                                  │  - idle workers  │
//! │            │                                  └───────┬────────┘
//! │            │                                          │
//! │            │                   per-worker mpsc (1 in-flight)  │
//! │            └──────────────────────────────────────────┬────────┘
//! │                                                      ▼
//! │   ┌───────────────────────┐   framed JSON over stdin/stdout   ┌───────────────────────┐
//! │   │  worker task (id = 0) │ ◀────────────────────────────────▶ │ beacon-arrow-netcdf-  │
//! │   │  - owns ChildProc      │                                   │ mpio (process)        │
//! │   │  - handle_request()    │                                   │ - reads NetCDF        │
//! │   └───────────────────────┘                                   │ - returns Arrow IPC   │
//! │               ▲                                                └───────────────────────┘
//!               ...
//! │   ┌───────────────────────┐
//! │   │  worker task (id = N) │
//! │   └───────────────────────┘
//! └───────────────────────────────────────────────────────────────┘
//! ```
//!
//! **Wire protocol (client <-> worker)**
//!
//! All control messages are length-prefixed JSON frames:
//! - `[u32 little-endian length]` followed by that many bytes of UTF-8 JSON.
//!
//! For `CommandReponse::BatchesStream { length, has_more: false, .. }`, the JSON frame
//! is immediately followed by exactly `length` bytes of Arrow IPC stream payload.
//! (Streaming multiple payload frames is not supported here; `has_more=true` is treated
//! as a protocol error.)
//!
//! **Concurrency model**
//!
//! - The dispatcher receives requests, queues them FIFO, and assigns them to idle workers.
//! - Each worker task has a `mpsc::channel(1)` so it processes at most one request at a time.
//! - If a worker task dies, the dispatcher respawns it; the in-flight request is retried once.
//!
//! **Operational knobs**
//!
//! - `BEACON_NETCDF_MPIO_WORKER`: override the worker executable path.
//! - `BEACON_NETCDF_MPIO_REQUEST_TIMEOUT_MS`: per-request timeout. If set, the caller will
//!   error if no response arrives within this duration.
//!
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use beacon_arrow_netcdf::mpio_utils::{CommandResponse, ReadCommand};
use beacon_config::CONFIG;
use beacon_object_storage::DatasetsStore;
use object_store::ObjectMeta;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::OnceCell;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::netcdf::object_resolver::NetCDFObjectResolver;
use crate::netcdf::source::options::get_mpio_opts;

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("Worker process failed with IO error: {0}")]
    IoError(std::io::Error),
    #[error("Worker process failed with protocol error: {0}")]
    SerdeError(serde_json::Error),
    #[error("Worker process returned an error for request {request_id}: {message}")]
    WorkerError { request_id: u32, message: String },
    #[error("Worker protocol violation: {0}")]
    ProtocolError(String),
    #[error("Worker produced an unexpected response")]
    UnexpectedResponse,
    #[error("Failed to translate NetCDF URL path: {0}")]
    UrlTranslationError(beacon_object_storage::error::StorageError),
    #[error("MPIO pool must be initialized inside a Tokio runtime")]
    NotInTokioRuntime,
    #[error("Arrow IPC error: {0}")]
    ArrowIpcError(arrow::error::ArrowError),
    #[error("MPIO request timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
}

impl From<arrow::error::ArrowError> for PoolError {
    fn from(value: arrow::error::ArrowError) -> Self {
        Self::ArrowIpcError(value)
    }
}
struct MpioReadRequest {
    pub command: ReadCommand,
    pub responder: oneshot::Sender<Result<MpioReadResponse, PoolError>>,
}

struct MpioReadResponse {
    pub response: CommandResponse,
    pub payload: Option<Vec<u8>>,
}

struct WorkerProc {
    #[allow(dead_code)]
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static MPIO_POOL: OnceCell<MpioPool> = OnceCell::const_new();

struct MpioPool {
    tx: mpsc::Sender<MpioReadRequest>,
}

impl MpioPool {
    /// Returns the global pool (initialized once).
    ///
    /// Must be called from within a Tokio runtime.
    pub async fn global() -> Result<&'static Self, PoolError> {
        MPIO_POOL
            .get_or_try_init(|| async {
                tokio::runtime::Handle::try_current().map_err(|_| PoolError::NotInTokioRuntime)?;

                let (tx, rx) = mpsc::channel(128);
                tokio::spawn(async move {
                    if let Err(e) = worker_dispatcher(rx).await {
                        tracing::error!(error = ?e, "mpio worker dispatcher exited");
                    }
                });

                Ok(MpioPool { tx })
            })
            .await
    }

    /// Sends a single request through the pool and awaits the worker response.
    ///
    /// This uses a oneshot channel per request to route the response back to the caller.
    ///
    /// If `BEACON_NETCDF_MPIO_REQUEST_TIMEOUT_MS` is set, the wait for the response is bounded.
    pub async fn send_request(&self, command: ReadCommand) -> Result<MpioReadResponse, PoolError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        let request = MpioReadRequest {
            command,
            responder: resp_tx,
        };

        self.tx.send(request).await.map_err(|e| {
            PoolError::IoError(std::io::Error::other(format!(
                "Failed to send request to MPIO pool: {}",
                e
            )))
        })?;

        if let Some(timeout) = mpio_request_timeout() {
            let timeout_ms = timeout.as_millis().min(u64::MAX as u128) as u64;
            match tokio::time::timeout(timeout, resp_rx).await {
                Ok(result) => result.map_err(|e| {
                    PoolError::IoError(std::io::Error::other(format!(
                        "Failed to receive response from MPIO pool: {}",
                        e
                    )))
                })?,
                Err(_) => Err(PoolError::Timeout { timeout_ms }),
            }
        } else {
            resp_rx.await.map_err(|e| {
                PoolError::IoError(std::io::Error::other(format!(
                    "Failed to receive response from MPIO pool: {}",
                    e
                )))
            })?
        }
    }
}

fn mpio_request_timeout() -> Option<Duration> {
    // Centralized via `beacon-config`.
    // Default is 0 (disabled) to avoid surprising behavior changes for large reads.
    let timeout_ms = CONFIG.netcdf_mpio_request_timeout_ms;
    if timeout_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(timeout_ms))
    }
}

fn next_request_id() -> u32 {
    // The worker protocol uses `u32` request ids. We generate them from a process-wide
    // counter and wrap to stay in range.
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    (id % (u32::MAX as u64)) as u32
}

fn worker_executable_path() -> PathBuf {
    if cfg!(test) {
        // In tests, prefer the just-built binary sitting next to the test harness.
        if cfg!(windows) {
            return std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("beacon-arrow-netcdf-mpio.exe");
        } else {
            return std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("beacon-arrow-netcdf-mpio");
        }
    }

    if let Some(path) = CONFIG.netcdf_mpio_worker.clone() {
        tracing::debug!(path = ?path, "using beacon-arrow-netcdf-mpio from config");
        return path;
    }

    // Check for `beacon-arrow-netcdf-mpio` in PATH.
    if let Ok(path) = which::which("beacon-arrow-netcdf-mpio") {
        tracing::debug!(path = ?path, "using beacon-arrow-netcdf-mpio from PATH");
        return path;
    }

    // As a last resort, check if it nexts to the current executable (ourselves).
    if let Ok(current_exe) = std::env::current_exe() {
        let candidate = current_exe
            .parent()
            .unwrap()
            .join("beacon-arrow-netcdf-mpio");
        if candidate.exists() {
            tracing::debug!(
                path = ?candidate,
                "using beacon-arrow-netcdf-mpio next to current executable"
            );
            return candidate;
        }
    }

    // Default to resolving from PATH.
    tracing::debug!("using beacon-arrow-netcdf-mpio from default PATH resolution");
    PathBuf::from("beacon-arrow-netcdf-mpio")
}

async fn worker_dispatcher(mut rx: mpsc::Receiver<MpioReadRequest>) -> Result<(), PoolError> {
    // Central scheduler:
    // - Receives requests on `rx`.
    // - Stores them FIFO in `queue`.
    // - Assigns work to idle workers in a roughly round-robin fashion by popping from
    //   `idle_workers`.
    let num_workers = get_mpio_opts().num_processes.max(1);
    tracing::info!(num_workers, "starting MPIO pool");
    let worker_path = worker_executable_path();
    tracing::info!(worker_path = ?worker_path, "using MPIO worker executable");

    // Workers notify the dispatcher when they become idle again.
    let (idle_tx, mut idle_rx) = mpsc::channel::<usize>(num_workers * 4);
    let mut worker_senders: Vec<mpsc::Sender<MpioReadRequest>> = Vec::with_capacity(num_workers);
    let mut idle_workers = std::collections::VecDeque::new();

    for worker_id in 0..num_workers {
        let tx = spawn_worker_channel(worker_id, worker_path.clone(), idle_tx.clone());
        worker_senders.push(tx);
        idle_workers.push_back(worker_id);
    }

    let mut queue = std::collections::VecDeque::new();

    loop {
        tracing::trace!("mpio dispatcher waiting for requests or idle workers");
        tokio::select! {
            Some(req) = rx.recv() => {
                tracing::trace!(command = ?req.command, "mpio request received and pushed to command buffer.");
                queue.push_back(req);
            }

            Some(worker_id) = idle_rx.recv() => {
                tracing::trace!(worker_id, "mpio worker became idle");
                idle_workers.push_back(worker_id);
            }
        }
        tracing::trace!(
            queue_len = queue.len(),
            idle_workers = idle_workers.len(),
            "mpio dispatcher processing loop"
        );

        while !queue.is_empty() && !idle_workers.is_empty() {
            let req = queue.pop_front().unwrap();
            let worker_id = idle_workers.pop_front().unwrap();
            tracing::trace!(worker_id, command = ?req.command, "dispatching mpio request");
            let sender = worker_senders
                .get(worker_id)
                .ok_or_else(|| PoolError::ProtocolError("Worker id out of range".to_string()))?
                .clone();

            // If a worker task died (channel closed), respawn it and retry once.
            match sender.send(req).await {
                Ok(()) => {
                    tracing::trace!(worker_id, "mpio request sent to worker");
                }
                Err(e) => {
                    // `SendError` carries the original request back.
                    tracing::warn!(
                        worker_id,
                        "mpio worker channel closed; respawning with error: {}",
                        e
                    );
                    let req = e.0;

                    let new_tx =
                        spawn_worker_channel(worker_id, worker_path.clone(), idle_tx.clone());
                    if let Some(slot) = worker_senders.get_mut(worker_id) {
                        *slot = new_tx.clone();
                    }

                    match new_tx.send(req).await {
                        Ok(()) => {
                            // Worker is now busy; it will re-advertise idleness via `idle_tx`.
                        }
                        Err(e2) => {
                            let req = e2.0;
                            tracing::error!(
                                worker_id,
                                "failed to send request to respawned worker"
                            );
                            let _ =
                                req.responder
                                    .send(Err(PoolError::IoError(std::io::Error::other(
                                        "MPIO worker unavailable",
                                    ))));
                            // The respawned worker task exists and is idle; re-add it.
                            idle_workers.push_back(worker_id);
                        }
                    }
                }
            }
        }
        tracing::trace!("mpio dispatcher loop iteration complete");
    }
}

fn spawn_worker_channel(
    worker_id: usize,
    worker_path: PathBuf,
    idle_tx: mpsc::Sender<usize>,
) -> mpsc::Sender<MpioReadRequest> {
    // Capacity=1 enforces at most one in-flight request per worker.
    let (tx, rx_worker) = mpsc::channel::<MpioReadRequest>(1);
    spawn_worker_task(worker_id, worker_path, rx_worker, idle_tx);
    tx
}

fn spawn_worker_task(
    worker_id: usize,
    worker_path: PathBuf,
    mut rx: mpsc::Receiver<MpioReadRequest>,
    idle_tx: mpsc::Sender<usize>,
) {
    // One Tokio task per worker.
    // Owns the child process and its stdin/stdout handles.
    tokio::spawn(async move {
        let mut proc = match spawn_worker(&worker_path).await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(worker_id, error = ?e, "failed to spawn mpio worker");
                return;
            }
        };

        while let Some(req) = rx.recv().await {
            tracing::trace!(worker_id, command = ?req.command, "mpio worker handling request");
            let result = handle_request(&mut proc, req.command).await;

            // If the worker process crashed or the protocol stream is corrupted, refresh
            // the process so subsequent requests don't fail in a loop.
            if matches!(
                &result,
                Err(PoolError::IoError(_))
                    | Err(PoolError::SerdeError(_))
                    | Err(PoolError::ProtocolError(_))
            ) {
                tracing::warn!(
                    worker_id,
                    error = ?result.as_ref().err(),
                    "mpio worker unhealthy; respawning process"
                );
                match spawn_worker(&worker_path).await {
                    Ok(new_proc) => proc = new_proc,
                    Err(e) => {
                        tracing::error!(worker_id, error = ?e, "failed to respawn mpio worker process");
                        // Keep the original error for this request; future sends will fail and
                        // the dispatcher will respawn the worker task.
                    }
                }
            }

            tracing::trace!(worker_id, "mpio worker completed request");
            let _ = req.responder.send(result);
            let _ = idle_tx.send(worker_id).await;
        }
    });
}

async fn spawn_worker(worker_path: &PathBuf) -> Result<WorkerProc, PoolError> {
    tracing::debug!(worker_path = ?worker_path, "spawning mpio worker process");
    let mut child = Command::new(worker_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(PoolError::IoError)?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::other("no stdin"))
        .map_err(PoolError::IoError)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("no stdout"))
        .map_err(PoolError::IoError)?;

    Ok(WorkerProc {
        child,
        stdin: BufWriter::new(stdin),
        // Use a larger buffer to reduce syscalls when reading Arrow IPC payloads.
        stdout: BufReader::with_capacity(2 * 1024 * 1024, stdout),
    })
}

async fn write_framed_json<W: AsyncWrite + Unpin, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), PoolError> {
    // Frame format: [u32 little-endian length] + JSON bytes.
    let json = serde_json::to_vec(value).map_err(PoolError::SerdeError)?;
    let len = (json.len() as u32).to_le_bytes();
    writer.write_all(&len).await.map_err(PoolError::IoError)?;
    writer.write_all(&json).await.map_err(PoolError::IoError)?;
    writer.flush().await.map_err(PoolError::IoError)?;
    tracing::trace!(len = json.len(), "wrote framed json");
    Ok(())
}

async fn read_framed_json<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
) -> Result<T, PoolError> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(PoolError::IoError)?;
    tracing::trace!(len = u32::from_le_bytes(len_buf), "read framed json length");
    let json_len = u32::from_le_bytes(len_buf) as usize;
    if json_len == 0 {
        return Err(PoolError::ProtocolError(
            "zero-length json frame".to_string(),
        ));
    }
    let mut json_buf = vec![0u8; json_len];
    reader
        .read_exact(&mut json_buf)
        .await
        .map_err(PoolError::IoError)?;
    serde_json::from_slice(&json_buf).map_err(PoolError::SerdeError)
}

async fn handle_request(
    proc: &mut WorkerProc,
    command: ReadCommand,
) -> Result<MpioReadResponse, PoolError> {
    write_framed_json(&mut proc.stdin, &command).await?;
    tracing::trace!(command = ?command, "sent command to mpio worker");
    let response: CommandResponse = read_framed_json(&mut proc.stdout).await?;
    tracing::trace!(response = ?response, "received response from mpio worker");
    match &response {
        CommandResponse::Error {
            request_id,
            message,
        } => Err(PoolError::WorkerError {
            request_id: *request_id,
            message: message.clone(),
        }),
        CommandResponse::ArrowSchema { .. } => Ok(MpioReadResponse {
            response,
            payload: None,
        }),
        CommandResponse::BatchesStream {
            length, has_more, ..
        } => {
            if *has_more {
                return Err(PoolError::ProtocolError(
                    "has_more=true is not supported".to_string(),
                ));
            }
            // Immediately after the JSON response frame, the worker writes the raw
            // Arrow IPC stream bytes.
            let mut payload = vec![0u8; *length];
            proc.stdout
                .read_exact(&mut payload)
                .await
                .map_err(PoolError::IoError)?;
            Ok(MpioReadResponse {
                response,
                payload: Some(payload),
            })
        }
    }
}

pub async fn read_schema(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
) -> Result<SchemaRef, PoolError> {
    // Schema reads are small; we only return the JSON response.
    let pool = MpioPool::global().await?;
    let request_id = next_request_id();
    let path = datasets_object_store
        .translate_netcdf_url_path(&object.location)
        .map_err(|e| PoolError::UrlTranslationError(e))?;

    let resp = pool
        .send_request(ReadCommand::ReadArrowSchema { request_id, path })
        .await?;
    match resp.response {
        CommandResponse::ArrowSchema { schema, .. } => Ok(Arc::new(schema)),
        _ => Err(PoolError::UnexpectedResponse),
    }
}

pub async fn read_file_as_batch(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
    projection: Option<Vec<usize>>,
    chunk_size: usize,
    stream_size: usize,
) -> Result<RecordBatch, PoolError> {
    // File reads return an Arrow IPC stream containing at least one RecordBatch.
    let pool = MpioPool::global().await?;
    let request_id = next_request_id();
    let path = datasets_object_store
        .translate_netcdf_url_path(&object.location)
        .map_err(|e| PoolError::UrlTranslationError(e))?;

    let resp = pool
        .send_request(ReadCommand::ReadFile {
            request_id,
            path,
            projection,
            chunk_size,
            stream_size,
        })
        .await?;

    let payload = resp
        .payload
        .ok_or_else(|| PoolError::ProtocolError("missing IPC payload".to_string()))?;

    let cursor = std::io::Cursor::new(payload);
    let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)?;
    let maybe_batch = reader
        .next()
        .ok_or_else(|| PoolError::ProtocolError("empty IPC stream".to_string()))?;
    let batch = maybe_batch?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_object_storage::get_datasets_object_store;
    use tokio::io::duplex;

    #[tokio::test]
    async fn framed_json_round_trip() {
        let (mut client, mut server) = duplex(1024 * 1024);

        let send = ReadCommand::ReadArrowSchema {
            request_id: 42,
            path: "x.nc".to_string(),
        };

        let server_task = tokio::spawn(async move {
            let got: ReadCommand = read_framed_json(&mut server).await.unwrap();
            match got {
                ReadCommand::ReadArrowSchema { request_id, path } => {
                    assert_eq!(request_id, 42);
                    assert_eq!(path, "x.nc");
                }
                _ => panic!("unexpected command"),
            }

            let response = CommandResponse::BatchesStream {
                request_id: 42,
                length: 3,
                has_more: false,
            };
            write_framed_json(&mut server, &response).await.unwrap();
            server.write_all(&[1u8, 2u8, 3u8]).await.unwrap();
            server.flush().await.unwrap();
        });

        write_framed_json(&mut client, &send).await.unwrap();
        let resp: CommandResponse = read_framed_json(&mut client).await.unwrap();
        match resp {
            CommandResponse::BatchesStream {
                request_id,
                length,
                has_more,
            } => {
                assert_eq!(request_id, 42);
                assert_eq!(length, 3);
                assert!(!has_more);
            }
            _ => panic!("unexpected response"),
        }

        let mut payload = vec![0u8; 3];
        client.read_exact(&mut payload).await.unwrap();
        assert_eq!(payload, vec![1u8, 2u8, 3u8]);

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn e2e_read_schema_and_first_batch_with_worker_and_test_file() {
        let exe = std::env::current_exe().unwrap();
        let bin = exe
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("beacon-arrow-netcdf-mpio");

        assert!(bin.exists(), "MPIO worker executable not found");

        // Requires the `beacon-arrow-netcdf-mpio` executable.
        // Provide a path via `BEACON_NETCDF_MPIO_WORKER`.
        let datasets_object_store = get_datasets_object_store().await;

        let object = ObjectMeta {
            location: object_store::path::Path::from("test-files/gridded-example.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };

        let schema = read_schema(datasets_object_store.clone(), object.clone())
            .await
            .unwrap();
        assert!(!schema.fields().is_empty());

        // Read the first RecordBatch through the worker and sanity-check the result.
        // Keep sizes small to avoid making this test slow.
        let batch = read_file_as_batch(
            datasets_object_store.clone(),
            object,
            Some(vec![0]),
            1024,
            1024,
        )
        .await
        .unwrap();

        assert!(batch.num_columns() > 0);
        assert!(batch.num_rows() > 0);
        assert_eq!(batch.schema().fields().len(), 1);
    }
}
