use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use beacon_arrow_netcdf::mpio_utils::{CommandReponse, ReadCommand};
use object_store::ObjectMeta;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::OnceCell;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::netcdf::object_resolver::NetCDFObjectResolver;
use crate::netcdf::source_::options::get_mpio_opts;

use std::iter::Iterator as _;

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
    #[error("MPIO pool must be initialized inside a Tokio runtime")]
    NotInTokioRuntime,
    #[error("Arrow IPC error: {0}")]
    ArrowIpcError(arrow::error::ArrowError),
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
    pub response: CommandReponse,
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

        resp_rx.await.map_err(|e| {
            PoolError::IoError(std::io::Error::other(format!(
                "Failed to receive response from MPIO pool: {}",
                e
            )))
        })?
    }
}

fn next_request_id() -> u32 {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    (id % (u32::MAX as u64)) as u32
}

fn worker_executable_path() -> PathBuf {
    if cfg!(test) {
        // Check if windows or unix style executable
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

    if let Ok(path) = std::env::var("BEACON_NETCDF_MPIO_WORKER") {
        return PathBuf::from(path);
    }

    // Default to resolving from PATH.
    PathBuf::from("beacon-arrow-netcdf-mpio")
}

async fn worker_dispatcher(mut rx: mpsc::Receiver<MpioReadRequest>) -> Result<(), PoolError> {
    let num_workers = get_mpio_opts().num_processes.max(1);
    println!("Starting MPIO pool with {} workers", num_workers);
    let worker_path = worker_executable_path();
    println!("Using MPIO worker executable at {:?}", worker_path);

    // Workers notify the dispatcher when they become idle again.
    let (idle_tx, mut idle_rx) = mpsc::channel::<usize>(num_workers * 4);
    let mut worker_senders: Vec<mpsc::Sender<MpioReadRequest>> = Vec::with_capacity(num_workers);
    let mut idle_workers = std::collections::VecDeque::new();

    for worker_id in 0..num_workers {
        let (tx, rx_worker) = mpsc::channel::<MpioReadRequest>(1);
        spawn_worker_task(worker_id, worker_path.clone(), rx_worker, idle_tx.clone());
        worker_senders.push(tx);
        idle_workers.push_back(worker_id);
    }

    let mut queue = std::collections::VecDeque::new();

    loop {
        println!(
            "MPIO dispatcher loop: queue size {}, idle workers {}",
            queue.len(),
            idle_workers.len()
        );
        tokio::select! {
            Some(req) = rx.recv() => {
                println!("Received request from client: {:?}", req.command);
                queue.push_back(req);
            }

            Some(worker_id) = idle_rx.recv() => {
                idle_workers.push_back(worker_id);
            }
        }

        while let (Some(req), Some(worker_id)) = (queue.pop_front(), idle_workers.pop_front()) {
            println!(
                "Dispatching request {:?} to worker {}",
                req.command, worker_id
            );
            let sender = worker_senders
                .get(worker_id)
                .ok_or_else(|| PoolError::ProtocolError("Worker id out of range".to_string()))?
                .clone();

            if sender.send(req).await.is_err() {
                println!("Worker {} channel closed", worker_id);
                // Worker task died; report error back to caller.
                // (We could respawn here, but the simplest safe behavior is to fail fast.)
                // Re-insert the worker id as idle so future work doesn't stall.
                idle_workers.push_back(worker_id);
            }
        }
    }
}

fn spawn_worker_task(
    worker_id: usize,
    worker_path: PathBuf,
    mut rx: mpsc::Receiver<MpioReadRequest>,
    idle_tx: mpsc::Sender<usize>,
) {
    tokio::spawn(async move {
        let mut proc = match spawn_worker(&worker_path).await {
            Ok(p) => p,
            Err(e) => {
                println!("Failed to spawn MPIO worker: {}", e);
                tracing::error!(worker_id, error = ?e, "failed to spawn mpio worker");
                return;
            }
        };

        while let Some(req) = rx.recv().await {
            println!("Worker {} processing request {:?}", worker_id, req.command);
            let result = handle_request(&mut proc, req.command).await;
            println!("Worker {} completed request", worker_id);
            let _ = req.responder.send(result);
            let _ = idle_tx.send(worker_id).await;
        }
    });
}

async fn spawn_worker(worker_path: &PathBuf) -> Result<WorkerProc, PoolError> {
    println!("Spawning MPIO worker process at {:?}", worker_path);
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
        stdout: BufReader::with_capacity(2 * 1024 * 1024, stdout),
    })
}

async fn write_framed_json<W: AsyncWrite + Unpin, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), PoolError> {
    let json = serde_json::to_vec(value).map_err(PoolError::SerdeError)?;
    let len = (json.len() as u32).to_le_bytes();
    writer.write_all(&len).await.map_err(PoolError::IoError)?;
    writer.write_all(&json).await.map_err(PoolError::IoError)?;
    writer.flush().await.map_err(PoolError::IoError)?;
    println!("Wrote frame of length {}", json.len());
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
    println!("Read frame length: {}", u32::from_le_bytes(len_buf));
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
    println!("Sent command to worker: {:?}", command);
    panic!("test");
    let response: CommandReponse = read_framed_json(&mut proc.stdout).await?;
    println!("Received response from worker: {:?}", response);
    match &response {
        CommandReponse::Error {
            request_id,
            message,
        } => Err(PoolError::WorkerError {
            request_id: *request_id,
            message: message.clone(),
        }),
        CommandReponse::ArrowSchema { .. } => Ok(MpioReadResponse {
            response,
            payload: None,
        }),
        CommandReponse::BatchesStream {
            length, has_more, ..
        } => {
            if *has_more {
                return Err(PoolError::ProtocolError(
                    "has_more=true is not supported".to_string(),
                ));
            }
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
    resolver: Arc<NetCDFObjectResolver>,
    object: ObjectMeta,
) -> Result<SchemaRef, PoolError> {
    let pool = MpioPool::global().await?;
    let request_id = next_request_id();
    let path = resolver
        .resolve_object_meta(object)
        .to_string_lossy()
        .to_string();

    let resp = pool
        .send_request(ReadCommand::ReadArrowSchema { request_id, path })
        .await?;

    match resp.response {
        CommandReponse::ArrowSchema { schema, .. } => Ok(Arc::new(schema)),
        _ => Err(PoolError::UnexpectedResponse),
    }
}

pub async fn read_file_as_batch(
    resolver: Arc<NetCDFObjectResolver>,
    object: ObjectMeta,
    projection: Option<Vec<usize>>,
    chunk_size: usize,
    stream_size: usize,
) -> Result<RecordBatch, PoolError> {
    let pool = MpioPool::global().await?;
    let request_id = next_request_id();
    let path = resolver
        .resolve_object_meta(object)
        .to_string_lossy()
        .to_string();

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

            let response = CommandReponse::BatchesStream {
                request_id: 42,
                length: 3,
                has_more: false,
            };
            write_framed_json(&mut server, &response).await.unwrap();
            server.write_all(&[1u8, 2u8, 3u8]).await.unwrap();
            server.flush().await.unwrap();
        });

        write_framed_json(&mut client, &send).await.unwrap();
        let resp: CommandReponse = read_framed_json(&mut client).await.unwrap();
        match resp {
            CommandReponse::BatchesStream {
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
    async fn e2e_read_schema_with_worker_and_test_file() {
        let exe = std::env::current_exe().unwrap();
        let bin = exe
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("beacon-arrow-netcdf-mpio.exe");

        assert!(bin.exists(), "MPIO worker executable not found");

        // Requires the `beacon-arrow-netcdf-mpio` executable.
        // Provide a path via `BEACON_NETCDF_MPIO_WORKER`.
        let resolver = Arc::new(NetCDFObjectResolver::new(
            env!("CARGO_MANIFEST_DIR").to_string(),
            None,
            None,
        ));

        let object = ObjectMeta {
            location: object_store::path::Path::from("test-files/gridded-example.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };

        let schema = read_schema(resolver, object).await.unwrap();

        assert!(!schema.fields().is_empty());
    }
}
