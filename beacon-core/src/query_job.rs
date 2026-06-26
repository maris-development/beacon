//! Async query jobs: submit a query, then poll for incremental results.
//!
//! The synchronous streaming endpoint (`/api/query`) commits `200 OK` before the
//! first batch, so a mid-stream execution error can only truncate the response —
//! it can never surface as a clean error. Query jobs decouple execution from
//! delivery: a job runs in the background, and the client polls for status and
//! results. A query that fails mid-execution becomes an observable `Failed`
//! state instead of a silently truncated stream.
//!
//! Two job kinds:
//! - [`JobKind::Streamable`] — Arrow IPC / the default in-memory result. Batches
//!   are delivered incrementally as they are produced, via a
//!   [`SpillableBatchBuffer`].
//! - [`JobKind::File`] — any explicit file `output` (Parquet, NetCDF, …). The
//!   full file is materialized first, then downloaded.
//!
//! Buffered batches are bounded by a process-wide [`BufferBudget`] counted in
//! bytes across all jobs; once the budget is exhausted, further batches spill to
//! a per-job temp file rather than growing memory. Delivery is drain-on-read
//! (at-most-once): a poll response that never reaches the client loses those rows.

use std::collections::VecDeque;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use tempfile::NamedTempFile;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use utoipa::ToSchema;

use crate::query_result::QueryOutputFile;

/// Which delivery model a job uses, decided at submit time from the query's
/// `output` format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    /// Incremental Arrow batch delivery via the stream poll endpoint.
    Streamable,
    /// A fully-materialized file, downloaded once complete.
    File,
}

/// Terminal-or-running state of a job.
#[derive(Debug, Clone)]
pub enum QueryJobState {
    /// Still executing.
    Running,
    /// Finished successfully (file ready, or stream fully produced).
    Succeeded,
    /// Execution failed; carries the error message.
    Failed { error: String },
    /// Cancelled by the client or the idle sweeper.
    Cancelled,
}

impl QueryJobState {
    fn is_terminal(&self) -> bool {
        !matches!(self, QueryJobState::Running)
    }
}

/// Process-wide budget for in-memory buffered batch bytes, shared by every
/// streamable job's buffer.
#[derive(Debug)]
pub struct BufferBudget {
    used: AtomicU64,
    limit: u64,
}

impl BufferBudget {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicU64::new(0),
            limit,
        })
    }

    /// Try to reserve `bytes` of the in-memory budget. Returns `true` on success.
    fn try_reserve(&self, bytes: u64) -> bool {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            if current.saturating_add(bytes) > self.limit {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                current + bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Current in-memory bytes reserved across all jobs (for tests/metrics).
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }
}

/// A buffered batch, either resident in memory or spilled to the job's temp file.
enum Slot {
    InMemory { batch: RecordBatch, bytes: u64 },
    Spilled { offset: u64, len: u64 },
}

enum Terminal {
    Completed,
    Failed(String),
}

struct BufferInner {
    queue: VecDeque<Slot>,
    spill_bytes: u64,
    terminal: Option<Terminal>,
}

/// A FIFO buffer of Arrow batches that keeps recent batches in memory (bounded by
/// the shared [`BufferBudget`]) and spills the overflow to a temp file. Producers
/// call [`push`](Self::push) / [`finish`](Self::finish); the consumer calls
/// [`drain`](Self::drain).
pub struct SpillableBatchBuffer {
    schema: SchemaRef,
    budget: Arc<BufferBudget>,
    tmp_dir: PathBuf,
    max_spill_bytes: u64,
    inner: parking_lot::Mutex<BufferInner>,
    /// Lazily-created spill file; the `NamedTempFile` deletes on drop.
    spill: parking_lot::Mutex<Option<NamedTempFile>>,
    notify: Notify,
}

/// What a [`SpillableBatchBuffer::drain`] / poll yielded.
pub enum DrainOutcome {
    /// One or more batches were delivered (more may still follow).
    Batches(Vec<RecordBatch>),
    /// The buffer is empty and the producer finished successfully.
    Completed,
    /// The buffer is empty and the producer failed.
    Failed(String),
    /// Nothing became available within the wait; the job is still running.
    Pending,
}

impl SpillableBatchBuffer {
    pub fn new(
        schema: SchemaRef,
        budget: Arc<BufferBudget>,
        tmp_dir: PathBuf,
        max_spill_bytes: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            schema,
            budget,
            tmp_dir,
            max_spill_bytes,
            inner: parking_lot::Mutex::new(BufferInner {
                queue: VecDeque::new(),
                spill_bytes: 0,
                terminal: None,
            }),
            spill: parking_lot::Mutex::new(None),
            notify: Notify::new(),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Append a produced batch. Keeps it in memory if the global budget allows,
    /// otherwise spills it to disk. Returns `Err` if the per-job spill cap would
    /// be exceeded — the caller should then `finish(Failed(..))`.
    pub fn push(&self, batch: RecordBatch) -> Result<(), String> {
        let bytes = batch.get_array_memory_size() as u64;
        if self.budget.try_reserve(bytes) {
            self.inner
                .lock()
                .queue
                .push_back(Slot::InMemory { batch, bytes });
            self.notify.notify_one();
            return Ok(());
        }

        // Budget exhausted: spill the batch to the job's temp file.
        let blob = serialize_batch(&self.schema, &batch).map_err(|e| e.to_string())?;
        let len = blob.len() as u64;

        let offset = {
            let mut inner = self.inner.lock();
            if self.max_spill_bytes != 0
                && inner.spill_bytes.saturating_add(len) > self.max_spill_bytes
            {
                return Err(format!(
                    "query job spill limit exceeded ({} bytes)",
                    self.max_spill_bytes
                ));
            }
            let offset = inner.spill_bytes;
            inner.spill_bytes += len;
            offset
        };

        self.write_spill(offset, &blob).map_err(|e| e.to_string())?;
        self.inner
            .lock()
            .queue
            .push_back(Slot::Spilled { offset, len });
        self.notify.notify_one();
        Ok(())
    }

    /// Mark the producer as finished. Wakes any pending poll.
    pub fn finish(&self, success: Result<(), String>) {
        let terminal = match success {
            Ok(()) => Terminal::Completed,
            Err(error) => Terminal::Failed(error),
        };
        self.inner.lock().terminal = Some(terminal);
        self.notify.notify_one();
    }

    /// Drain up to `max` batches in FIFO order, waiting up to `wait` for the first
    /// one. Releases in-memory budget for delivered batches and reads spilled ones
    /// back from disk.
    pub async fn drain(&self, max: usize, wait: std::time::Duration) -> DrainOutcome {
        loop {
            // Register for notification *before* checking, so a push between the
            // check and the await is not lost.
            let notified = self.notify.notified();

            let popped = {
                let mut inner = self.inner.lock();
                if inner.queue.is_empty() {
                    match &inner.terminal {
                        Some(Terminal::Completed) => return DrainOutcome::Completed,
                        Some(Terminal::Failed(e)) => return DrainOutcome::Failed(e.clone()),
                        None => None,
                    }
                } else {
                    let take = max.min(inner.queue.len());
                    Some(inner.queue.drain(..take).collect::<Vec<_>>())
                }
            };

            if let Some(slots) = popped {
                return DrainOutcome::Batches(self.materialize(slots));
            }

            // Queue empty and not terminal: wait for a push/finish or time out.
            match tokio::time::timeout(wait, notified).await {
                Ok(()) => continue,
                Err(_) => return DrainOutcome::Pending,
            }
        }
    }

    /// Decode popped slots into batches, releasing budget for in-memory ones.
    fn materialize(&self, slots: Vec<Slot>) -> Vec<RecordBatch> {
        let mut out = Vec::with_capacity(slots.len());
        for slot in slots {
            match slot {
                Slot::InMemory { batch, bytes } => {
                    self.budget.release(bytes);
                    out.push(batch);
                }
                Slot::Spilled { offset, len } => match self.read_spill(offset, len) {
                    Ok(batch) => out.push(batch),
                    Err(e) => {
                        tracing::error!("failed to read spilled query-job batch: {e}");
                    }
                },
            }
        }
        out
    }

    fn write_spill(&self, offset: u64, blob: &[u8]) -> std::io::Result<()> {
        let mut guard = self.spill.lock();
        if guard.is_none() {
            *guard = Some(
                tempfile::Builder::new()
                    .prefix("beacon_qjob_spill_")
                    .suffix(".arrows")
                    .tempfile_in(&self.tmp_dir)?,
            );
        }
        let file = guard.as_mut().expect("spill file present").as_file_mut();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(blob)?;
        file.flush()?;
        Ok(())
    }

    fn read_spill(&self, offset: u64, len: u64) -> std::io::Result<RecordBatch> {
        let path = {
            let guard = self.spill.lock();
            guard
                .as_ref()
                .map(|f| f.path().to_path_buf())
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotFound, "spill file missing")
                })?
        };
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf)?;
        deserialize_batch(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

/// Drop releases any still-buffered in-memory budget so a cancelled/evicted job
/// does not leak the global allowance.
impl Drop for SpillableBatchBuffer {
    fn drop(&mut self) {
        let inner = self.inner.get_mut();
        let mut to_release = 0u64;
        for slot in inner.queue.drain(..) {
            if let Slot::InMemory { bytes, .. } = slot {
                to_release += bytes;
            }
        }
        if to_release > 0 {
            self.budget.release(to_release);
        }
    }
}

/// Serialize a single batch as a self-contained Arrow IPC stream (schema + batch).
fn serialize_batch(
    schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Decode the first batch from a self-contained Arrow IPC stream blob.
fn deserialize_batch(bytes: &[u8]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    reader.next().transpose()?.ok_or_else(|| {
        arrow::error::ArrowError::IpcError("spilled batch blob contained no batch".to_string())
    })
}

/// Metadata needed to stream a File-kind job's result back to the client.
#[derive(Debug, Clone)]
pub struct FileResultMeta {
    pub path: PathBuf,
    pub content_type: &'static str,
    pub file_ext: &'static str,
}

/// A registered job: its kind, state, lifecycle timestamps, and either a stream
/// buffer (streamable) or a materialized output file (file).
pub struct QueryJob {
    pub kind: JobKind,
    pub state: QueryJobState,
    /// When the job reached a terminal state (drives TTL eviction).
    pub finished_at: Option<Instant>,
    /// Last time a stream poll touched this job (drives idle-abort for streamable jobs).
    pub last_poll_at: Instant,
    /// Background execution task handle, for cancellation. Cleared on terminal.
    pub handle: Option<JoinHandle<()>>,
    /// Streamable jobs only: the incremental batch buffer.
    pub buffer: Option<Arc<SpillableBatchBuffer>>,
    /// File jobs only: the materialized result (owns the `NamedTempFile`).
    pub output: Option<QueryOutputFile>,
}

impl QueryJob {
    pub fn new_running(kind: JobKind, buffer: Option<Arc<SpillableBatchBuffer>>) -> Self {
        Self {
            kind,
            state: QueryJobState::Running,
            finished_at: None,
            last_poll_at: Instant::now(),
            handle: None,
            buffer,
            output: None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

/// Read-only view of a job for the status/result handlers, captured without
/// holding the registry lock during async IO.
#[derive(Debug, Clone)]
pub struct QueryJobSnapshot {
    pub kind: JobKind,
    pub state: QueryJobState,
    /// Present for succeeded File jobs.
    pub file: Option<FileResultMeta>,
}

/// Result of a stream poll, mapped by the API layer to an HTTP response.
pub enum PollOutcome {
    /// Batches drained this round (more may follow). Carries the schema so the
    /// API can serialize a self-contained Arrow IPC stream.
    Batches {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
    /// Still running, no batch became ready within the wait.
    Pending,
    /// Stream fully delivered and the query succeeded.
    Completed,
    /// The query failed; carries the error message.
    Failed(String),
    /// The job is a File job — use the status/result endpoints instead.
    NotStreamable,
    /// The job was cancelled.
    Cancelled,
    /// No job with that id.
    NotFound,
}

/// Outcome of a cancellation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// A running job was cancelled.
    Cancelled,
    /// The job had already finished; nothing to cancel.
    AlreadyFinished,
    /// No job with that id.
    NotFound,
}
