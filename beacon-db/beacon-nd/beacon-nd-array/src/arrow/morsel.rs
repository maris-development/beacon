//! Morsel-driven scanning: one queue for a scan, and workers that pull from it.
//!
//! Nothing is assigned at plan time. Every file of a scan goes into one queue,
//! and a worker takes the next unit of work when it is free. Balance follows
//! completion, so no estimate is needed and no estimate can be wrong.
//!
//! This replaces a deal made from file sizes. That deal could not balance,
//! because file size does not say what a file costs: an nd file's work follows
//! the uncompressed cells the query keeps, and a netCDF collection holds those
//! at compression ratios that differ file to file. One CORA year is 19 GB of
//! grid in 2.3 GB on disk.
//!
//! See `MORSEL_DRIVEN_SCAN.md` at the crate root for the measurements this
//! comes from.
//!
//! # Two levels
//!
//! A whole file is not a fine enough unit on its own. Four large files cannot
//! fill twenty-four partitions. So work is cut at two levels, and a worker
//! reaches the second only when the first runs dry:
//!
//! ```text
//! LEVEL 1 — a file                   LEVEL 2 — a chunk of an open file
//! ─────────────────                  ─────────────────────────────────
//! unit : one whole file              unit : one subset off its queue
//! cost : one open                    cost : none, the file is open
//! when : files remain unopened       when : no file remains unopened
//! gives: balance over many files     gives: balance over few large files
//! ```
//!
//! Level 2 is not new. [`FileRead`] already holds a queue of the chunks a
//! file is worth reading, and already hands them out one at a time. This module
//! only decides *who* may draw from it, and when.
//!
//! # The worker loop
//!
//! ```text
//!            ┌───────────────────────┐  yes   ┌──────────────────┐
//!            │ chunks left in the    ├───────>│ read one chunk,  │
//!            │ file I hold?          │        │ emit a batch     │──┐
//!            └───────────┬───────────┘        └──────────────────┘  │
//!                        │ no                                       │
//!                        v                                          │
//!            ┌───────────────────────┐  yes   ┌──────────────────┐  │
//!            │ a file left unopened? ├───────>│ open it, hold it │──┤
//!            │  (level 1)            │        └──────────────────┘  │
//!            └───────────┬───────────┘                              │
//!                        │ no                                       │
//!                        v                                          │
//!            ┌───────────────────────┐  yes                         │
//!            │ an open file with     ├─────────────────────────────>┤
//!            │ chunks left? (level 2)│                              │
//!            └───────────┬───────────┘                              │
//!                        │ no                                       │
//!                        v                                          │
//!                   ┌─────────┐<─────────────────────────────────────┘
//!                   │  done   │
//!                   └─────────┘
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::record_batch::RecordBatch;
use crossbeam::queue::ArrayQueue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig};
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;
use futures::stream::BoxStream;
use parking_lot::RwLock;

use crate::arrow::metrics::ReadMetrics;
use crate::arrow::file_read::FileRead;

/// How a format opens one of its files.
///
/// The only thing a format supplies. Everything else about dividing a scan is
/// the same for netCDF, HDF5 and Zarr, because all three read through
/// [`FileRead`].
#[async_trait::async_trait]
pub trait OpenFile: Send + Sync + 'static {
    /// Open `file` and plan what the query reads from it.
    async fn open(&self, file: &PartitionedFile) -> Result<Arc<FileRead>>;
}

/// Every file of one scan, and the ones already open.
///
/// One of these per scan, shared by every partition. Between them the workers
/// read every file exactly once: a file leaves `unopened` once, and the queue
/// behind an open file hands out each chunk once.
#[derive(Debug)]
pub struct MorselSource {
    /// Files nobody has opened. Level 1.
    unopened: ArrayQueue<PartitionedFile>,
    /// Files opened and not yet drained. Level 2.
    ///
    /// Pruned whenever it is touched, so it holds about one entry per worker
    /// rather than one per file. Holding every file a scan ever opened would
    /// keep every one of their arrays alive.
    open: RwLock<Vec<Arc<FileRead>>>,
    /// Opens in progress.
    ///
    /// A worker that finds `unopened` empty and nothing open is not necessarily
    /// finished: another worker may be part-way through an open whose chunks it
    /// could share. This is how it tells the difference.
    opening: AtomicUsize,
    /// Woken when an open finishes, so a worker waiting on one sleeps instead of
    /// spinning.
    ///
    /// It used to spin on [`tokio::task::yield_now`], which is a poor way to
    /// wait on a runtime that is busy: the waiter is rescheduled continuously
    /// and competes with the very task it is waiting for. Beacon's server runs
    /// its scans on 8 worker threads by default while planning 24 partitions, so
    /// "busy" is the normal case rather than the exception.
    opened: tokio::sync::Notify,
    /// Files this scan started with. For diagnostics.
    files: usize,
}

impl MorselSource {
    /// A source over every file the scan reads.
    pub fn new(files: Vec<PartitionedFile>) -> Arc<Self> {
        let count = files.len();
        // `ArrayQueue` will not take a capacity of zero, and a scan with no
        // files still needs a queue for its workers to find empty.
        let unopened = ArrayQueue::new(count.max(1));
        for file in files {
            // The capacity is the file count, so this cannot fail.
            let _ = unopened.push(file);
        }
        Arc::new(Self {
            unopened,
            open: RwLock::new(Vec::new()),
            opening: AtomicUsize::new(0),
            opened: tokio::sync::Notify::new(),
            files: count,
        })
    }

    /// How many files this scan started with.
    pub fn files(&self) -> usize {
        self.files
    }

    /// Files nobody has opened yet. For tests and diagnostics.
    pub fn unopened(&self) -> usize {
        self.unopened.len()
    }

    /// Files open and not yet drained. For tests and diagnostics.
    pub fn open_files(&self) -> usize {
        self.open.read().len()
    }

    /// One worker's stream over the whole scan.
    ///
    /// Every partition calls this on the same `MorselSource`. `worker` is the
    /// partition index; it decides which open file this worker prefers at level
    /// 2, so the workers spread over the last few files rather than meeting at
    /// one of them.
    ///
    /// The stream is a plain [`Stream`](futures::Stream) and needs no task of
    /// its own: it is the worker loop, driven by whoever polls it.
    pub fn stream(
        self: &Arc<Self>,
        worker: usize,
        opener: Arc<dyn OpenFile>,
        metrics: Option<ReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let state = Worker {
            source: Arc::clone(self),
            worker,
            opener,
            metrics,
            failed: false,
            next: None,
        };

        futures::stream::unfold(state, |mut state| async move {
            if state.failed {
                return None;
            }
            match state.take().await {
                Ok(Some(dataset)) => {
                    let batches = dataset.stream(state.metrics.clone());
                    Some((batches, state))
                }
                Ok(None) => None,
                Err(error) => {
                    // Report it and stop. Carrying on would read a partial scan
                    // and return it as a whole one.
                    state.failed = true;
                    Some((
                        futures::stream::once(async move { Err(error) }).boxed(),
                        state,
                    ))
                }
            }
        })
        .flatten()
        .boxed()
    }

    /// Open one file, naming it if that fails.
    ///
    /// `FileStream` would have put the path in the error. Nothing else will,
    /// because the whole scan reaches DataFusion as one file.
    async fn open_one(
        &self,
        opener: &dyn OpenFile,
        file: &PartitionedFile,
    ) -> Result<Arc<FileRead>> {
        opener.open(file).await.map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to open {}: {error}",
                file.object_meta.location
            ))
        })
    }

    /// Add a newly opened file to the ones workers may draw from.
    fn register(&self, dataset: Arc<FileRead>) {
        let mut open = self.open.write();
        open.retain(|dataset| dataset.remaining() > 0);
        open.push(dataset);
    }

    /// An open file with chunks left, preferring this worker's own.
    fn borrow_open(&self, worker: usize) -> Option<Arc<FileRead>> {
        let mut open = self.open.write();
        open.retain(|dataset| dataset.remaining() > 0);
        if open.is_empty() {
            return None;
        }
        // Spread the workers over what is left rather than piling them onto the
        // first entry. A drained file is dropped on the next look, so a worker
        // that lands on one simply comes back.
        Some(Arc::clone(&open[worker % open.len()]))
    }
}

/// Refuse a scan whose table declares partition columns.
///
/// A `PARTITIONED BY` column lives in the *path* of a file rather than inside
/// it, and `FileStream` appends its value to every batch of that file — which it
/// can do only because it knows which file each batch came from.
///
/// An nd scan reads a whole collection behind one plan entry, so `FileStream`
/// does not know. The file readers do it themselves instead: a morsel carries
/// its file's values and
/// [`FilePartitions`](crate::arrow::partition::FilePartitions) appends them to
/// its batches. That works because a morsel is a file.
///
/// A Zarr table is not made of files. Its entries are groups inside a store,
/// and a group is not a path a partition value can be read off, so Zarr still
/// refuses such a table. The alternatives are to return those columns silently
/// empty or to say so, and a query that quietly drops a column it was asked for
/// is the worse of the two.
///
/// `format` names the reader in the error, since a user reaches this through
/// `CREATE EXTERNAL TABLE ... PARTITIONED BY` and needs to know which format
/// refused.
pub fn reject_partition_columns(format: &str, config: &FileScanConfig) -> Result<()> {
    let columns = config.table_partition_cols();
    if columns.is_empty() {
        return Ok(());
    }

    let names: Vec<&str> = columns.iter().map(|field| field.name().as_str()).collect();
    Err(DataFusionError::NotImplemented(format!(
        "{format} does not support partitioned tables (PARTITIONED BY {}). \
         A partition column is part of a file's path, and this reader scans a \
         collection as one unit, so it cannot tell which file a row came from.",
        names.join(", ")
    )))
}

/// Plan a scan morsel-driven: one queue for its files, one entry per partition.
///
/// Returns the source every partition draws from, and the file groups to put in
/// the scan config. A format calls this from `repartitioned` and stores the
/// source on itself, so the openers it builds later reach the same one.
///
/// ```text
///  planned by a deal                    planned morsel-driven
///  ─────────────────                    ─────────────────────
///  [f0 ... f148]                        [scan]
///  [f149 ... f297]                      [scan]
///   ...                                  ...
///  [f3441 ... f3583]                    [scan]
///
///  3,584 entries, 3,584 opens           24 entries, 3,584 opens
/// ```
///
/// The entry is not a file. It is the scan, and the opener behind it reads
/// whatever the queue hands out. Its path says so, and its size is the scan's,
/// so `EXPLAIN` still shows how much a partition is pointed at.
///
/// A partitioned table divides the same way. A file's `PARTITIONED BY` values
/// travel on its [`PartitionedFile`], the queue hands that whole entry to
/// whoever opens it, and the reader appends the values to the batches it reads
/// from it — see
/// [`FilePartitions`](crate::arrow::partition::FilePartitions). So the standing
/// entry below carries no values of its own, and needs none.
///
/// Returns `None` when the scan should keep the grouping it was planned with:
///
/// - one partition, where there is nobody to divide the work with;
/// - no files.
pub fn morsel_scan(
    file_groups: &[FileGroup],
    target_partitions: usize,
) -> Option<(Arc<MorselSource>, Vec<FileGroup>)> {
    if target_partitions <= 1 {
        return None;
    }

    let files: Vec<PartitionedFile> = file_groups
        .iter()
        .flat_map(FileGroup::iter)
        .cloned()
        .collect();
    if files.is_empty() {
        return None;
    }

    let count = files.len();
    let total: u64 = files.iter().map(|file| file.effective_size()).sum();
    let source = MorselSource::new(files);

    let entry = PartitionedFile::new(format!("nd-morsel-scan/{count}-files"), total);
    let groups = (0..target_partitions)
        .map(|_| FileGroup::new(vec![entry.clone()]))
        .collect();

    Some((source, groups))
}

/// One worker's place in the scan.
struct Worker {
    source: Arc<MorselSource>,
    worker: usize,
    opener: Arc<dyn OpenFile>,
    metrics: Option<ReadMetrics>,
    /// An open failed. The stream reported it and is over.
    failed: bool,
    /// This worker's next file, already opening.
    ///
    /// The worker loop is a stream of streams: while the batches of one file are
    /// being polled, the loop that would fetch the next file is not. So without
    /// this the two never overlap, and every worker sits idle through every open
    /// it makes. `FileStream` opened one file ahead for exactly this reason, and
    /// a scan behind one entry has to do it itself.
    next: Option<tokio::task::JoinHandle<Result<Arc<FileRead>>>>,
}

impl Worker {
    /// Start opening this worker's next file, unless one is already in flight.
    ///
    /// The open runs as its own task, so it makes progress while this worker
    /// reads the file it already holds. A no-op when nothing is left to open.
    fn prefetch(&mut self) {
        if self.next.is_some() {
            return;
        }
        let Some(file) = self.source.unopened.pop() else {
            return;
        };

        // Counted from before the task starts. A worker asking whether the scan
        // is over must not see a gap between the pop and the open, or it will
        // finish while this file is still on its way.
        self.source.opening.fetch_add(1, Ordering::AcqRel);
        let source = Arc::clone(&self.source);
        let opener = Arc::clone(&self.opener);

        self.next = Some(tokio::spawn(async move {
            let opened = source.open_one(opener.as_ref(), &file).await;
            if let Ok(dataset) = &opened {
                // Published as soon as it is open, rather than when its own
                // worker gets to it: a worker that runs dry meanwhile can take
                // its chunks.
                source.register(Arc::clone(dataset));
            }
            source.opening.fetch_sub(1, Ordering::AcqRel);
            // After the register and the decrement, so a woken worker sees both.
            // Also on failure: a worker waiting for this open must not wait for
            // one that is never coming.
            source.opened.notify_waiters();
            opened
        }));
    }

    /// The next thing this worker should read, or `None` when the scan is done.
    async fn take(&mut self) -> Result<Option<Arc<FileRead>>> {
        loop {
            // Level 1. Opening a file is preferred over helping with one: it
            // gives this worker something no other worker is on, and it adds a
            // queue the others can draw from later.
            self.prefetch();

            if let Some(handle) = self.next.take() {
                // Start the one after it *before* waiting on this one, so the
                // pipeline stays full across the whole scan rather than just
                // the first file.
                self.prefetch();
                return handle
                    .await
                    .map_err(|error| {
                        DataFusionError::Execution(format!("nd morsel open failed: {error}"))
                    })?
                    .map(Some);
            }

            // Nothing left to open. Register for the next open to finish
            // *before* looking at what is available, so an open that completes
            // between the look and the wait still wakes this worker.
            // `notify_waiters` wakes only those already registered, so checking
            // first and registering after would lose that wake-up and hang here
            // for the rest of the scan.
            let mut opened = std::pin::pin!(self.source.opened.notified());
            opened.as_mut().enable();

            // Level 2. Help with a file somebody else opened.
            if let Some(dataset) = self.source.borrow_open(self.worker) {
                return Ok(Some(dataset));
            }

            // Nothing open and nothing left to open. If another worker is still
            // opening a file, its chunks are work this one could take, so wait
            // for it rather than finishing early — finishing early is the whole
            // problem this module exists to solve. The wait is bounded by one
            // open and only happens at the end of a scan.
            if self.source.opening.load(Ordering::Acquire) == 0 {
                return Ok(None);
            }
            opened.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;

    use arrow::datatypes::{Schema, SchemaRef};
    use datafusion::error::DataFusionError;
    use futures::TryStreamExt;
    use indexmap::IndexMap;
    use parking_lot::Mutex;

    use super::*;
    use crate::NdArray;
    use crate::NdArrayD;
    use crate::dataset::{AnyDataset, Dataset};

    /// A projection that wants no column, so a plan takes the `COUNT(*)` path
    /// and a batch carries only its row count. These tests are about who reads
    /// what, not about column values.
    fn no_columns() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    /// A dataset of `rows` values on one dimension.
    async fn dataset(rows: usize) -> AnyDataset {
        let values = NdArray::<i64>::try_new_from_vec_in_mem(
            (0..rows as i64).collect(),
            vec![rows],
            vec!["row".to_string()],
            None,
        )
        .unwrap();

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("value".to_string(), Arc::new(values));
        AnyDataset::Regular(Dataset::new("morsel".to_string(), arrays).await)
    }

    /// A file of `rows` rows, named `f-{index}`.
    fn file(index: usize, rows: usize) -> PartitionedFile {
        PartitionedFile::new(format!("f-{index:04}.nc"), rows as u64)
    }

    /// An opener over in-memory datasets, recording what it was asked to open.
    ///
    /// A file's row count is its `object_meta.size`, so a test says how much
    /// work each file holds by how big it claims to be.
    #[derive(Default)]
    struct Fake {
        opened: Mutex<Vec<String>>,
        batch_size: usize,
        /// Paths that fail to open.
        broken: HashSet<String>,
        /// How long an open takes. Long enough makes the race in
        /// [`MorselSource::take`] happen every run instead of sometimes.
        opening_takes: Option<std::time::Duration>,
    }

    impl Fake {
        fn new(batch_size: usize) -> Arc<Self> {
            Arc::new(Self {
                opened: Mutex::new(Vec::new()),
                batch_size,
                broken: HashSet::new(),
                opening_takes: None,
            })
        }

        fn breaking(batch_size: usize, path: &str) -> Arc<Self> {
            Arc::new(Self {
                opened: Mutex::new(Vec::new()),
                batch_size,
                broken: HashSet::from([path.to_string()]),
                opening_takes: None,
            })
        }

        fn slow(batch_size: usize, millis: u64) -> Arc<Self> {
            Arc::new(Self {
                opened: Mutex::new(Vec::new()),
                batch_size,
                broken: HashSet::new(),
                opening_takes: Some(std::time::Duration::from_millis(millis)),
            })
        }

        fn opened(&self) -> Vec<String> {
            self.opened.lock().clone()
        }
    }

    #[async_trait::async_trait]
    impl OpenFile for Fake {
        async fn open(&self, file: &PartitionedFile) -> Result<Arc<FileRead>> {
            let path = file.object_meta.location.to_string();
            if self.broken.contains(&path) {
                return Err(DataFusionError::Execution("the disk said no".to_string()));
            }
            if let Some(delay) = self.opening_takes {
                tokio::time::sleep(delay).await;
            }
            self.opened.lock().push(path);
            FileRead::plan(
                dataset(file.object_meta.size as usize).await,
                no_columns(),
                self.batch_size,
                None,
                crate::arrow::partition::FilePartitions::none(),
                None,
            )
            .await
        }
    }

    /// Run `workers` workers over `source` and return the rows each one read.
    /// The longest any test here may take before it is treated as stuck.
    ///
    /// A worker waits on [`MorselSource::opened`] when an open is in flight, and
    /// the way that goes wrong is a missed wake-up: the worker sleeps and the
    /// scan never finishes. Without this bound the suite hangs instead of
    /// failing, which reports as a timed-out job rather than a broken test.
    /// Removing the `notify_waiters` call is exactly that mutation, and it turns
    /// this into a clean failure naming the worker that never came back.
    const BEFORE_STUCK: std::time::Duration = std::time::Duration::from_secs(20);

    async fn run(source: &Arc<MorselSource>, opener: Arc<dyn OpenFile>, workers: usize) -> Vec<usize> {
        let mut tasks = Vec::new();
        for worker in 0..workers {
            let stream = source.stream(worker, Arc::clone(&opener), None);
            tasks.push(tokio::spawn(async move {
                let batches: Vec<RecordBatch> = stream.try_collect().await?;
                Ok::<usize, DataFusionError>(batches.iter().map(|b| b.num_rows()).sum())
            }));
        }

        let mut rows = Vec::new();
        for (worker, task) in tasks.into_iter().enumerate() {
            let read = tokio::time::timeout(BEFORE_STUCK, task)
                .await
                .unwrap_or_else(|_| {
                    panic!("worker {worker} never finished — a wake-up was missed")
                })
                .expect("the worker finishes")
                .expect("it reads");
            rows.push(read);
        }
        rows
    }

    /// Every file is read, once, however the workers happen to interleave.
    ///
    /// This is the correctness property the whole module rests on. A file leaves
    /// the queue once, and the queue behind an open file hands out each chunk
    /// once, so no worker can duplicate another's work and none can be skipped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn every_file_is_read_exactly_once() {
        const FILES: usize = 200;
        const ROWS: usize = 64;
        const WORKERS: usize = 8;

        let opener = Fake::new(16);
        let source = MorselSource::new((0..FILES).map(|i| file(i, ROWS)).collect());

        let rows = run(&source, opener.clone(), WORKERS).await;

        assert_eq!(
            rows.iter().sum::<usize>(),
            FILES * ROWS,
            "every row of every file came back exactly once"
        );

        let opened = opener.opened();
        assert_eq!(opened.len(), FILES, "and every file was opened once");
        assert_eq!(
            opened.iter().collect::<HashSet<_>>().len(),
            FILES,
            "no file was opened twice"
        );
        assert_eq!(source.unopened(), 0, "the queue is empty");
    }

    /// The work spreads over the workers instead of landing on one.
    ///
    /// Not a balance assertion — that depends on scheduling — but the thing a
    /// deal could not guarantee at all: every worker gets some of it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn every_worker_gets_some_of_the_scan() {
        const FILES: usize = 400;
        const WORKERS: usize = 8;

        let opener = Fake::new(16);
        let source = MorselSource::new((0..FILES).map(|i| file(i, 64)).collect());

        let rows = run(&source, opener, WORKERS).await;

        assert_eq!(rows.len(), WORKERS);
        assert!(
            rows.iter().all(|read| *read > 0),
            "no worker sat idle while the others worked: {rows:?}"
        );
    }

    /// One worker reads the whole scan on its own.
    ///
    /// A scan on one partition has nobody to share with, and must still be
    /// complete.
    #[tokio::test]
    async fn one_worker_reads_the_whole_scan() {
        const FILES: usize = 20;
        const ROWS: usize = 32;

        let opener = Fake::new(8);
        let source = MorselSource::new((0..FILES).map(|i| file(i, ROWS)).collect());

        let rows = run(&source, opener, 1).await;
        assert_eq!(rows, vec![FILES * ROWS]);
    }

    /// More workers than files is not an error.
    ///
    /// The surplus find the queue empty. They must finish, not hang, and they
    /// must not invent rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn more_workers_than_files_still_finish() {
        const FILES: usize = 3;
        const ROWS: usize = 16;
        const WORKERS: usize = 16;

        let opener = Fake::new(8);
        let source = MorselSource::new((0..FILES).map(|i| file(i, ROWS)).collect());

        let rows = run(&source, opener, WORKERS).await;

        assert_eq!(rows.len(), WORKERS);
        assert_eq!(rows.iter().sum::<usize>(), FILES * ROWS);
    }

    /// A scan with no files finishes at once, on every worker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_empty_scan_finishes() {
        let opener = Fake::new(8);
        let source = MorselSource::new(Vec::new());

        let rows = run(&source, opener, 4).await;
        assert_eq!(rows, vec![0; 4]);
        assert_eq!(source.files(), 0);
    }

    /// Few files and many workers: the file is opened once and read once.
    ///
    /// This is level 2, and the case a deal of whole files cannot serve at all.
    /// Eight workers reach one file; between them they read it exactly once,
    /// however the eight happen to interleave.
    ///
    /// No test asserts how much of the file each worker gets. The scheduler
    /// decides that. The first worker can empty the queue before the other seven
    /// start. One worker then reads the whole file alone. That result is
    /// correct.
    ///
    /// One property holds on every schedule. A worker does not stop while an
    /// open is in flight.
    /// [`a_worker_waits_for_a_file_another_is_still_opening`] asserts it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn one_big_file_is_read_once_by_whichever_workers_reach_it() {
        const ROWS: usize = 1_024;
        const BATCH: usize = 16;
        const WORKERS: usize = 8;

        let opener = Fake::new(BATCH);
        let source = MorselSource::new(vec![file(0, ROWS)]);

        let rows = run(&source, opener.clone(), WORKERS).await;

        assert_eq!(rows.iter().sum::<usize>(), ROWS, "the file is read once over");
        assert_eq!(opener.opened().len(), 1, "and opened once");
    }

    /// A worker waits for a file that another worker opens.
    ///
    /// The queue is empty and no file is open. The scan is not over. One worker
    /// is part way through an open. The other workers can take the chunks of
    /// that file. A worker that reads this state as "done" stops too early. This
    /// module exists to prevent that failure.
    ///
    /// This test sets the open in flight by hand. It then drives one worker step
    /// by step. A scan of real workers cannot show the wait. It shows only the
    /// result. The number of workers with rows depends on the speed of the first
    /// worker. The scheduler controls that speed. A count of those workers is
    /// therefore not a valid assertion. It reports a busy machine as a bug.
    ///
    /// This test asserts the two parts of the wait instead. Part 1: the worker
    /// stays while the open is in flight. Part 2: the worker takes the chunks
    /// after the open publishes them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_worker_waits_for_a_file_another_is_still_opening() {
        const ROWS: usize = 512;
        const BATCH: usize = 8;
        /// A worker that stops early stops in less time than this.
        const A_MOMENT: std::time::Duration = std::time::Duration::from_millis(50);

        let opener = Fake::new(BATCH);
        // The scan has no files. Nothing is open, and nothing is left to open.
        // The open in flight is the only reason to stay.
        let source = MorselSource::new(Vec::new());
        source.opening.fetch_add(1, Ordering::AcqRel);

        let mut worker = Worker {
            source: Arc::clone(&source),
            worker: 0,
            opener: opener.clone(),
            metrics: None,
            failed: false,
            next: None,
        };
        // The test holds this future across both parts. A second call to `take`
        // cannot show that the wake-up reaches the first waiter.
        let mut taking = std::pin::pin!(worker.take());

        assert!(
            tokio::time::timeout(A_MOMENT, taking.as_mut()).await.is_err(),
            "the worker stays for the open in flight. It does not report the scan as done"
        );

        // The open finishes. The prefetch task ends in this order: publish the
        // file, drop the count, wake the waiters.
        let dataset = opener.open(&file(0, ROWS)).await.expect("the file opens");
        source.register(dataset);
        source.opening.fetch_sub(1, Ordering::AcqRel);
        source.opened.notify_waiters();

        let taken = tokio::time::timeout(BEFORE_STUCK, taking)
            .await
            .expect("the worker wakes when the open finishes")
            .expect("and it takes work, not an error")
            .expect("the scan is not over: the file has chunks");

        let batches: Vec<RecordBatch> = taken.stream(None).try_collect().await.expect("it reads");
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            ROWS,
            "and the worker takes every chunk of the file"
        );
    }

    /// A slow open reads the file one time, with real workers.
    ///
    /// [`a_worker_waits_for_a_file_another_is_still_opening`] drives one worker
    /// by hand. This test uses the worker loop itself. Eight workers read one
    /// file. The open is slow, so seven workers reach the empty state before it
    /// ends. Each worker must wake and stop. A lost wake-up keeps a worker
    /// asleep. [`BEFORE_STUCK`] then fails the test and names that worker. A
    /// test without that limit hangs the job instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_slow_open_is_still_read_once() {
        const ROWS: usize = 512;
        const BATCH: usize = 8;
        const WORKERS: usize = 8;

        let opener = Fake::slow(BATCH, 60);
        let source = MorselSource::new(vec![file(0, ROWS)]);

        let rows = run(&source, opener.clone(), WORKERS).await;

        assert_eq!(rows.iter().sum::<usize>(), ROWS, "the file is read once over");
        assert_eq!(opener.opened().len(), 1, "and opened once");
    }

    /// The open list holds workers, not files — *while the scan runs*.
    ///
    /// It is pruned whenever it is touched. Left unpruned it would hold every
    /// file the scan ever opened, and every one of their arrays with it, for the
    /// length of the query.
    ///
    /// Two per worker is the ceiling, and it is the pipeline: a worker holds the
    /// file it is reading and has published the one it prefetched behind it.
    ///
    /// The measurement has to happen mid-scan. Checking after it ends proves
    /// nothing: the last look prunes the list on its way out, so a version that
    /// never pruned during the scan still finishes with it empty.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_open_list_does_not_grow_while_the_scan_runs() {
        const FILES: usize = 60;
        const WORKERS: usize = 2;

        // A slow open stretches the scan out, so the watcher below samples it
        // many times over rather than racing it.
        let opener = Fake::slow(16, 2);
        let source = MorselSource::new((0..FILES).map(|i| file(i, 32)).collect());

        let peak = Arc::new(AtomicUsize::new(0));
        let watched = Arc::clone(&source);
        let seen = Arc::clone(&peak);
        let watcher = tokio::spawn(async move {
            loop {
                seen.fetch_max(watched.open_files(), Ordering::AcqRel);
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        let rows = run(&source, opener, WORKERS).await;
        watcher.abort();

        assert_eq!(rows.iter().sum::<usize>(), FILES * 32, "the scan completed");
        let peak = peak.load(Ordering::Acquire);
        assert!(peak > 0, "the watcher saw the scan at all");
        assert!(
            peak <= 2 * WORKERS + 1,
            "{peak} files listed as open at once, for {WORKERS} workers over {FILES} files \
             — the ceiling is one held and one prefetched each"
        );
    }

    /// A morsel scan plans one entry per partition, whatever the file count.
    ///
    /// This is the point of the whole module. A deal plans one entry per file,
    /// and `FileStream` calls the opener once per entry — so a 3,584-file scan
    /// paid 3,584 opener calls whether or not it read the file behind each one.
    #[test]
    fn a_morsel_scan_plans_one_entry_per_partition() {
        const PARTITIONS: usize = 24;

        for files in [1, 100, 3_584] {
            let group = FileGroup::new((0..files).map(|i| file(i, 32)).collect());

            let (source, groups) = morsel_scan(&[group], PARTITIONS)
                .unwrap_or_else(|| panic!("{files} files plan morsel-driven"));

            assert_eq!(groups.len(), PARTITIONS, "one group per partition");
            for group in &groups {
                assert_eq!(group.len(), 1, "holding one entry, whatever the file count");
            }
            assert_eq!(source.files(), files, "and the queue holds every file");
            assert_eq!(source.unopened(), files);
        }
    }

    /// The entry names the scan, not a file, and carries its size.
    ///
    /// `EXPLAIN` shows this instead of a file list, so it has to say what a
    /// partition is pointed at.
    #[test]
    fn the_entry_describes_the_scan() {
        let group = FileGroup::new((0..10).map(|i| file(i, 100)).collect());
        let (_, groups) = morsel_scan(&[group], 4).expect("ten files plan morsel-driven");

        let entry = groups[0].iter().next().expect("an entry");
        assert_eq!(entry.object_meta.location.to_string(), "nd-morsel-scan/10-files");
        assert_eq!(entry.object_meta.size, 1_000, "the scan's bytes, not a file's");
    }

    /// A partitioned table divides like any other, values and all.
    ///
    /// The values travel on the [`PartitionedFile`] the queue holds, so whoever
    /// opens that file has them and appends them to what it reads. Only the
    /// standing entry is valueless, and nothing reads it as a file.
    #[test]
    fn a_partitioned_table_divides_and_keeps_its_values() {
        use datafusion::scalar::ScalarValue;

        let year = |value: &str| vec![ScalarValue::Utf8(Some(value.to_string()))];
        let mut first = file(0, 32);
        first.partition_values = year("2023");
        let mut second = file(1, 32);
        second.partition_values = year("2024");
        let group = FileGroup::new(vec![first, second]);

        let (source, groups) =
            morsel_scan(&[group], 8).expect("a partitioned table plans morsel-driven");

        assert_eq!(groups.len(), 8, "one entry per partition, as for any scan");
        assert_eq!(source.files(), 2, "and both files are in the queue");

        let mut taken: Vec<Vec<ScalarValue>> = Vec::new();
        while let Some(file) = source.unopened.pop() {
            taken.push(file.partition_values.clone());
        }
        taken.sort_by_key(|values| format!("{values:?}"));
        assert_eq!(
            taken,
            vec![year("2023"), year("2024")],
            "each file keeps the values of its own path"
        );

        let entry = groups[0].iter().next().expect("an entry");
        assert!(
            entry.partition_values.is_empty(),
            "the standing entry stands for the scan, not for a file, so it holds no values"
        );
    }

    /// One partition, or none at all, is left alone.
    #[test]
    fn a_scan_with_nothing_to_divide_is_left_alone() {
        let group = FileGroup::new((0..10).map(|i| file(i, 32)).collect());
        assert!(morsel_scan(&[group], 1).is_none(), "one partition divides nothing");
        assert!(morsel_scan(&[], 8).is_none(), "no files to divide");
        assert!(
            morsel_scan(&[FileGroup::new(vec![])], 8).is_none(),
            "an empty group is no files either"
        );
    }

    /// The planned scan reads every file exactly once, end to end.
    ///
    /// The planner and the worker loop are separately correct above; this is the
    /// two of them together, which is what a format actually wires up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_planned_scan_reads_every_file_once() {
        const FILES: usize = 300;
        const ROWS: usize = 64;
        const PARTITIONS: usize = 8;

        let group = FileGroup::new((0..FILES).map(|i| file(i, ROWS)).collect());
        let (source, groups) = morsel_scan(&[group], PARTITIONS).expect("it plans");

        let opener = Fake::new(16);
        // One worker per group, as `create_file_opener` would build them.
        let rows = run(&source, opener.clone(), groups.len()).await;

        assert_eq!(rows.iter().sum::<usize>(), FILES * ROWS);
        assert_eq!(opener.opened().len(), FILES, "every file opened once");
    }

    /// A file that will not open fails the scan, and says which file it was.
    ///
    /// DataFusion's `FileStream` would have put the path in the error. Nothing
    /// else will, because the whole scan reaches it as one file.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_file_that_will_not_open_names_itself() {
        let opener = Fake::breaking(16, "f-0003.nc");
        let source = MorselSource::new((0..8).map(|i| file(i, 32)).collect());

        // Whichever worker draws the broken file fails; run one so it is this one.
        let stream = source.stream(0, opener, None);
        let error = stream
            .try_collect::<Vec<RecordBatch>>()
            .await
            .expect_err("the scan fails");

        let message = error.to_string();
        assert!(
            message.contains("f-0003.nc"),
            "the error names the file: {message}"
        );
        assert!(
            message.contains("the disk said no"),
            "and keeps what the reader said: {message}"
        );
    }
}
