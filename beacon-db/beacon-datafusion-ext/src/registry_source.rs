//! A [`DataSource`] whose partitions are queries against the file-statistics
//! registry, not lists of files.
//!
//! # What actually had to change
//!
//! DataFusion already streams file *contents*: a `FileStream` opens one file
//! at a time and prefetches the next one's metadata. What it does not stream is
//! the *identity list* — `FileGroup { files: Vec<PartitionedFile>, .. }`, fixed
//! at plan time, ~280 bytes plus a path per file, which is over a gigabyte at
//! three million files, per plan, per concurrent query.
//!
//! So a partition here is a description of work rather than the work itself:
//! *this prefix, this path range, this snapshot*. At `execute(i)` the source
//! walks the registry over that range and yields file identities into the same
//! opener machinery. Memory per partition is a cursor and one in-flight file.
//!
//! # The three things that demanded the list up front
//!
//! 1. **Partition count.** `output_partitioning` is synchronous and must answer
//!    before execution. It no longer counts a file list: the count is *chosen*
//!    — `target_partitions`, applied to the registry's cheap per-prefix totals.
//! 2. **Plan-time statistics.** Summed from the registry's recorded row and
//!    byte counts, or `Absent` where a file has none. Nothing is opened.
//! 3. **`EXPLAIN`.** It can no longer print a file list, because there is none.
//!    It prints the predicate's effect instead — `files=N pruned=M` — and the
//!    `file_stats_files_listed` / `_pruned` counters under the node become the
//!    primary evidence rather than a nice-to-have.
//!
//! # Two modes, because pruning cannot be lazy
//!
//! - [`Partitions::Streaming`] — no usable predicate. Partitions are path
//!   shards cut by [`shard_prefix`](beacon_file_stats::RegistrySnapshot::shard_prefix),
//!   which reads only the path index and so never decodes a record at plan
//!   time. Nothing is enumerated: a `SELECT *` over three million files plans
//!   in constant memory.
//! - [`Partitions::Ids`] — a predicate the statistics can answer.
//!   [`prune_files`](beacon_file_stats::prune_files) evaluates a row per
//!   candidate, so the candidates must be named; what the plan then holds is
//!   the survivors as 8-byte ids (24 MB at 3M, against 1.1 GB of
//!   `PartitionedFile`), fetched back as records in chunks at execute time.
//!
//! # Consistency
//!
//! Both modes read one [`RegistrySnapshot`] opened at plan time and shared by
//! every partition, so a discovery pass committing mid-query cannot shift the
//! ground under a running scan. redb read transactions are MVCC, so this costs
//! nothing but the transaction's lifetime.
//!
//! # What is still a `FileScanConfig`
//!
//! The scan's *configuration* — schema, projection, pushed-down predicate,
//! limit, object-store URL — with an **empty** file list. That is deliberate
//! reuse: every format's [`FileOpener`] is created from such a config, and
//! DataFusion's optimizer pushes projections and filters into sources by
//! rewriting it. Delegating those rewrites keeps Parquet row-group and page
//! pruning working inside each file while the list of files stays a query. The
//! config's per-file machinery is simply never fed a file.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use beacon_file_stats::{FileId, FileRecord, PathShard, SharedSnapshot};
use chrono::TimeZone;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener, FileScanConfig};
use datafusion::datasource::source::DataSource;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, Partitioning};
use futures::Stream;
use futures::stream::BoxStream;
use object_store::ObjectMeta;

/// Files fetched per registry read in [`Partitions::Ids`], and files buffered
/// per walk step in [`Partitions::Streaming`]. Small enough that a partition's
/// working set stays constant, large enough to amortise the read.
const FETCH_CHUNK: usize = 1024;

/// One streaming partition: a path range, and the prefix that bounds it.
///
/// The prefix travels with the shard rather than with the scan, because a
/// table over several URLs cuts each one's shards under its own prefix. A
/// shared prefix would let the last shard of one URL walk into the next.
#[derive(Debug, Clone)]
pub struct ShardQuery {
    pub prefix: String,
    pub shard: PathShard,
}

/// Which files a partition reads, and how it learns them.
#[derive(Debug, Clone)]
pub enum Partitions {
    /// A partition is a path range walked lazily. Nothing was enumerated.
    Streaming {
        /// The extension a walked path must carry, and the URLs whose globs
        /// decide whether it belongs to this table. Every URL gets a say: a
        /// file under one table path must not be dropped for failing another's
        /// glob.
        extension: String,
        urls: Arc<Vec<ListingTableUrl>>,
        ignore_subdirectory: bool,
        shards: Arc<Vec<ShardQuery>>,
    },
    /// A partition is a slice of surviving ids, because a predicate had to be
    /// evaluated against named candidates.
    Ids {
        ids: Arc<Vec<FileId>>,
        ranges: Arc<Vec<Range<usize>>>,
    },
}

impl Partitions {
    fn len(&self) -> usize {
        match self {
            Partitions::Streaming { shards, .. } => shards.len(),
            Partitions::Ids { ranges, .. } => ranges.len(),
        }
    }

    /// Files this scan will read. Exact once pruning has named them, and the
    /// shards' own count while streaming — which is what the registry saw at
    /// plan time, before the glob had its say per file.
    fn planned_files(&self) -> u64 {
        match self {
            Partitions::Streaming { shards, .. } => {
                shards.iter().map(|query| query.shard.files).sum()
            }
            Partitions::Ids { ids, .. } => ids.len() as u64,
        }
    }
}

/// A scan source planned from the registry.
pub struct RegistryScanSource {
    /// Schema, projection, predicate, limit and store URL — with an empty file
    /// list. Openers are created from this, and optimizer pushdowns rewrite
    /// it; see the module docs for why this reuse is safe.
    base: FileScanConfig,
    /// The scan's output schema, projection applied.
    projected_schema: SchemaRef,
    /// One view of the registry for the whole query.
    snapshot: SharedSnapshot,
    partitions: Partitions,
    /// Aggregate plan-time statistics, in output-schema terms.
    statistics: Statistics,
    /// Files the plan considered. An estimate in streaming mode, where nothing
    /// was enumerated.
    files_considered: usize,
    files_pruned: usize,
}

impl RegistryScanSource {
    pub fn new(
        base: FileScanConfig,
        projected_schema: SchemaRef,
        snapshot: SharedSnapshot,
        partitions: Partitions,
        statistics: Statistics,
        files_considered: usize,
        files_pruned: usize,
    ) -> Self {
        Self {
            base,
            projected_schema,
            snapshot,
            partitions,
            statistics,
            files_considered,
            files_pruned,
        }
    }

    /// How this scan learns its files. For diagnostics and tests.
    pub fn partitions(&self) -> &Partitions {
        &self.partitions
    }

    pub fn snapshot(&self) -> &SharedSnapshot {
        &self.snapshot
    }

    /// Rebuild around a rewritten configuration, keeping the partitions.
    ///
    /// A pushdown may change the projection and with it the output schema, so
    /// both the schema and the column shape of the statistics are re-derived
    /// from the new config rather than copied.
    fn with_base(&self, base: FileScanConfig) -> Self {
        let projected_schema = DataSource::eq_properties(&base).schema().clone();
        let mut statistics = Statistics::new_unknown(projected_schema.as_ref());
        statistics.num_rows = self.statistics.num_rows;
        statistics.total_byte_size = self.statistics.total_byte_size;
        Self {
            base,
            projected_schema,
            snapshot: Arc::clone(&self.snapshot),
            partitions: self.partitions.clone(),
            statistics,
            files_considered: self.files_considered,
            files_pruned: self.files_pruned,
        }
    }
}

impl fmt::Debug for RegistryScanSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryScanSource")
            .field("files", &self.partitions.planned_files())
            .field("considered", &self.files_considered)
            .field("pruned", &self.files_pruned)
            .field("partitions", &self.partitions.len())
            .field("file_type", &self.base.file_source.file_type())
            .finish()
    }
}

impl DataSource for RegistryScanSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let store = context
            .runtime_env()
            .object_store(&self.base.object_store_url)?;
        // The batch size reaches the source at open time, exactly as
        // `FileScanConfig::open` hands it over; an opener created without one
        // panics.
        let batch_size = self
            .base
            .batch_size
            .unwrap_or_else(|| context.session_config().batch_size());
        let source = self.base.file_source.with_batch_size(batch_size);
        let opener = source.create_file_opener(store, &self.base, partition)?;

        let cursor = match &self.partitions {
            Partitions::Streaming {
                extension,
                urls,
                ignore_subdirectory,
                shards,
            } => {
                let query = shards.get(partition).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "registry scan asked for partition {partition} of {}",
                        shards.len()
                    ))
                })?;
                Cursor::Walk {
                    prefix: query.prefix,
                    extension: extension.clone(),
                    urls: Arc::clone(urls),
                    ignore_subdirectory: *ignore_subdirectory,
                    shard: query.shard,
                    resume: None,
                    done: false,
                }
            }
            Partitions::Ids { ids, ranges } => {
                let range = ranges.get(partition).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "registry scan asked for partition {partition} of {}",
                        ranges.len()
                    ))
                })?;
                Cursor::Ids {
                    ids: Arc::clone(ids),
                    range,
                }
            }
        };

        Ok(Box::pin(cooperative(RegistryFileStream {
            schema: Arc::clone(&self.projected_schema),
            snapshot: Arc::clone(&self.snapshot),
            cursor,
            queue: VecDeque::new(),
            opener,
            state: FileStreamState::Idle,
            remaining: self.base.limit,
        })))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        // No file list to print: the plan holds a query, not files. The counts
        // and the metrics counters are the evidence instead.
        let mode = match &self.partitions {
            Partitions::Streaming { .. } => "streaming",
            Partitions::Ids { .. } => "pruned",
        };
        // `files` is what the scan will read; `pruned` is what the predicate
        // removed on the way there. Their sum is what it considered.
        write!(
            f,
            "RegistryScanExec: file_type={}, mode={mode}, files={}, pruned={}, partitions={}",
            self.base.file_source.file_type(),
            self.partitions.planned_files(),
            self.files_pruned,
            self.partitions.len(),
        )?;
        if let Some(limit) = self.base.limit {
            write!(f, ", limit={limit}")?;
        }
        self.base.file_source.fmt_extra(t, f)
    }

    fn output_partitioning(&self) -> Partitioning {
        // Chosen, not derived from a file list.
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(Arc::clone(&self.projected_schema))
    }

    fn scheduling_type(&self) -> SchedulingType {
        // The stream is wrapped in `cooperative` at open, like the listing
        // scan's, so long files yield to their peers.
        SchedulingType::Cooperative
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        match partition {
            // Row counts are recorded per file, not per shard; only the
            // aggregate is claimed.
            Some(_) => Ok(Statistics::new_unknown(self.projected_schema.as_ref())),
            None => Ok(self.statistics.clone()),
        }
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let base =
            datafusion::datasource::physical_plan::FileScanConfigBuilder::from(self.base.clone())
                .with_limit(limit)
                .build();
        Some(Arc::new(self.with_base(base)))
    }

    fn fetch(&self) -> Option<usize> {
        self.base.limit
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.base.file_source.metrics().clone()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &datafusion::physical_expr::projection::ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        // The inner config owns projection pushdown; adopt its rewrite and
        // keep the partitions. This is how a narrow `SELECT` reaches the file
        // reader.
        match DataSource::try_swapping_with_projection(&self.base, projection)? {
            Some(rewritten) => Ok(rebuilt(self, rewritten)),
            None => Ok(None),
        }
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        // Likewise for predicates: the inner config hands them to the file
        // source, which is what keeps Parquet row-group and page pruning
        // working inside each file.
        let result = DataSource::try_pushdown_filters(&self.base, filters, config)?;
        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node: result.updated_node.and_then(|node| rebuilt(self, node)),
        })
    }
}

/// Adopt a rewritten inner config, or decline when it is not one.
///
/// Declining costs an optimization, never correctness: the source keeps the
/// configuration it had.
fn rebuilt(
    source: &RegistryScanSource,
    rewritten: Arc<dyn DataSource>,
) -> Option<Arc<dyn DataSource>> {
    let base = rewritten.as_any().downcast_ref::<FileScanConfig>()?.clone();
    Some(Arc::new(source.with_base(base)) as Arc<dyn DataSource>)
}

/// Where a partition's next file identities come from.
enum Cursor {
    /// Walk a path range of the registry, resuming where the last step
    /// stopped.
    Walk {
        prefix: String,
        extension: String,
        urls: Arc<Vec<ListingTableUrl>>,
        ignore_subdirectory: bool,
        shard: PathShard,
        /// The key the next step starts at. `None` means the shard's start.
        resume: Option<Vec<u8>>,
        done: bool,
    },
    /// Step through a slice of already-decided ids.
    Ids {
        ids: Arc<Vec<FileId>>,
        range: Range<usize>,
    },
}

/// One partition's reader: a cursor to file identities to open files to
/// batches, a chunk at a time.
struct RegistryFileStream {
    schema: SchemaRef,
    snapshot: SharedSnapshot,
    cursor: Cursor,
    /// Identities fetched but not yet opened. Never longer than
    /// [`FETCH_CHUNK`].
    queue: VecDeque<FileRecord>,
    opener: Arc<dyn FileOpener>,
    state: FileStreamState,
    /// Rows this partition may still emit under the scan's limit.
    remaining: Option<usize>,
}

enum FileStreamState {
    /// No file is open; the next identity in the queue is due.
    Idle,
    /// A file is being opened.
    Opening(FileOpenFuture),
    /// A file's batches are being read.
    Reading(BoxStream<'static, Result<RecordBatch>>),
    /// Every file is read, or an error ended the stream.
    Done,
}

impl RegistryFileStream {
    /// The next file to open, refilling from the cursor when the queue runs
    /// dry. `Ok(None)` when the partition is exhausted.
    fn next_file(&mut self) -> Result<Option<FileRecord>> {
        if self.queue.is_empty() {
            self.refill()?;
        }
        Ok(self.queue.pop_front())
    }

    fn refill(&mut self) -> Result<()> {
        match &mut self.cursor {
            Cursor::Walk {
                prefix,
                extension,
                urls,
                ignore_subdirectory,
                shard,
                resume,
                done,
            } => {
                if *done {
                    return Ok(());
                }
                // Walk from where the last step stopped, taking one chunk.
                // `resume` narrows the shard rather than replacing it, so the
                // shard's own end bound still applies.
                let mut step = shard.clone();
                if let Some(resume) = resume.take() {
                    step.start = resume;
                }
                let queue = &mut self.queue;
                let mut last: Option<Vec<u8>> = None;
                let mut filled = 0usize;
                self.snapshot
                    .for_each_in_shard(prefix, &step, |_, record| {
                        last = Some(record.path.as_bytes().to_vec());
                        if keep(urls, extension, *ignore_subdirectory, &record) {
                            queue.push_back(record);
                            filled += 1;
                        }
                        filled < FETCH_CHUNK
                    })
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                match last {
                    // Resume just past the last key seen, so no file is read
                    // twice and none is skipped.
                    Some(key) if filled >= FETCH_CHUNK => {
                        let mut next = key;
                        next.push(0);
                        *resume = Some(next);
                    }
                    _ => *done = true,
                }
                Ok(())
            }
            Cursor::Ids { ids, range } => {
                if range.start >= range.end {
                    return Ok(());
                }
                let take = range.len().min(FETCH_CHUNK);
                let chunk = &ids[range.start..range.start + take];
                range.start += take;

                let records = self
                    .snapshot
                    .records_for_ids(chunk)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                for (id, record) in chunk.iter().zip(records) {
                    let record = record.ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "the registry planned file {id} into this scan but no longer holds it"
                        ))
                    })?;
                    self.queue.push_back(record);
                }
                Ok(())
            }
        }
    }
}

/// Whether a walked file belongs to this table.
///
/// The shard bounds only say "in this path range"; the glob and the extension
/// still decide, exactly as they do on the listing path. Any URL matching is
/// enough — a table over several paths must not drop a file of one for failing
/// another's glob. A path that does not parse cannot be opened, so it is not
/// this scan's file.
fn keep(
    urls: &[ListingTableUrl],
    extension: &str,
    ignore_subdirectory: bool,
    record: &FileRecord,
) -> bool {
    if !record.path.ends_with(extension) {
        return false;
    }
    match object_store::path::Path::parse(&record.path) {
        Ok(location) => urls
            .iter()
            .any(|url| url.contains(&location, ignore_subdirectory)),
        Err(_) => false,
    }
}

/// The file identity the opener needs, built from a record and dropped after
/// the open.
fn partitioned_file(record: &FileRecord) -> Result<PartitionedFile> {
    let location = object_store::path::Path::parse(&record.path)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let last_modified = chrono::Utc
        .timestamp_millis_opt(record.last_modified_millis)
        .single()
        .unwrap_or_else(chrono::Utc::now);
    Ok(PartitionedFile::from(ObjectMeta {
        location,
        last_modified,
        size: record.size,
        e_tag: record.e_tag.clone(),
        version: None,
    }))
}

impl Stream for RegistryFileStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                FileStreamState::Done => return Poll::Ready(None),
                FileStreamState::Idle => {
                    if this.remaining == Some(0) {
                        this.state = FileStreamState::Done;
                        continue;
                    }
                    let record = match this.next_file() {
                        Ok(Some(record)) => record,
                        Ok(None) => {
                            this.state = FileStreamState::Done;
                            continue;
                        }
                        Err(error) => {
                            this.state = FileStreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    };
                    let opened =
                        partitioned_file(&record).and_then(|file| this.opener.open(file));
                    match opened {
                        Ok(future) => this.state = FileStreamState::Opening(future),
                        Err(error) => {
                            this.state = FileStreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
                FileStreamState::Opening(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(stream)) => this.state = FileStreamState::Reading(stream),
                    Poll::Ready(Err(error)) => {
                        this.state = FileStreamState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                FileStreamState::Reading(stream) => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        let batch = match &mut this.remaining {
                            Some(remaining) => {
                                let take = batch.num_rows().min(*remaining);
                                *remaining -= take;
                                if take < batch.num_rows() {
                                    batch.slice(0, take)
                                } else {
                                    batch
                                }
                            }
                            None => batch,
                        };
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Some(Err(error))) => {
                        this.state = FileStreamState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Ready(None) => this.state = FileStreamState::Idle,
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

impl RecordBatchStream for RegistryFileStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
