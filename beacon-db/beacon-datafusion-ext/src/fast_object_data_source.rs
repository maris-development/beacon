//! [`FastObjectDataSource`]: a scan whose partitions are cursors, and whose
//! pruning happens while it reads.
//!
//! # What this replaces
//!
//! DataFusion's own [`DataSource`] for files is `FileScanConfig`, whose file
//! list is `Vec<FileGroup>` of `Vec<PartitionedFile>` — ~280 bytes plus a path
//! per file, fixed at plan time. At three million files that is over a
//! gigabyte, per plan, per concurrent query, built before the first byte is
//! read. This source holds none of it. A partition is a cursor: a path range
//! of the registry, or a slice of an already-listed store. File identities are
//! produced one chunk at a time while the scan runs, and dropped once opened.
//!
//! # Pruning is part of the stream
//!
//! Statistics pruning used to be a plan-time phase: name every candidate, read
//! the segments its predicate columns live in, and hand the survivors to the
//! plan. That blocks the planner on object-store reads, serially, before the
//! query starts — and the work it does is exactly the work the scan is about
//! to do anyway.
//!
//! So it moved into the stream. The planner compiles the predicate (pure CPU,
//! no I/O) and stops. Each partition then walks its own range and, chunk by
//! chunk, asks [`prune_files`](beacon_file_stats::prune_files) which of *those*
//! files can match — reading only the segments that chunk needs, in parallel
//! with every other partition and pipelined with the file reads. A predicate
//! that rules out a whole chunk costs one segment read and no file opens.
//!
//! The visible consequence: `EXPLAIN` cannot say how many files were pruned,
//! because nothing has been pruned yet. `EXPLAIN ANALYZE` reports it, from the
//! counters this stream increments as it goes.
//!
//! # The one thing `FileScanConfig` is still needed for
//!
//! `FileSource::create_file_opener(&self, store, base_config: &FileScanConfig,
//! partition)` is DataFusion's trait signature, implemented by every format —
//! DataFusion's own and Beacon's ten. Beacon's read `projected_schema()` from
//! it, Parquet reads `limit`, `preserve_order` and the expression adapter; none
//! reads the file list. There is no other API in this version that turns a
//! format into a [`FileOpener`], so an empty one is built at `open()` purely as
//! that call's argument. It is a parameter block, not state: this struct has no
//! `FileScanConfig` field.
//!
//! # Pushdown
//!
//! Projections and filters are delegated straight to the [`FileSource`], which
//! owns them, so a narrow `SELECT` and a `WHERE` still reach the file reader
//! and still drive Parquet's row-group and page pruning inside each file.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use beacon_file_stats::{FileId, FileRecord, FileStatsStore, PathShard, SharedSnapshot};
use chrono::TimeZone;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfigBuilder, FileSource,
};
use datafusion::datasource::source::DataSource;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::utils::reassign_expr_columns;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::{DisplayFormatType, Partitioning};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream};
use object_store::ObjectMeta;

/// Files a cursor produces per step, and so the batch one prune call covers.
///
/// Sized against a segment rather than a page: the collector writes ~10 000
/// files per segment, so a chunk this size reads about one segment's block per
/// predicate column. Much smaller and the same block is read over and over;
/// much larger and the first rows wait longer.
const CHUNK: usize = 4096;

/// One streaming partition: a path range, and the prefix that bounds it.
///
/// The prefix travels with the shard rather than with the scan, because a
/// table over several URLs cuts each one's shards under its own prefix. A
/// shared prefix would let the last shard of one URL — which has no end bound
/// — walk into the next URL's range.
#[derive(Debug, Clone)]
pub struct ShardQuery {
    pub prefix: String,
    pub shard: PathShard,
}

/// How a partition learns which files it reads.
#[derive(Debug, Clone)]
pub enum Identities {
    /// Walk a path range of the registry. Nothing was enumerated to plan this.
    Shards {
        /// The extension a walked path must carry, and the URLs whose globs
        /// decide whether it belongs to this table. Any URL matching is
        /// enough: a file under one table path must not be dropped for failing
        /// another's glob.
        extension: String,
        urls: Arc<Vec<ListingTableUrl>>,
        ignore_subdirectory: bool,
        shards: Arc<Vec<ShardQuery>>,
    },
    /// Step through objects a store listing already reported. The shape a
    /// collection with no registry gets: no worse than the listing path, and
    /// still no `PartitionedFile` vector.
    Listed {
        objects: Arc<Vec<ObjectMeta>>,
        ranges: Arc<Vec<Range<usize>>>,
    },
}

impl Identities {
    /// How many partitions this scan runs.
    pub fn partitions(&self) -> usize {
        match self {
            Identities::Shards { shards, .. } => shards.len(),
            Identities::Listed { ranges, .. } => ranges.len(),
        }
    }

    /// Files this scan may read, before any predicate has been applied.
    ///
    /// An estimate while streaming: it is what the registry saw when the
    /// shards were cut, before each URL's glob had its say per file and before
    /// pruning ran — which now happens while the scan reads.
    pub fn files(&self) -> u64 {
        match self {
            Identities::Shards { shards, .. } => shards.iter().map(|q| q.shard.files).sum(),
            Identities::Listed { objects, .. } => objects.len() as u64,
        }
    }

    /// How this scan learns its files, for `EXPLAIN`.
    pub fn mode(&self) -> &'static str {
        match self {
            Identities::Shards { .. } => "streaming",
            Identities::Listed { .. } => "listed",
        }
    }
}

/// Everything the stream needs to drop files a predicate rules out.
///
/// Built at plan time, which costs no I/O: the predicate is compiled and the
/// store handle cloned. Every read it implies happens while the scan runs.
#[derive(Clone)]
pub struct StreamPruning {
    pub store: Arc<FileStatsStore>,
    pub predicate: Arc<dyn PhysicalExpr>,
    /// The table schema the predicate is written against — not the projected
    /// one, because a column a predicate prunes on need not be selected.
    pub table_schema: SchemaRef,
}

impl fmt::Debug for StreamPruning {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamPruning")
            .field("predicate", &self.predicate)
            .finish()
    }
}

/// A file scan that holds cursors instead of files.
pub struct FastObjectDataSource {
    /// The format's reader, and the owner of projection and filter pushdown.
    file_source: Arc<dyn FileSource>,
    /// Where the files live.
    object_store_url: ObjectStoreUrl,
    /// The scan's output schema, projection applied.
    projected_schema: SchemaRef,
    /// One view of the registry for the whole query. Absent when the
    /// identities came from a store listing.
    snapshot: Option<SharedSnapshot>,
    identities: Identities,
    /// Present when a predicate can be answered from stored statistics.
    pruning: Option<StreamPruning>,
    limit: Option<usize>,
    statistics: Statistics,
}

impl FastObjectDataSource {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        file_source: Arc<dyn FileSource>,
        object_store_url: ObjectStoreUrl,
        projected_schema: SchemaRef,
        snapshot: Option<SharedSnapshot>,
        identities: Identities,
        pruning: Option<StreamPruning>,
        limit: Option<usize>,
        statistics: Statistics,
    ) -> Self {
        Self {
            file_source,
            object_store_url,
            projected_schema,
            snapshot,
            identities,
            pruning,
            limit,
            statistics,
        }
    }

    /// How this scan learns its files. For diagnostics and tests.
    pub fn identities(&self) -> &Identities {
        &self.identities
    }

    pub fn snapshot(&self) -> Option<&SharedSnapshot> {
        self.snapshot.as_ref()
    }

    /// Whether a predicate is applied while this scan reads.
    pub fn prunes(&self) -> bool {
        self.pruning.is_some()
    }

    /// Adopt a rewritten file source, re-deriving the output schema from it.
    ///
    /// A projection pushdown changes what the scan emits, so the schema and
    /// the column shape of the statistics are re-derived rather than copied.
    fn with_file_source(&self, file_source: Arc<dyn FileSource>) -> Result<Self> {
        let projected_schema = projected_schema_of(&file_source)?;
        let mut statistics = Statistics::new_unknown(projected_schema.as_ref());
        statistics.num_rows = self.statistics.num_rows;
        statistics.total_byte_size = self.statistics.total_byte_size;
        Ok(Self {
            file_source,
            object_store_url: self.object_store_url.clone(),
            projected_schema,
            snapshot: self.snapshot.clone(),
            identities: self.identities.clone(),
            pruning: self.pruning.clone(),
            limit: self.limit,
            statistics,
        })
    }
}

/// What a file source emits once its projection is applied.
pub fn projected_schema_of(file_source: &Arc<dyn FileSource>) -> Result<SchemaRef> {
    let schema = file_source.table_schema().table_schema();
    match file_source.projection() {
        Some(projection) => Ok(Arc::new(projection.project_schema(schema)?)),
        None => Ok(Arc::clone(schema)),
    }
}

impl fmt::Debug for FastObjectDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastObjectDataSource")
            .field("mode", &self.identities.mode())
            .field("files", &self.identities.files())
            .field("prunes", &self.prunes())
            .field("partitions", &self.identities.partitions())
            .field("file_type", &self.file_source.file_type())
            .finish()
    }
}

impl DataSource for FastObjectDataSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let store = context.runtime_env().object_store(&self.object_store_url)?;
        // The batch size reaches the source at open time, the way
        // `FileScanConfig::open` hands it over; an opener built without one
        // panics.
        let batch_size = context.session_config().batch_size();
        let file_source = self.file_source.with_batch_size(batch_size);
        // Empty of files, and dropped after the call. See the module docs.
        let opener_config =
            FileScanConfigBuilder::new(self.object_store_url.clone(), Arc::clone(&file_source))
                .with_limit(self.limit)
                .build();
        let opener = file_source.create_file_opener(store, &opener_config, partition)?;

        let out_of_range = || {
            DataFusionError::Internal(format!(
                "fast object scan asked for partition {partition} of {}",
                self.identities.partitions()
            ))
        };
        let cursor = match &self.identities {
            Identities::Shards {
                extension,
                urls,
                ignore_subdirectory,
                shards,
            } => {
                let query = shards.get(partition).cloned().ok_or_else(out_of_range)?;
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
            Identities::Listed { objects, ranges } => Cursor::Listed {
                objects: Arc::clone(objects),
                range: ranges.get(partition).cloned().ok_or_else(out_of_range)?,
            },
        };

        // Counted as the scan runs, not as it plans: with pruning in the
        // stream, this is the only place the numbers exist.
        let metrics = self.file_source.metrics();
        let considered = MetricBuilder::new(metrics).global_counter("file_stats_files_considered");
        let pruned = MetricBuilder::new(metrics).global_counter("file_stats_files_pruned");

        Ok(Box::pin(cooperative(FastObjectStream {
            schema: Arc::clone(&self.projected_schema),
            snapshot: self.snapshot.clone(),
            cursor,
            pruning: self.pruning.clone(),
            pending: Vec::new(),
            queue: VecDeque::new(),
            opener,
            state: StreamState::Idle,
            remaining: self.limit,
            considered,
            pruned,
        })))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        // No file list to print: the plan holds cursors. `files` is what the
        // scan may read; how many a predicate removes is not known until it
        // runs, so `EXPLAIN ANALYZE`'s counters are what report that.
        write!(
            f,
            "FastObjectScan: file_type={}, mode={}, files={}, partitions={}",
            self.file_source.file_type(),
            self.identities.mode(),
            self.identities.files(),
            self.identities.partitions(),
        )?;
        if self.prunes() {
            write!(f, ", prune=stream")?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        self.file_source.fmt_extra(t, f)
    }

    fn output_partitioning(&self) -> Partitioning {
        // Chosen when the scan was planned, never derived from a file list.
        Partitioning::UnknownPartitioning(self.identities.partitions())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(Arc::clone(&self.projected_schema))
    }

    fn scheduling_type(&self) -> SchedulingType {
        // The stream is wrapped in `cooperative` at open, so a long file
        // yields to its peers.
        SchedulingType::Cooperative
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        match partition {
            // Row counts are recorded per file, not per cursor; only the
            // aggregate is claimed.
            Some(_) => Ok(Statistics::new_unknown(self.projected_schema.as_ref())),
            None => Ok(self.statistics.clone()),
        }
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let mut next = self.with_file_source(Arc::clone(&self.file_source)).ok()?;
        next.limit = limit;
        Some(Arc::new(next))
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.file_source.metrics().clone()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        // The file source owns projection pushdown; adopt its rewrite and keep
        // the cursors. This is how a narrow `SELECT` reaches the file reader.
        match self.file_source.try_pushdown_projection(projection)? {
            Some(file_source) => Ok(Some(Arc::new(self.with_file_source(file_source)?) as _)),
            None => Ok(None),
        }
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        // Filters arrive in terms of this source's *output*, so a projection
        // already pushed down has to be undone and the column indices remapped
        // to the table schema before the file source sees them. Skipping this
        // hands the reader indices that mean a different column.
        let table_schema = self.file_source.table_schema().table_schema();
        let filters = match self.file_source.projection() {
            Some(projection) => filters
                .into_iter()
                .map(|filter| projection.unproject_expr(&filter))
                .collect::<Result<Vec<_>>>()?,
            None => filters,
        };
        let filters = filters
            .into_iter()
            .map(|filter| reassign_expr_columns(filter, table_schema))
            .collect::<Result<Vec<_>>>()?;

        let result = self.file_source.try_pushdown_filters(filters, config)?;
        let updated_node = match result.updated_node {
            Some(file_source) => Some(Arc::new(self.with_file_source(file_source)?) as _),
            None => None,
        };
        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node,
        })
    }
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
    /// Step through objects a listing already reported.
    Listed {
        objects: Arc<Vec<ObjectMeta>>,
        range: Range<usize>,
    },
}

/// One partition's reader: a cursor to file identities, pruned, opened, read —
/// a chunk at a time.
struct FastObjectStream {
    schema: SchemaRef,
    snapshot: Option<SharedSnapshot>,
    cursor: Cursor,
    pruning: Option<StreamPruning>,
    /// The chunk a prune call is deciding on. Empty otherwise.
    pending: Vec<(Option<FileId>, ObjectMeta)>,
    /// Files cleared to open. Never longer than [`CHUNK`].
    queue: VecDeque<ObjectMeta>,
    opener: Arc<dyn FileOpener>,
    state: StreamState,
    /// Rows this partition may still emit under the scan's limit.
    remaining: Option<usize>,
    considered: Count,
    pruned: Count,
}

enum StreamState {
    /// No file is open; the next identity in the queue is due.
    Idle,
    /// A chunk's survivors are being decided.
    Pruning(BoxFuture<'static, Vec<FileId>>),
    /// A file is being opened.
    Opening(FileOpenFuture),
    /// A file's batches are being read.
    Reading(BoxStream<'static, Result<RecordBatch>>),
    /// Every file is read, or an error ended the stream.
    Done,
}

impl FastObjectStream {
    /// Take the next chunk of identities from the cursor into `pending`.
    ///
    /// `Ok(false)` when the cursor is exhausted.
    fn fill_pending(&mut self) -> Result<bool> {
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
                    return Ok(false);
                }
                let Some(snapshot) = &self.snapshot else {
                    return Err(DataFusionError::Internal(
                        "a registry walk needs a snapshot".to_string(),
                    ));
                };
                // Walk from where the last step stopped, taking one chunk.
                // `resume` narrows the shard rather than replacing it, so the
                // shard's own end bound still applies.
                let mut step = shard.clone();
                if let Some(resume) = resume.take() {
                    step.start = resume;
                }
                let pending = &mut self.pending;
                let mut last: Option<Vec<u8>> = None;
                let mut filled = 0usize;
                snapshot
                    .for_each_in_shard(prefix, &step, |id, record| {
                        last = Some(record.path.as_bytes().to_vec());
                        if let Some(meta) =
                            object_meta(&record, urls, extension, *ignore_subdirectory)
                        {
                            pending.push((Some(id), meta));
                            filled += 1;
                        }
                        filled < CHUNK
                    })
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                match last {
                    // Resume just past the last key seen, so no file is read
                    // twice and none is skipped.
                    Some(key) if filled >= CHUNK => {
                        let mut next = key;
                        next.push(0);
                        *resume = Some(next);
                    }
                    _ => *done = true,
                }
                Ok(!self.pending.is_empty())
            }
            Cursor::Listed { objects, range } => {
                if range.start >= range.end {
                    return Ok(false);
                }
                let take = (range.end - range.start).min(CHUNK);
                let chunk = &objects[range.start..range.start + take];
                range.start += take;

                // A listed store may still be registered, and its ids are what
                // pruning needs. One batched lookup per chunk; a path the
                // registry has never seen keeps `None` and is never dropped.
                let ids = match &self.pruning {
                    Some(pruning) => {
                        let paths: Vec<String> =
                            chunk.iter().map(|meta| meta.location.to_string()).collect();
                        let borrowed: Vec<&str> = paths.iter().map(String::as_str).collect();
                        pruning
                            .store
                            .registry()
                            .file_ids(&borrowed)
                            .unwrap_or_else(|_| vec![None; chunk.len()])
                    }
                    None => vec![None; chunk.len()],
                };
                self.pending
                    .extend(ids.into_iter().zip(chunk.iter().cloned()));
                Ok(true)
            }
        }
    }

    /// The prune call for the chunk now in `pending`, if there is one to make.
    fn prune_pending(&mut self) -> Option<BoxFuture<'static, Vec<FileId>>> {
        let pruning = self.pruning.clone()?;
        let mut ids: Vec<FileId> = self.pending.iter().filter_map(|(id, _)| *id).collect();
        if ids.is_empty() {
            return None; // nothing here is registered, so nothing is prunable
        }
        // `prune_files` wants them ascending; a path walk is not id order.
        ids.sort_unstable();
        ids.dedup();
        Some(
            async move {
                beacon_file_stats::prune_files(
                    &pruning.store,
                    &pruning.predicate,
                    &pruning.table_schema,
                    &ids,
                )
                .await
            }
            .boxed(),
        )
    }

    /// Move `pending` into the queue, keeping only what `kept` names.
    ///
    /// A file the registry has never seen has no statistics, so it stays: a
    /// partially backfilled store must not lose files. `kept` is ascending, so
    /// membership is a binary search.
    fn accept(&mut self, kept: &[FileId]) {
        let mut dropped = 0usize;
        for (id, meta) in self.pending.drain(..) {
            let keep = match id {
                Some(id) => kept.binary_search(&id).is_ok(),
                None => true,
            };
            if keep {
                self.queue.push_back(meta);
            } else {
                dropped += 1;
            }
        }
        self.pruned.add(dropped);
    }

    /// Move `pending` into the queue unfiltered.
    fn accept_all(&mut self) {
        for (_, meta) in self.pending.drain(..) {
            self.queue.push_back(meta);
        }
    }
}

/// The object metadata for a walked record, when it belongs to this table.
///
/// The shard bounds only say "in this path range"; the glob and the extension
/// still decide, exactly as they do on the listing path. Any URL matching is
/// enough — a table over several paths must not drop a file of one for failing
/// another's glob.
fn object_meta(
    record: &FileRecord,
    urls: &[ListingTableUrl],
    extension: &str,
    ignore_subdirectory: bool,
) -> Option<ObjectMeta> {
    if !record.path.ends_with(extension) {
        return None;
    }
    let meta = meta_of(record)?;
    urls.iter()
        .any(|url| url.contains(&meta.location, ignore_subdirectory))
        .then_some(meta)
}

/// Everything a reader needs to open the file, straight from the record.
///
/// The registry kept exactly these fields to decide whether a file changed, so
/// no `head` request is made — avoiding that per-file round trip is the point.
fn meta_of(record: &FileRecord) -> Option<ObjectMeta> {
    let location = object_store::path::Path::parse(&record.path).ok()?;
    let last_modified = chrono::Utc
        .timestamp_millis_opt(record.last_modified_millis)
        .single()
        .unwrap_or_else(chrono::Utc::now);
    Some(ObjectMeta {
        location,
        last_modified,
        size: record.size,
        e_tag: record.e_tag.clone(),
        version: None,
    })
}

impl Stream for FastObjectStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                StreamState::Done => return Poll::Ready(None),
                StreamState::Idle => {
                    if this.remaining == Some(0) {
                        this.state = StreamState::Done;
                        continue;
                    }
                    if let Some(meta) = this.queue.pop_front() {
                        match this.opener.open(PartitionedFile::from(meta)) {
                            Ok(future) => this.state = StreamState::Opening(future),
                            Err(error) => {
                                this.state = StreamState::Done;
                                return Poll::Ready(Some(Err(error)));
                            }
                        }
                        continue;
                    }
                    // The queue is dry: take the next chunk, and decide it.
                    match this.fill_pending() {
                        Ok(false) => {
                            this.state = StreamState::Done;
                            continue;
                        }
                        Ok(true) => {}
                        Err(error) => {
                            this.state = StreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    this.considered.add(this.pending.len());
                    match this.prune_pending() {
                        Some(future) => this.state = StreamState::Pruning(future),
                        None => this.accept_all(),
                    }
                }
                StreamState::Pruning(future) => match future.as_mut().poll(cx) {
                    Poll::Ready(kept) => {
                        this.accept(&kept);
                        this.state = StreamState::Idle;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Opening(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(stream)) => this.state = StreamState::Reading(stream),
                    Poll::Ready(Err(error)) => {
                        this.state = StreamState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Reading(stream) => match Pin::new(stream).poll_next(cx) {
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
                        this.state = StreamState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Ready(None) => this.state = StreamState::Idle,
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

impl RecordBatchStream for FastObjectStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
