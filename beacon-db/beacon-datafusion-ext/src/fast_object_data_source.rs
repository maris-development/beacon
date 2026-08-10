//! [`FastObjectDataSource`]: a scan that prunes and reads at the same time.
//!
//! # What this replaces
//!
//! DataFusion's own [`DataSource`] for files is `FileScanConfig`, whose file
//! list is `Vec<FileGroup>` of `Vec<PartitionedFile>` — ~280 bytes plus a path
//! per file, fixed at plan time, and built before a byte is read. This source
//! holds the listing's own [`ObjectMeta`]s instead, which are what the store
//! reported and what a reader needs, and turns one into a `PartitionedFile`
//! only at the moment it opens it.
//!
//! # Pruning runs beside the reading, not before it
//!
//! Pruning used to be a plan-time phase: name every candidate, read the
//! segments its predicate columns live in, and hand the survivors to the plan.
//! That blocks the planner on reads, serially, before the query starts.
//!
//! Here each partition takes its files a chunk at a time, and the chunk after
//! the one being read is pruned *while* it is read: the prune is spawned, so it
//! makes progress on another worker rather than waiting to be polled. A
//! partition therefore alternates nothing — it reads continuously, and the next
//! chunk's survivors are usually decided by the time it needs them.
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

use beacon_file_stats::{FileId, FileStatsStore};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::{FileRange, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileGroup, FileOpenFuture, FileOpener, FileScanConfigBuilder, FileSource,
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
use futures::stream::BoxStream;
use futures::Stream;
use object_store::ObjectMeta;
use tokio::task::JoinHandle;

/// Files a partition takes per step, and so the batch one prune call covers.
///
/// Sized against a segment rather than a page: the collector writes ~10 000
/// files per segment, so a chunk this size reads about one segment's block per
/// predicate column. Much smaller and the same block is read over and over;
/// much larger and the first rows wait longer.
const CHUNK: usize = 4096;

/// What one partition reads.
///
/// Whole files, normally — a contiguous slice of the shared listing, which
/// costs two indices however many files it covers. A format that can read part
/// of a file (Parquet, by row group) may have one split across partitions to
/// fill the machine, and those partitions carry the pieces instead. netCDF,
/// HDF5, ODV and TIFF decline splitting in their own `FileSource`, because
/// their readers cannot honour a byte range.
#[derive(Debug, Clone)]
pub enum Partition {
    Whole(Range<usize>),
    /// `(index into the listing, byte range)`, bounded by the partition count.
    Parts(Vec<(usize, FileRange)>),
}

impl Partition {
    fn len(&self) -> usize {
        match self {
            Partition::Whole(range) => range.end.saturating_sub(range.start),
            Partition::Parts(parts) => parts.len(),
        }
    }
}

/// Everything a partition needs to drop files a predicate rules out.
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

/// A file scan that reads a listing and prunes as it goes.
pub struct FastObjectDataSource {
    /// The format's reader, and the owner of projection and filter pushdown.
    file_source: Arc<dyn FileSource>,
    /// Where the files live.
    object_store_url: ObjectStoreUrl,
    /// The scan's output schema, projection applied.
    projected_schema: SchemaRef,
    /// The listing, shared by every partition; a partition reads one range of
    /// it. These are the store's own metadata, not per-file plan objects.
    objects: Arc<Vec<ObjectMeta>>,
    partitions: Arc<Vec<Partition>>,
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
        objects: Arc<Vec<ObjectMeta>>,
        partitions: Arc<Vec<Partition>>,
        pruning: Option<StreamPruning>,
        limit: Option<usize>,
        statistics: Statistics,
    ) -> Self {
        Self {
            file_source,
            object_store_url,
            projected_schema,
            objects,
            partitions,
            pruning,
            limit,
            statistics,
        }
    }

    /// The listing this scan reads. For diagnostics and tests.
    pub fn objects(&self) -> &Arc<Vec<ObjectMeta>> {
        &self.objects
    }

    /// What each partition reads.
    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }

    /// The format's reader. For diagnostics and tests.
    pub fn file_source(&self) -> &Arc<dyn FileSource> {
        &self.file_source
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
            objects: Arc::clone(&self.objects),
            partitions: Arc::clone(&self.partitions),
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
            .field("files", &self.objects.len())
            .field("prunes", &self.prunes())
            .field("partitions", &self.partitions.len())
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

        let assignment = self.partitions.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "fast object scan asked for partition {partition} of {}",
                self.partitions.len()
            ))
        })?;

        // Counted as the scan runs, not as it plans: with pruning beside the
        // reading, this is the only place the numbers exist.
        let metrics = self.file_source.metrics();
        let considered = MetricBuilder::new(metrics).global_counter("file_stats_files_considered");
        let pruned = MetricBuilder::new(metrics).global_counter("file_stats_files_pruned");

        let mut stream = FastObjectStream {
            schema: Arc::clone(&self.projected_schema),
            objects: Arc::clone(&self.objects),
            cursor: Cursor::new(assignment),
            pruning: self.pruning.clone(),
            inflight: None,
            queue: VecDeque::new(),
            opener,
            state: StreamState::Idle,
            remaining: self.limit,
            considered,
            pruned,
        };
        // Start the first chunk now, so its pruning overlaps the plan's own
        // start-up rather than the first poll.
        stream.begin_chunk();

        Ok(Box::pin(cooperative(stream)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        // `files` is what the scan may read; how many a predicate removes is
        // not known until it runs, so `EXPLAIN ANALYZE`'s counters report that.
        write!(
            f,
            "FastObjectScan: file_type={}, files={}, partitions={}",
            self.file_source.file_type(),
            self.objects.len(),
            self.partitions.len(),
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
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    /// Split a file across partitions when there are more partitions than
    /// files and the format can read part of one.
    ///
    /// Without this a single large Parquet file scans on one thread, where
    /// `ListingTable` would have spread its row groups across the machine.
    /// The splitting itself is DataFusion's — `FileGroupPartitioner` decides
    /// the byte ranges, and the result is mapped back onto the shared listing
    /// so the plan still holds indices rather than a file list.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        if self.partitions.len() >= target_partitions {
            return Ok(None);
        }
        // Only reached when files are scarcer than partitions, so building
        // them is bounded by the partition count, not by the collection.
        let by_path: std::collections::HashMap<&object_store::path::Path, usize> = self
            .objects
            .iter()
            .enumerate()
            .map(|(index, meta)| (&meta.location, index))
            .collect();
        let groups: Vec<FileGroup> = self
            .partitions
            .iter()
            .map(|partition| {
                FileGroup::new(match partition {
                    Partition::Whole(range) => self.objects[range.clone()]
                        .iter()
                        .cloned()
                        .map(PartitionedFile::from)
                        .collect(),
                    Partition::Parts(parts) => parts
                        .iter()
                        .map(|(index, range)| {
                            PartitionedFile::from(self.objects[*index].clone())
                                .with_range(range.start, range.end)
                        })
                        .collect(),
                })
            })
            .collect();

        // The *format* decides whether its files may be split, and how. Asking
        // it is the whole safety of this: netCDF, HDF5, ODV and TIFF answer
        // `None` because their readers ignore a byte range, and splitting one
        // of those would have every partition read the whole file and return
        // its rows again. `supports_repartitioning()` is not that answer — it
        // defaults to true, including for those formats.
        let probe = FileScanConfigBuilder::new(
            self.object_store_url.clone(),
            Arc::clone(&self.file_source),
        )
        .with_file_groups(groups)
        .build();
        let Some(split) = self.file_source.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            &probe,
        )?
        else {
            return Ok(None);
        };
        let split = split.file_groups;

        let mut partitions = Vec::with_capacity(split.len());
        for group in &split {
            let mut parts = Vec::with_capacity(group.len());
            for file in group.iter() {
                let Some(index) = by_path.get(&file.object_meta.location) else {
                    // A file the splitter produced that is not in the listing
                    // cannot be mapped back; leave the plan as it is.
                    return Ok(None);
                };
                let range = file.range.clone().unwrap_or(FileRange {
                    start: 0,
                    end: file.object_meta.size as i64,
                });
                parts.push((*index, range));
            }
            partitions.push(Partition::Parts(parts));
        }

        let mut next = self.with_file_source(Arc::clone(&self.file_source))?;
        next.partitions = Arc::new(partitions);
        Ok(Some(Arc::new(next)))
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
        // the listing. This is how a narrow `SELECT` reaches the file reader.
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

/// One file a partition will read: the store's metadata, and which part of it
/// when the file was split across partitions.
type Piece = (ObjectMeta, Option<FileRange>);

/// A chunk of the partition's files, on its way to being read.
enum Chunk {
    /// Nothing to prune: these are ready to open.
    Ready(Vec<Piece>),
    /// Being decided on another worker, alongside the reading of the chunk
    /// before it.
    Pruning {
        considered: usize,
        handle: JoinHandle<Vec<Piece>>,
    },
}

/// Where a partition's next files come from.
enum Cursor {
    /// Whole files: an index range into the shared listing.
    Whole(Range<usize>),
    /// Parts of files, already decided. `next` is how far it has read.
    Parts { parts: Vec<(usize, FileRange)>, next: usize },
}

impl Cursor {
    fn new(partition: Partition) -> Self {
        match partition {
            Partition::Whole(range) => Cursor::Whole(range),
            Partition::Parts(parts) => Cursor::Parts { parts, next: 0 },
        }
    }

    fn remaining(&self) -> usize {
        match self {
            Cursor::Whole(range) => range.end.saturating_sub(range.start),
            Cursor::Parts { parts, next } => parts.len() - next,
        }
    }

    /// Take up to `CHUNK` files, resolving each against the listing.
    fn take(&mut self, objects: &[ObjectMeta]) -> Vec<Piece> {
        let take = self.remaining().min(CHUNK);
        match self {
            Cursor::Whole(range) => {
                let start = range.start;
                range.start += take;
                objects[start..start + take]
                    .iter()
                    .cloned()
                    .map(|meta| (meta, None))
                    .collect()
            }
            Cursor::Parts { parts, next } => {
                let start = *next;
                *next += take;
                parts[start..start + take]
                    .iter()
                    .map(|(index, range)| (objects[*index].clone(), Some(range.clone())))
                    .collect()
            }
        }
    }
}

/// One partition's reader: a range of the listing, pruned a chunk ahead and
/// read a file at a time.
struct FastObjectStream {
    schema: SchemaRef,
    objects: Arc<Vec<ObjectMeta>>,
    /// The part of this partition's work not taken yet.
    cursor: Cursor,
    pruning: Option<StreamPruning>,
    /// The chunk after the one being read.
    inflight: Option<Chunk>,
    /// Files cleared to open.
    queue: VecDeque<Piece>,
    opener: Arc<dyn FileOpener>,
    state: StreamState,
    /// Rows this partition may still emit under the scan's limit.
    remaining: Option<usize>,
    considered: Count,
    pruned: Count,
}

enum StreamState {
    /// No file is open; the next one in the queue is due.
    Idle,
    /// A file is being opened.
    Opening(FileOpenFuture),
    /// A file's batches are being read, while the next one opens alongside it.
    Reading {
        reader: BoxStream<'static, Result<RecordBatch>>,
        next: Option<NextOpen>,
    },
    /// Every file is read, or an error ended the stream.
    Done,
}

/// The file after the one being read.
///
/// Opening it costs a round trip, so it starts while the current file is still
/// being scanned and is waiting by the time it is due — the same overlap
/// DataFusion's own `FileStream` performs.
enum NextOpen {
    Pending(FileOpenFuture),
    Ready(Result<BoxStream<'static, Result<RecordBatch>>>),
}

/// The file identity the opener reads, built at the moment it is opened and
/// dropped after.
fn partitioned_file((meta, range): Piece) -> PartitionedFile {
    let file = PartitionedFile::from(meta);
    match range {
        Some(range) => file.with_range(range.start, range.end),
        None => file,
    }
}

/// Drop the files in `chunk` whose recorded ranges say they cannot match.
///
/// Runs on its own task, so it overlaps the reading of the previous chunk.
/// A path the registry has never seen has no statistics and is kept: a
/// partially analyzed store must not lose files.
async fn prune_chunk(pruning: StreamPruning, chunk: Vec<Piece>) -> Vec<Piece> {
    let paths: Vec<String> = chunk.iter().map(|(m, _)| m.location.to_string()).collect();
    let borrowed: Vec<&str> = paths.iter().map(String::as_str).collect();
    let Ok(ids) = pruning.store.registry().file_ids(&borrowed) else {
        return chunk;
    };

    let mut candidates: Vec<FileId> = ids.iter().filter_map(|id| *id).collect();
    if candidates.is_empty() {
        return chunk; // nothing here is analyzed, so nothing is prunable
    }
    // `prune_files` wants them ascending, and answers ascending.
    candidates.sort_unstable();
    candidates.dedup();

    let kept = beacon_file_stats::prune_files(
        &pruning.store,
        &pruning.predicate,
        &pruning.table_schema,
        &candidates,
    )
    .await;

    chunk
        .into_iter()
        .zip(ids)
        .filter(|(_, id)| match id {
            Some(id) => kept.binary_search(id).is_ok(),
            None => true,
        })
        .map(|(piece, _)| piece)
        .collect()
}

impl FastObjectStream {
    /// Take the next chunk off the cursor and start deciding it.
    ///
    /// Called as soon as the previous chunk lands, so the deciding happens
    /// while that one is read.
    fn begin_chunk(&mut self) {
        if self.inflight.is_some() || self.cursor.remaining() == 0 {
            return;
        }
        let chunk = self.cursor.take(&self.objects);

        self.inflight = Some(match self.pruning.clone() {
            Some(pruning) => Chunk::Pruning {
                considered: chunk.len(),
                handle: tokio::spawn(prune_chunk(pruning, chunk)),
            },
            None => Chunk::Ready(chunk),
        });
    }

    /// Move a decided chunk into the queue and start the next one.
    fn accept(&mut self, kept: Vec<Piece>, considered: usize) {
        self.considered.add(considered);
        self.pruned.add(considered - kept.len());
        self.queue.extend(kept);
        self.begin_chunk();
    }

    /// Begin opening the next queued file, if there is one.
    fn begin_next_open(&mut self) -> Option<NextOpen> {
        let piece = self.queue.pop_front()?;
        match self.opener.open(partitioned_file(piece)) {
            Ok(future) => Some(NextOpen::Pending(future)),
            Err(error) => Some(NextOpen::Ready(Err(error))),
        }
    }
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
                    if let Some(piece) = this.queue.pop_front() {
                        match this.opener.open(partitioned_file(piece)) {
                            Ok(future) => this.state = StreamState::Opening(future),
                            Err(error) => {
                                this.state = StreamState::Done;
                                return Poll::Ready(Some(Err(error)));
                            }
                        }
                        continue;
                    }
                    // The queue is dry, so the next chunk is due. It has been
                    // deciding since the last one landed.
                    match this.inflight.take() {
                        Some(Chunk::Ready(chunk)) => {
                            let considered = chunk.len();
                            this.accept(chunk, considered);
                        }
                        Some(Chunk::Pruning {
                            considered,
                            mut handle,
                        }) => match Pin::new(&mut handle).poll(cx) {
                            Poll::Ready(Ok(kept)) => this.accept(kept, considered),
                            Poll::Ready(Err(error)) => {
                                this.state = StreamState::Done;
                                return Poll::Ready(Some(Err(DataFusionError::Execution(
                                    format!("a file-statistics prune task failed: {error}"),
                                ))));
                            }
                            Poll::Pending => {
                                this.inflight = Some(Chunk::Pruning { considered, handle });
                                return Poll::Pending;
                            }
                        },
                        None => {
                            this.state = StreamState::Done;
                        }
                    }
                }
                StreamState::Opening(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(reader)) => {
                        // The next file starts opening now, so its round trip
                        // overlaps this one's scan.
                        let next = this.begin_next_open();
                        this.state = StreamState::Reading { reader, next };
                    }
                    Poll::Ready(Err(error)) => {
                        this.state = StreamState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Reading { reader, next } => {
                    // Drive the next file's open forward, so it is ready — or
                    // nearly — by the time this reader runs out.
                    if let Some(NextOpen::Pending(future)) = next
                        && let Poll::Ready(opened) = Pin::new(future).poll(cx)
                    {
                        *next = Some(NextOpen::Ready(opened));
                    }

                    match Pin::new(reader).poll_next(cx) {
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
                        Poll::Ready(None) => {
                            this.state = match next.take() {
                                Some(NextOpen::Ready(Ok(reader))) => {
                                    let next = this.begin_next_open();
                                    StreamState::Reading { reader, next }
                                }
                                Some(NextOpen::Ready(Err(error))) => {
                                    this.state = StreamState::Done;
                                    return Poll::Ready(Some(Err(error)));
                                }
                                Some(NextOpen::Pending(future)) => StreamState::Opening(future),
                                // Chunk boundary: the next chunk is due.
                                None => StreamState::Idle,
                            };
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for FastObjectStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
