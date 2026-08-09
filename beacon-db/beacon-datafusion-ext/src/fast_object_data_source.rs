//! [`FastObjectDataSource`]: a scan whose partitions are cursors, not file
//! lists.
//!
//! # What this replaces
//!
//! DataFusion's own [`DataSource`] for files is `FileScanConfig`, whose file
//! list is `Vec<FileGroup>` of `Vec<PartitionedFile>` — ~280 bytes plus a path
//! per file, fixed at plan time. At three million files that is over a
//! gigabyte, per plan, per concurrent query, and it is built before the first
//! byte is read. This source holds none of it. A partition is a cursor:
//! a path range of the registry, a slice of surviving ids, or a chunk of an
//! already-listed store. File identities are produced one at a time while the
//! scan runs, and dropped once opened.
//!
//! # The one thing `FileScanConfig` is still needed for
//!
//! `FileSource::create_file_opener(&self, store, base_config: &FileScanConfig,
//! partition)` is DataFusion's trait signature, implemented by every format —
//! DataFusion's own and Beacon's ten. Beacon's read `projected_schema()` from
//! it, Parquet reads `limit`, `preserve_order` and the expression adapter; none
//! reads the file list. There is no other API in this version that turns a
//! format into a [`FileOpener`], so [`opener_config`] builds one at
//! `open()` — with no files in it, from this source's own schema and file
//! source — purely as that call's argument. It is a parameter block, not
//! state: this struct has no `FileScanConfig` field, and nothing here plans
//! through one.
//!
//! Avoiding even that would mean abandoning `FileSource`/`FileOpener` and
//! hand-writing a reader per format, which would drop Parquet row-group and
//! page pruning along with netCDF, HDF5, Zarr, ODV, CSV, IPC, BBF, Atlas,
//! GeoParquet and TIFF support.
//!
//! # Pushdown
//!
//! Projections and filters are delegated straight to the [`FileSource`], which
//! is what owns them — `try_pushdown_projection` and `try_pushdown_filters` on
//! the trait — so a narrow `SELECT` and a `WHERE` still reach the file reader
//! and still drive Parquet's row-group and page pruning inside each file.

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
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSource;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, Partitioning};
use datafusion::datasource::physical_plan::FileSource;
use datafusion::physical_expr::utils::reassign_expr_columns;
use futures::Stream;
use futures::stream::BoxStream;
use object_store::ObjectMeta;

/// Files produced per cursor step. Small enough that a partition's working set
/// stays constant, large enough to amortise a registry read.
const FETCH_CHUNK: usize = 1024;

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
    /// Step through surviving ids, because a predicate had to be evaluated
    /// against named candidates.
    Ids {
        ids: Arc<Vec<FileId>>,
        ranges: Arc<Vec<Range<usize>>>,
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
            Identities::Ids { ranges, .. } | Identities::Listed { ranges, .. } => ranges.len(),
        }
    }

    /// Files this scan will read. Exact once they are named, and the shards'
    /// own count while streaming — what the registry saw at plan time, before
    /// each URL's glob had its say per file.
    pub fn files(&self) -> u64 {
        match self {
            Identities::Shards { shards, .. } => shards.iter().map(|q| q.shard.files).sum(),
            Identities::Ids { ids, .. } => ids.len() as u64,
            Identities::Listed { objects, .. } => objects.len() as u64,
        }
    }

    /// How this scan learns its files, for `EXPLAIN`.
    pub fn mode(&self) -> &'static str {
        match self {
            Identities::Shards { .. } => "streaming",
            Identities::Ids { .. } => "pruned",
            Identities::Listed { .. } => "listed",
        }
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
    limit: Option<usize>,
    statistics: Statistics,
    /// Files the plan considered, and how many a predicate removed.
    files_considered: usize,
    files_pruned: usize,
}

impl FastObjectDataSource {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        file_source: Arc<dyn FileSource>,
        object_store_url: ObjectStoreUrl,
        projected_schema: SchemaRef,
        snapshot: Option<SharedSnapshot>,
        identities: Identities,
        limit: Option<usize>,
        statistics: Statistics,
        files_considered: usize,
        files_pruned: usize,
    ) -> Self {
        Self {
            file_source,
            object_store_url,
            projected_schema,
            snapshot,
            identities,
            limit,
            statistics,
            files_considered,
            files_pruned,
        }
    }

    /// How this scan learns its files. For diagnostics and tests.
    pub fn identities(&self) -> &Identities {
        &self.identities
    }

    pub fn snapshot(&self) -> Option<&SharedSnapshot> {
        self.snapshot.as_ref()
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
            limit: self.limit,
            statistics,
            files_considered: self.files_considered,
            files_pruned: self.files_pruned,
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

/// The argument `FileSource::create_file_opener` demands.
///
/// Empty of files, and built fresh at each `open`. Formats read the projected
/// schema, the limit and the ordering flag from it; none reads a file list.
/// See the module docs for why this exists at all.
fn opener_config(source: &FastObjectDataSource, file_source: Arc<dyn FileSource>) -> FileScanConfig {
    FileScanConfigBuilder::new(source.object_store_url.clone(), file_source)
        .with_limit(source.limit)
        .build()
}

impl fmt::Debug for FastObjectDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastObjectDataSource")
            .field("mode", &self.identities.mode())
            .field("files", &self.identities.files())
            .field("considered", &self.files_considered)
            .field("pruned", &self.files_pruned)
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
        let config = opener_config(self, Arc::clone(&file_source));
        let opener = file_source.create_file_opener(store, &config, partition)?;

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
            Identities::Ids { ids, ranges } => Cursor::Ids {
                ids: Arc::clone(ids),
                range: ranges.get(partition).cloned().ok_or_else(out_of_range)?,
            },
            Identities::Listed { objects, ranges } => Cursor::Listed {
                objects: Arc::clone(objects),
                range: ranges.get(partition).cloned().ok_or_else(out_of_range)?,
            },
        };

        Ok(Box::pin(cooperative(FastObjectStream {
            schema: Arc::clone(&self.projected_schema),
            snapshot: self.snapshot.clone(),
            cursor,
            queue: VecDeque::new(),
            opener,
            state: StreamState::Idle,
            remaining: self.limit,
        })))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        // No file list to print: the plan holds cursors. `files` is what the
        // scan will read and `pruned` what the predicate removed on the way
        // there, and the metrics counters carry the same numbers into
        // `EXPLAIN ANALYZE`.
        write!(
            f,
            "FastObjectScan: file_type={}, mode={}, files={}, pruned={}, partitions={}",
            self.file_source.file_type(),
            self.identities.mode(),
            self.identities.files(),
            self.files_pruned,
            self.identities.partitions(),
        )?;
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
    /// Step through already-decided ids.
    Ids {
        ids: Arc<Vec<FileId>>,
        range: Range<usize>,
    },
    /// Step through objects a listing already reported.
    Listed {
        objects: Arc<Vec<ObjectMeta>>,
        range: Range<usize>,
    },
}

/// One partition's reader: a cursor to file identities to open files to
/// batches, a chunk at a time.
struct FastObjectStream {
    schema: SchemaRef,
    snapshot: Option<SharedSnapshot>,
    cursor: Cursor,
    /// Identities fetched but not yet opened. Never longer than
    /// [`FETCH_CHUNK`].
    queue: VecDeque<ObjectMeta>,
    opener: Arc<dyn FileOpener>,
    state: StreamState,
    /// Rows this partition may still emit under the scan's limit.
    remaining: Option<usize>,
}

enum StreamState {
    /// No file is open; the next identity in the queue is due.
    Idle,
    /// A file is being opened.
    Opening(FileOpenFuture),
    /// A file's batches are being read.
    Reading(BoxStream<'static, Result<RecordBatch>>),
    /// Every file is read, or an error ended the stream.
    Done,
}

impl FastObjectStream {
    /// The next file to open, refilling from the cursor when the queue runs
    /// dry. `Ok(None)` when the partition is exhausted.
    fn next_file(&mut self) -> Result<Option<ObjectMeta>> {
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
                let queue = &mut self.queue;
                let mut last: Option<Vec<u8>> = None;
                let mut filled = 0usize;
                snapshot
                    .for_each_in_shard(prefix, &step, |_, record| {
                        last = Some(record.path.as_bytes().to_vec());
                        if let Some(meta) =
                            object_meta(&record, urls, extension, *ignore_subdirectory)
                        {
                            queue.push_back(meta);
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
                let Some(snapshot) = &self.snapshot else {
                    return Err(DataFusionError::Internal(
                        "an id cursor needs a snapshot".to_string(),
                    ));
                };
                let take = (range.end - range.start).min(FETCH_CHUNK);
                let chunk = &ids[range.start..range.start + take];
                range.start += take;

                let records = snapshot
                    .records_for_ids(chunk)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                for (id, record) in chunk.iter().zip(records) {
                    let record = record.ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "the registry planned file {id} into this scan but no longer holds it"
                        ))
                    })?;
                    let meta = meta_of(&record).ok_or_else(|| {
                        DataFusionError::Internal(format!("file {id} has an unusable path"))
                    })?;
                    self.queue.push_back(meta);
                }
                Ok(())
            }
            Cursor::Listed { objects, range } => {
                if range.start >= range.end {
                    return Ok(());
                }
                let take = (range.end - range.start).min(FETCH_CHUNK);
                self.queue
                    .extend(objects[range.start..range.start + take].iter().cloned());
                range.start += take;
                Ok(())
            }
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
                    let meta = match this.next_file() {
                        Ok(Some(meta)) => meta,
                        Ok(None) => {
                            this.state = StreamState::Done;
                            continue;
                        }
                        Err(error) => {
                            this.state = StreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    };
                    match this.opener.open(PartitionedFile::from(meta)) {
                        Ok(future) => this.state = StreamState::Opening(future),
                        Err(error) => {
                            this.state = StreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
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
