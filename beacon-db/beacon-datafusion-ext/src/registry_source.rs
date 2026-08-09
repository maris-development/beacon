//! A [`DataSource`] whose file list is file ids in the statistics registry,
//! not materialised `PartitionedFile`s.
//!
//! # Why not `FileScanConfig`'s file list
//!
//! A `PartitionedFile` costs ~280 bytes plus its path, so a plan over three
//! million files holds more than a gigabyte of file list — per plan, per
//! concurrent query. The registry already holds every one of those fields,
//! indexed by an 8-byte [`FileId`]. This source therefore keeps only the ids:
//! one shared, path-ordered vector of survivors, and per partition a range of
//! indexes into it. At execute time each partition turns a small chunk of ids
//! back into records under one redb read, builds each file's identity just
//! long enough to open it, and lets it go. The file list never exists as
//! objects all at once.
//!
//! # What is still a `FileScanConfig`
//!
//! The scan's *configuration* — schema, projection, pushed-down predicate,
//! limit, object-store URL — lives in a `FileScanConfig` with an **empty**
//! file list. That is deliberate reuse rather than contradiction: every
//! format's [`FileOpener`] is created from such a config, and DataFusion's
//! physical optimizer pushes projections and filters into sources by rewriting
//! it. Delegating those rewrites to the inner config is what keeps Parquet
//! row-group and page pruning working inside each file while the list of files
//! stays ids. The config's per-file machinery is simply never fed a file.
//!
//! # What `EXPLAIN` shows instead of a file list
//!
//! `RegistryScanExec: files=N pruned=M partitions=K`, and the
//! `file_stats_files_listed` / `file_stats_files_pruned` counters under the
//! node in `EXPLAIN ANALYZE`. With no materialised list there is no list to
//! print, so the counts are the evidence.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use beacon_file_stats::{FileId, FileRecord, Registry};
use chrono::TimeZone;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener, FileScanConfig};
use datafusion::datasource::source::DataSource;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, Partitioning};
use futures::Stream;
use futures::stream::BoxStream;
use object_store::ObjectMeta;

/// Records fetched per registry read at execute time. Small enough that a
/// partition's working set stays constant, large enough to amortise the read
/// transaction.
const RECORD_FETCH_CHUNK: usize = 1024;

/// A scan source planned from the registry: ids, not files.
pub struct RegistryScanSource {
    /// Schema, projection, predicate, limit and store URL — with an empty file
    /// list. Openers are created from this, and optimizer pushdowns rewrite
    /// it; see the module docs for why this reuse is safe.
    base: FileScanConfig,
    /// The scan's output schema, projection applied.
    projected_schema: SchemaRef,
    registry: Arc<Registry>,
    /// Surviving file ids in path order, shared by every partition.
    ids: Arc<Vec<FileId>>,
    /// One contiguous index range into `ids` per partition, sharded on
    /// cumulative file size.
    partitions: Arc<Vec<Range<usize>>>,
    /// Aggregate plan-time statistics, in output-schema terms.
    statistics: Statistics,
    files_listed: usize,
    files_pruned: usize,
}

impl RegistryScanSource {
    pub fn new(
        base: FileScanConfig,
        projected_schema: SchemaRef,
        registry: Arc<Registry>,
        ids: Vec<FileId>,
        partitions: Vec<Range<usize>>,
        statistics: Statistics,
        files_listed: usize,
        files_pruned: usize,
    ) -> Self {
        Self {
            base,
            projected_schema,
            registry,
            ids: Arc::new(ids),
            partitions: Arc::new(partitions),
            statistics,
            files_listed,
            files_pruned,
        }
    }

    /// The surviving file ids, path-ordered. For diagnostics and tests; the
    /// paths themselves live in the registry.
    pub fn file_ids(&self) -> &Arc<Vec<FileId>> {
        &self.ids
    }

    pub fn partitions(&self) -> &[Range<usize>] {
        &self.partitions
    }

    /// Rebuild around a rewritten configuration, keeping the file ids.
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
            registry: Arc::clone(&self.registry),
            ids: Arc::clone(&self.ids),
            partitions: Arc::clone(&self.partitions),
            statistics,
            files_listed: self.files_listed,
            files_pruned: self.files_pruned,
        }
    }
}

impl fmt::Debug for RegistryScanSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryScanSource")
            .field("files", &self.ids.len())
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
        let range = self.partitions.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "registry scan asked for partition {partition} of {}",
                self.partitions.len()
            ))
        })?;
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

        Ok(Box::pin(cooperative(RegistryFileStream {
            schema: Arc::clone(&self.projected_schema),
            registry: Arc::clone(&self.registry),
            ids: Arc::clone(&self.ids),
            range,
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
        write!(
            f,
            "RegistryScanExec: file_type={}, files={}, pruned={}, partitions={}",
            self.base.file_source.file_type(),
            self.ids.len(),
            self.files_pruned,
            self.partitions.len(),
        )?;
        if let Some(limit) = self.base.limit {
            write!(f, ", limit={limit}")?;
        }
        self.base.file_source.fmt_extra(t, f)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn scheduling_type(&self) -> datafusion::physical_plan::execution_plan::SchedulingType {
        // The stream is wrapped in `cooperative` at open, like the listing
        // scan's, so long files yield to their peers.
        datafusion::physical_plan::execution_plan::SchedulingType::Cooperative
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(Arc::clone(&self.projected_schema))
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
        let base = datafusion::datasource::physical_plan::FileScanConfigBuilder::from(
            self.base.clone(),
        )
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
        // keep the ids. This is how a narrow `SELECT` reaches the file reader.
        match DataSource::try_swapping_with_projection(&self.base, projection)? {
            Some(rewritten) => {
                let base = rewritten
                    .as_any()
                    .downcast_ref::<FileScanConfig>()
                    .expect("FileScanConfig rewrites into FileScanConfig")
                    .clone();
                Ok(Some(Arc::new(self.with_base(base))))
            }
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
            updated_node: match result.updated_node {
                Some(rewritten) => {
                    let base = rewritten
                        .as_any()
                        .downcast_ref::<FileScanConfig>()
                        .expect("FileScanConfig rewrites into FileScanConfig")
                        .clone();
                    Some(Arc::new(self.with_base(base)) as Arc<dyn DataSource>)
                }
                None => None,
            },
        })
    }
}

/// One partition's reader: ids to records to open files to batches, a chunk
/// of records at a time.
struct RegistryFileStream {
    schema: SchemaRef,
    registry: Arc<Registry>,
    ids: Arc<Vec<FileId>>,
    /// Indexes into `ids` not yet fetched into `queue`.
    range: Range<usize>,
    /// Records fetched but not yet opened. Never longer than
    /// [`RECORD_FETCH_CHUNK`].
    queue: VecDeque<FileRecord>,
    opener: Arc<dyn FileOpener>,
    state: FileStreamState,
    /// Rows this partition may still emit under the scan's limit.
    remaining: Option<usize>,
}

enum FileStreamState {
    /// No file is open; the next record in the queue is due.
    Idle,
    /// A file is being opened.
    Opening(FileOpenFuture),
    /// A file's batches are being read.
    Reading(BoxStream<'static, Result<RecordBatch>>),
    /// Every file is read, or an error ended the stream.
    Done,
}

impl RegistryFileStream {
    /// The next record to open, fetching the next chunk of ids when the queue
    /// runs dry. `Ok(None)` when the partition is exhausted.
    fn next_record(&mut self) -> Result<Option<FileRecord>> {
        if self.queue.is_empty() && !self.range.is_empty() {
            let take = self.range.len().min(RECORD_FETCH_CHUNK);
            let chunk = &self.ids[self.range.start..self.range.start + take];
            self.range.start += take;

            let records = self
                .registry
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
        }
        Ok(self.queue.pop_front())
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
                    let record = match this.next_record() {
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
                    let opened = partitioned_file(&record)
                        .and_then(|file| this.opener.open(file));
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
