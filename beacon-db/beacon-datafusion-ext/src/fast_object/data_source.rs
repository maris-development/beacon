//! [`FastObjectDataSource`]: the scan node itself.
//!
//! Holds the listing and the plan-time decisions, and hands each partition a
//! [`FastObjectStream`] over the shared [`Ready`]. See the [module docs](super).

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::sync::{Arc, Weak};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, FileSource};
use datafusion::datasource::source::DataSource;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::utils::reassign_expr_columns;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::{DisplayFormatType, Partitioning};
use futures::future::BoxFuture;
use object_store::ObjectMeta;
use parking_lot::Mutex;

use super::plan::{Ready, Shared, StreamPruning, Work, prune_all};
use super::stream::{FastObjectStream, partitioned_file};

pub struct FastObjectDataSource {
    /// The format's reader, and the owner of projection and filter pushdown.
    file_source: Arc<dyn FileSource>,
    /// Where the files live.
    object_store_url: ObjectStoreUrl,
    /// The scan's output schema, projection applied.
    projected_schema: SchemaRef,
    /// The listing, shared by every partition. These are the store's own
    /// metadata, not per-file plan objects.
    objects: Arc<Vec<ObjectMeta>>,
    /// What [`DataSource::output_partitioning`] reports.
    partitions: usize,
    /// The pieces a split produced. `None` means whole files, in listing order.
    split: Option<Arc<Vec<Work>>>,
    /// Present when a predicate can be answered from stored statistics.
    pruning: Option<StreamPruning>,
    limit: Option<usize>,
    statistics: Statistics,
    /// The shared state of the execution in progress. See [`Self::open`].
    live: Mutex<Option<Live>>,
}

impl FastObjectDataSource {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        file_source: Arc<dyn FileSource>,
        object_store_url: ObjectStoreUrl,
        projected_schema: SchemaRef,
        objects: Arc<Vec<ObjectMeta>>,
        target_partitions: usize,
        pruning: Option<StreamPruning>,
        limit: Option<usize>,
        statistics: Statistics,
    ) -> Self {
        // A partition with no file to read is a thread that reports empty, so
        // the budget is capped by the listing. One is the floor: a scan always
        // has an output partition, even over nothing.
        let partitions = objects.len().min(target_partitions).max(1);
        Self {
            file_source,
            object_store_url,
            projected_schema,
            objects,
            partitions,
            split: None,
            pruning,
            limit,
            statistics,
            live: Mutex::new(None),
        }
    }

    /// The listing this scan reads. For diagnostics and tests.
    pub fn objects(&self) -> &Arc<Vec<ObjectMeta>> {
        &self.objects
    }

    /// The pieces a split produced, or `None` when files are read whole.
    /// For diagnostics and tests.
    pub fn split(&self) -> Option<&[Work]> {
        self.split.as_deref().map(Vec::as_slice)
    }

    /// How many output partitions this scan reports. For diagnostics and tests.
    pub fn partition_count(&self) -> usize {
        self.partitions
    }

    /// The format's reader. For diagnostics and tests.
    pub fn file_source(&self) -> &Arc<dyn FileSource> {
        &self.file_source
    }

    /// Whether a predicate is applied while this scan reads.
    pub fn prunes(&self) -> bool {
        self.pruning.is_some()
    }

    /// How many units of work this scan hands out.
    fn work_len(&self) -> usize {
        self.split.as_ref().map_or(self.objects.len(), |s| s.len())
    }

    /// The `index`th unit of work.
    fn work_at(&self, index: usize) -> Work {
        match &self.split {
            Some(split) => split[index].clone(),
            None => Work::Whole(index),
        }
    }

    /// Adopt a rewritten file source, re-deriving the output schema from it.
    ///
    /// A projection pushdown changes what the scan emits, so the schema and
    /// the column shape of the statistics are re-derived rather than copied.
    /// The live execution is *not* carried over: a rewritten source is a
    /// different plan node and starts its own.
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
            partitions: self.partitions,
            split: self.split.clone(),
            pruning: self.pruning.clone(),
            limit: self.limit,
            statistics,
            live: Mutex::new(None),
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
            .field("partitions", &self.partitions)
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
        if partition >= self.partitions {
            return Err(DataFusionError::Internal(format!(
                "fast object scan asked for partition {partition} of {}",
                self.partitions
            )));
        }

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

        let shared = self.shared_for(&context);

        // Counted as the scan runs, not as it plans: with pruning inside the
        // pipeline, this is the only place the numbers exist. Global counters,
        // and the shared prune adds to them once for the whole scan.
        let metrics = self.file_source.metrics();
        let considered = MetricBuilder::new(metrics).global_counter("file_stats_files_considered");
        let pruned = MetricBuilder::new(metrics).global_counter("file_stats_files_pruned");

        let objects = Arc::clone(&self.objects);
        let split = self.split.clone();
        let pruning = self.pruning.clone();
        let partitions = self.partitions;
        let limited = self.limit.is_some();
        let prepare: BoxFuture<'static, Arc<Ready>> = Box::pin(async move {
            Arc::clone(
                shared
                    .ready
                    .get_or_init(|| {
                        prune_all(
                            objects, split, pruning, partitions, limited, considered, pruned,
                        )
                    })
                    .await,
            )
        });

        Ok(Box::pin(cooperative(FastObjectStream::new(
            Arc::clone(&self.projected_schema),
            Arc::clone(&self.objects),
            partition,
            opener,
            self.limit,
            prepare,
        ))))
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
            self.partitions,
        )?;
        // Only when a split made work items and files different things.
        if let Some(split) = &self.split {
            write!(f, ", split={}", split.len())?;
        }
        if self.prunes() {
            write!(f, ", prune=stream")?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        self.file_source.fmt_extra(t, f)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions)
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
        let items = self.work_len();
        if items >= target_partitions {
            return Ok(None);
        }
        // Only reached when work items are scarcer than partitions, so building
        // them is bounded by the partition count, not by the collection.
        let by_path: HashMap<&object_store::path::Path, usize> = self
            .objects
            .iter()
            .enumerate()
            .map(|(index, meta)| (&meta.location, index))
            .collect();
        // One group per item, which leaves the partitioner free to divide them
        // however it likes.
        let groups: Vec<FileGroup> = (0..items)
            .map(|index| {
                FileGroup::new(vec![partitioned_file(&self.objects, &self.work_at(index))])
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

        let mut work = Vec::new();
        for group in &split.file_groups {
            for file in group.iter() {
                let Some(index) = by_path.get(&file.object_meta.location) else {
                    // A file the splitter produced that is not in the listing
                    // cannot be mapped back; leave the plan as it is.
                    return Ok(None);
                };
                work.push(match file.range.clone() {
                    Some(range) => Work::Part(*index, range),
                    None => Work::Whole(*index),
                });
            }
        }

        let mut next = self.with_file_source(Arc::clone(&self.file_source))?;
        next.partitions = work.len().min(target_partitions).max(1);
        next.split = Some(Arc::new(work));
        Ok(Some(Arc::new(next)))
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        // No ordering is claimed, and none may be. A shared queue cannot
        // honour one, and `with_preserve_order` and `try_pushdown_sort` are
        // left unimplemented for the same reason.
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

struct Live {
    ctx: Weak<TaskContext>,
    shared: Arc<Shared>,
    opened: usize,
}

impl FastObjectDataSource {
    /// The shared state for `context`, building it when this is a new
    /// execution.
    ///
    /// `open` takes `&self` behind an `Arc<dyn DataSource>`, so the state is
    /// built lazily here rather than at plan time. It must not outlive one
    /// execution: a drained queue handed to a second run returns no rows.
    ///
    /// DataFusion clones one `Arc<TaskContext>` to every partition of an
    /// execution and makes a fresh one for the next, so pointer identity says
    /// which execution a partition belongs to. Holding a `Weak` pins the
    /// allocation, so no later context can be built at the same address and the
    /// comparison cannot alias.
    ///
    /// The `opened` count is a second fuse, for a caller that reuses one
    /// context for two `collect` calls. Pointer identity alone would hand the
    /// second run a drained queue.
    fn shared_for(&self, context: &Arc<TaskContext>) -> Arc<Shared> {
        let mut live = self.live.lock();
        let stale = match live.as_ref() {
            None => true,
            Some(previous) => {
                let same = std::ptr::eq(previous.ctx.as_ptr(), Arc::as_ptr(context));
                if same && previous.opened < self.partitions {
                    // Replacing an execution that never opened every partition
                    // means two of them are reading the same queue. Say so:
                    // the rows are wrong, not just slow.
                    tracing::warn!(
                        opened = previous.opened,
                        partitions = self.partitions,
                        "a fast object scan started again before its previous run opened every partition"
                    );
                }
                !same || previous.opened >= self.partitions
            }
        };
        if stale {
            *live = Some(Live {
                ctx: Arc::downgrade(context),
                shared: Arc::default(),
                opened: 0,
            });
        }
        let current = live.as_mut().expect("a live execution was just set");
        current.opened += 1;
        Arc::clone(&current.shared)
    }
}
