//! DataFusion [`FileSource`]/[`FileOpener`] for HDF5 files on the Rust reader.
//!
//! The opener builds an [`AnyDataset`](beacon_nd_array::dataset::AnyDataset)
//! for the (projected) columns and streams it through the shared ND engine,
//! which handles predicate pushdown (chunk pruning + row masking) via
//! [`PushdownFilter`].
//!
//! This mirrors the netCDF source. The difference is where the bytes come from:
//! this one always reads through the scan's own object store, because its
//! reader needs no local file.

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::{
    arrow::{metrics::DatasetReadMetrics, share::SharedDataset},
    projection::DatasetProjection,
};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    error::DataFusionError,
    physical_expr::{conjunction, projection::ProjectionExprs, PhysicalExpr},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{stream::BoxStream, FutureExt};
use object_store::{ObjectMeta, ObjectStore};

use std::collections::HashMap;

use crate::cache::Hdf5ReaderCache;

/// The shares of one scan, keyed by object path.
///
/// Built by [`Hdf5Source::repartitioned`] and cloned into every opener, so the
/// partitions of a file find each other. A scan builds its own source, so this
/// does not outlive the plan.
type FileShares =
    Arc<HashMap<object_store::path::Path, Arc<tokio::sync::OnceCell<Arc<SharedDataset>>>>>;

/// The smallest file worth sharing across partitions.
///
/// Every share of a file opens that file. The reader cache turns the repeat
/// opens into hits, but each share still derives the schema, resolves the
/// projection and builds the chunk list before it reads a byte, and it holds a
/// partition open for as long as it runs. Below this, that setup costs more than
/// the parallelism returns.
///
/// This replaces DataFusion's `repartition_file_min_size` rather than deferring
/// to it. That default (10 MB) is a parquet number, where a byte range is a run
/// of row groups the reader seeks straight to; an HDF5 file is shared as a chunk
/// list that has to be built first, so the two are not measuring the same cost.
///
/// The test is on one file, not on the scan total. A share pays to open a file,
/// so a file is what has to be large enough to earn it: a collection of small
/// files still scans in parallel, one file per partition, but none of them is
/// opened by every partition for a fraction of its rows.
pub const MIN_SPLIT_SIZE: u64 = 8 * 1024 * 1024;

/// DataFusion [`FileSource`] for HDF5 (`.h5`/`.hdf5`) files.
#[derive(Debug, Clone)]
pub struct Hdf5Source {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Reader cache to consult for this scan. `None` disables caching.
    cache: Option<Hdf5ReaderCache>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// The shares of this scan. Cloned, not copied, so every partition of a file
    /// reaches the same one.
    partitions_shared_map: FileShares,
}

impl Hdf5Source {
    pub fn new(read_dimensions: Option<Vec<String>>, table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            read_dimensions,
            batch_size: usize::MAX,
            predicate: None,
            cache: None,
            projection: None,
            partitions_shared_map: Arc::new(HashMap::new()),
        }
    }

    /// Returns a copy of this source that consults `cache` (when `Some`) for
    /// opened datasets. The format wires in the runtime's shared cache here.
    pub fn with_cache(mut self, cache: Option<Hdf5ReaderCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }
}

impl FileSource for Hdf5Source {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(Hdf5Opener::new(
            projected_schema,
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            self.cache.clone(),
            self.execution_plan_metrics.clone(),
            partition,
            object_store,
            self.partitions_shared_map.clone(),
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    /// Whether a scan may split one file across partitions. It may.
    ///
    /// This source is the `oxcdf` reader's, and only ever that one. A table on
    /// netcdf-c never reaches here: `Hdf5FormatFactory` hands the whole call to
    /// the netCDF factory when `use_rust_reader` is off, so it is served by
    /// `NetCDFSource`, which declines the split because every netcdf-c call
    /// queues on one process-global mutex. `Hdf5Format` has private fields and
    /// one construction site, behind that same check, so the invariant is
    /// structural rather than a convention. `only_the_rust_reader_splits_one_file`
    /// in `tests/backend_parity.rs` holds it to that.
    ///
    /// `oxcdf` range-reads through the object store and holds no lock, so the
    /// partitions of one file run at the same time. Nothing is divided by byte
    /// range: they take chunks from one shared queue, so no two of them read the
    /// same chunk. See [`beacon_nd_array::arrow::share`].
    fn supports_repartitioning(&self) -> bool {
        true
    }

    /// Give every partition the files that are worth dividing, and one each of
    /// the files that are not.
    ///
    /// A file over [`MIN_SPLIT_SIZE`] goes into every partition's group and gets
    /// a cell in `partitions_shared_map`. Nothing about it is divided here: the
    /// partitions divide it as they read it, by taking chunks from the one queue
    /// behind that cell. Balance then follows completion rather than a guess
    /// made at plan time, which matters most under a predicate: an nd chunk list
    /// is C-ordered, so `WHERE time > …` prunes a prefix of it, and a deal made
    /// at plan time would leave the early partitions idle.
    ///
    /// A smaller file is left whole and dealt to one partition. Every partition
    /// opening it to take a chunk or two would cost more than it returns, and
    /// the listing has already spread these across the scan.
    ///
    /// `repartition_file_min_size` is DataFusion's parquet-shaped default and is
    /// not used; see [`MIN_SPLIT_SIZE`].
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if output_ordering.is_some() || target_partitions <= 1 {
            // An ordered scan cannot share: a partition holding an arbitrary
            // subset of a file cannot emit its rows in file order.
            return Ok(None);
        }

        Ok(beacon_datafusion_ext::file_groups::shared_file_groups(
            &config.file_groups,
            target_partitions,
            MIN_SPLIT_SIZE,
        )
        .map(|deal| {
            // One cell per shared file. Every partition that holds the file
            // reaches the same cell, so the first to arrive builds the read and
            // the rest attach to what it built.
            let shares = deal
                .shared
                .into_iter()
                .map(|path| (path, Arc::new(tokio::sync::OnceCell::new())))
                .collect();

            let mut config = config.clone();
            config.file_groups = deal.file_groups;
            // The openers are built from the config's source, so the map has to
            // travel with it.
            config.file_source = Arc::new(Self {
                partitions_shared_map: Arc::new(shares),
                ..self.clone()
            });
            config
        }))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "hdf5"
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> datafusion::error::Result<Option<Arc<dyn FileSource>>> {
        let merged = match &self.projection {
            Some(existing) => existing.try_merge(projection)?,
            None => projection.clone(),
        };
        let source = Self {
            projection: Some(merged),
            ..self.clone()
        };
        Ok(Some(Arc::new(source)))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let predicate = match self.predicate.clone() {
            Some(existing) => conjunction(std::iter::once(existing).chain(filters.clone())),
            None => conjunction(filters.clone()),
        };

        let source = Self {
            predicate: Some(predicate),
            ..self.clone()
        };

        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }
}

// ─── FileOpener ──────────────────────────────────────────────────────────────

/// Opens a single HDF5 file and streams its contents as ND-encoded Arrow
/// [`RecordBatch`]es.
struct Hdf5Opener {
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    cache: Option<Hdf5ReaderCache>,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    /// The store the scan lists from. The reader reads its byte ranges through
    /// it, so s3, gs and az work with no local copy.
    object_store: Arc<dyn ObjectStore>,
    /// The shares of this scan, so the partitions of a file find each other.
    partition_shares: FileShares,
}

impl Hdf5Opener {
    #[allow(clippy::too_many_arguments)]
    fn new(
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Option<Hdf5ReaderCache>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        object_store: Arc<dyn ObjectStore>,
        partition_shares: FileShares,
    ) -> Self {
        Self {
            projected_schema,
            read_dimensions,
            batch_size,
            predicate,
            cache,
            metrics,
            partition,
            object_store,
            partition_shares,
        }
    }

    /// Read one file, through its share when it has one.
    ///
    /// A shared file is opened and planned by whichever partition arrives first;
    /// the rest attach to what it built and pull from the same queue. An
    /// unshared file is planned by the one partition that holds it. Either way
    /// the reading is the same, so there is one path below the plan.
    ///
    /// Every input to the plan has to be identical in every partition of one
    /// file, or the partitions would not be reading the same file the same way.
    /// They are: the dataset, `projected_schema`, `batch_size` and `predicate`.
    /// A scan takes all four from one place, so they are. Keep it that way.
    #[allow(clippy::too_many_arguments)]
    async fn read(
        share: Option<Arc<tokio::sync::OnceCell<Arc<SharedDataset>>>>,
        store: Arc<dyn ObjectStore>,
        object: ObjectMeta,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        cache: Option<Hdf5ReaderCache>,
        _metrics: Option<DatasetReadMetrics>, // TODO: record the read metrics for the dataset, not just for the partition that built it.
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let plan = async move || {
            let dataset = Self::open_dataset(store, object, cache, read_dimensions).await?;
            SharedDataset::plan(dataset, projected_schema, batch_size, predicate).await
        };

        // The first partition to arrive opens the file and fills its queue. The
        // rest wait for it, then draw from that same queue.
        let dataset = match share {
            Some(cell) => cell
                .get_or_try_init::<DataFusionError, _, _>(plan)
                .await?
                .clone(),
            None => plan().await?,
        };

        Ok(dataset.stream())
    }

    /// Open the file and narrow it to the dimensions this scan reads on.
    async fn open_dataset(
        store: Arc<dyn ObjectStore>,
        object: ObjectMeta,
        cache: Option<Hdf5ReaderCache>,
        read_dimensions: Option<Vec<String>>,
    ) -> datafusion::error::Result<beacon_nd_array::dataset::AnyDataset> {
        let dataset = crate::cache::open_dataset(cache.as_ref(), &store, &object)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open HDF5 dataset {}: {e}",
                    object.location
                ))
            })?;

        // Apply dimension projection before deriving the file schema. When no
        // explicit dimensions were requested, fall back to the dataset's
        // auto-selected default (matching `fetch_schema`). No log label here:
        // this runs per file/partition, so logging would spam.
        let read_dimensions =
            beacon_nd_array::dataset::resolve_read_dimensions(&dataset, read_dimensions, None);
        let Some(dims) = read_dimensions else {
            return Ok(dataset);
        };
        dataset
            .project(&DatasetProjection {
                dimension_projection: Some(dims),
                index_projection: None,
            })
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to project HDF5 dataset with dimensions: {e}"
                ))
            })
    }
}

impl FileOpener for Hdf5Opener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        // A file whose statistics cannot satisfy the predicate never reaches
        // here: the plan prunes it, off the statistics the file registry already
        // holds. Testing them again per opener would repeat that work on the one
        // file that survived it.
        //
        // A file in the share map is in every partition's group, so it is read
        // through its share and no other way. A file that is not is this
        // partition's alone.
        let share = self
            .partition_shares
            .get(&file.object_meta.location)
            .cloned();

        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        Ok(Self::read(
            share,
            self.object_store.clone(),
            file.object_meta,
            self.projected_schema.clone(),
            self.read_dimensions.clone(),
            self.batch_size,
            self.cache.clone(),
            metrics,
            self.predicate.clone(),
        )
        .boxed())
    }
}
