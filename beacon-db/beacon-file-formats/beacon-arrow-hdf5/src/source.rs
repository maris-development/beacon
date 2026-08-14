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
    arrow::{
        metrics::SharedReadMetrics,
        morsel::{morsel_scan, MorselSource, OpenFile},
        share::{share_files, FileShares, SharedDataset},
    },
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

use crate::cache::Hdf5ReaderCache;

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
    /// The scan's file queue, when it is planned morsel-driven. See
    /// [`morsel_scan`].
    morsel: Option<Arc<MorselSource>>,
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
            partitions_shared_map: FileShares::default(),
            morsel: None,
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
            self.morsel.clone(),
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
    /// A file over the session's `repartition_file_min_size` goes into every
    /// partition's group and gets a share. Nothing about it is divided here: the
    /// partitions divide it as they read it, by taking chunks from the one queue
    /// behind that cell. Balance then follows completion rather than a guess
    /// made at plan time, which matters most under a predicate: an nd chunk list
    /// is C-ordered, so `WHERE time > …` prunes a prefix of it, and a deal made
    /// at plan time would leave the early partitions idle.
    ///
    /// A smaller file is left whole and dealt to one partition. Every partition
    /// opening it to take a chunk or two would cost more than it returns, and
    /// the listing has already spread these across the scan.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if output_ordering.is_some() || target_partitions <= 1 {
            // An ordered scan cannot share: a partition holding an arbitrary
            // subset of a file cannot emit its rows in file order.
            return Ok(None);
        }

        if let Some((morsel, file_groups)) = morsel_scan(&config.file_groups, target_partitions) {
            tracing::debug!(
                "Hdf5Source morsel scan: {} files over {target_partitions} partitions",
                morsel.files()
            );
            let mut config = config.clone();
            config.file_groups = file_groups;
            config.file_source = Arc::new(Self {
                morsel: Some(morsel),
                ..self.clone()
            });
            return Ok(Some(config));
        }

        Ok(share_files(
            &config.file_groups,
            target_partitions,
            Some(repartition_file_min_size as u64),
        )
        .map(|scan| {
            let mut config = config.clone();
            config.file_groups = scan.file_groups;
            // The openers are built from the config's source, so the shares
            // have to travel with it.
            config.file_source = Arc::new(Self {
                partitions_shared_map: scan.shares,
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
    /// This partition's counters, registered once. See [`SharedReadMetrics::new`].
    read_metrics: SharedReadMetrics,
    partition: usize,
    /// The store the scan lists from. The reader reads its byte ranges through
    /// it, so s3, gs and az work with no local copy.
    object_store: Arc<dyn ObjectStore>,
    /// The shares of this scan, so the partitions of a file find each other.
    partition_shares: FileShares,
    /// The scan's file queue, when it is planned morsel-driven. `Some` means the
    /// entry `FileStream` hands this opener is the scan, not a file.
    morsel: Option<Arc<MorselSource>>,
    /// How one file is opened, for the queue to call.
    files: Arc<dyn OpenFile>,
}

/// How one HDF5 file becomes a planned [`SharedDataset`].
///
/// This is everything a [`MorselSource`] needs of the format.
struct Hdf5Files {
    object_store: Arc<dyn ObjectStore>,
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    cache: Option<Hdf5ReaderCache>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    metrics: SharedReadMetrics,
}

impl std::fmt::Debug for Hdf5Files {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hdf5Files").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl OpenFile for Hdf5Files {
    async fn open(&self, file: &PartitionedFile) -> datafusion::error::Result<Arc<SharedDataset>> {
        let dataset = Hdf5Opener::open_dataset(
            self.object_store.clone(),
            file.object_meta.clone(),
            self.cache.clone(),
            self.read_dimensions.clone(),
        )
        .await?;

        SharedDataset::plan(
            dataset,
            self.projected_schema.clone(),
            self.batch_size,
            self.predicate.clone(),
            Some(&self.metrics),
        )
        .await
    }
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
        morsel: Option<Arc<MorselSource>>,
    ) -> Self {
        let read_metrics = SharedReadMetrics::new(&metrics, partition);
        let files = Arc::new(Hdf5Files {
            object_store: object_store.clone(),
            projected_schema: projected_schema.clone(),
            read_dimensions: read_dimensions.clone(),
            batch_size,
            cache: cache.clone(),
            predicate: predicate.clone(),
            metrics: read_metrics.clone(),
        });

        Self {
            morsel,
            files,
            projected_schema,
            read_dimensions,
            batch_size,
            predicate,
            cache,
            read_metrics,
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
        share: Option<Arc<beacon_nd_array::arrow::share::FileShare>>,
        store: Arc<dyn ObjectStore>,
        object: ObjectMeta,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        cache: Option<Hdf5ReaderCache>,
        metrics: SharedReadMetrics,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let planning = metrics.clone();
        let plan = async move || {
            let dataset = Self::open_dataset(store, object, cache, read_dimensions).await?;
            SharedDataset::plan(
                dataset,
                projected_schema,
                batch_size,
                predicate,
                Some(&planning),
            )
            .await
        };

        // The first partition to arrive opens the file and fills its queue.
        // What the rest do depends on the share's mode: draw from the same queue,
        // or leave this one to whoever claimed it and move on.
        let dataset = match share {
            Some(share) => match share.open(plan).await? {
                Some(dataset) => dataset,
                None => return Ok(Box::pin(futures::stream::empty())),
            },
            None => plan().await?,
        };

        Ok(dataset.stream(Some(metrics)))
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
        // A morsel-driven scan hands every partition the same standing entry.
        // It is not a file: the files are in the queue, and this partition reads
        // whatever it hands out until the scan is done.
        if let Some(morsel) = &self.morsel {
            let stream = morsel.stream(
                self.partition,
                Arc::clone(&self.files),
                Some(self.read_metrics.clone()),
            );
            return Ok(futures::future::ready(Ok(stream)).boxed());
        }

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

        let metrics = self.read_metrics.clone();
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
