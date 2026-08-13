use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::{
    arrow::{
        metrics::SharedReadMetrics,
        share::{share_files, FileShares, SharedDataset},
    },
    projection::DatasetProjection,
};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    physical_expr::{conjunction, projection::ProjectionExprs, PhysicalExpr},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{stream::BoxStream, FutureExt};
use object_store::ObjectMeta;

use super::reader::{self, FileAccess, NetcdfInput, NetcdfReaderCache};

/// DataFusion [`FileSource`] for NetCDF (`.nc`) files.
///
/// Integrates the `beacon_arrow_netcdf` reader with DataFusion's file scan
/// pipeline via a [`FileOpener`].
#[derive(Debug, Clone)]
pub struct NetCDFSource {
    access: FileAccess,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Reader cache to consult for this scan. `None` disables caching.
    cache: Option<NetcdfReaderCache>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// The shares of this scan. Cloned, not copied, so every partition of a file
    /// reaches the same one.
    partitions_shared_map: FileShares,
}

impl NetCDFSource {
    pub fn new(
        access: FileAccess,
        read_dimensions: Option<Vec<String>>,
        table_schema: TableSchema,
    ) -> Self {
        Self {
            access,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            read_dimensions,
            batch_size: usize::MAX,
            predicate: None,
            cache: None,
            projection: None,
            partitions_shared_map: FileShares::default(),
        }
    }

    /// Returns a copy of this source that consults `cache` (when `Some`) for
    /// opened datasets. The format wires in the runtime's shared cache here.
    pub fn with_cache(mut self, cache: Option<NetcdfReaderCache>) -> Self {
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

    /// Whether this scan reads `path` through a share, rather than as one
    /// partition's own whole file.
    #[cfg(test)]
    pub(crate) fn shares_file(&self, path: &object_store::path::Path) -> bool {
        self.partitions_shared_map.contains_key(path)
    }
}

impl FileSource for NetCDFSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(NetCDFOpener::new(
            self.access.clone(),
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

    fn supports_repartitioning(&self) -> bool {
        matches!(self.access, FileAccess::Oxcdf)
    }

    /// Give every partition the files that are worth dividing, and one each of
    /// the files that are not.
    ///
    /// A file worth dividing goes into every partition's group and gets a cell
    /// in `partitions_shared_map`. Nothing about it is divided here: the
    /// partitions divide it as they read it, by taking subsets from the one
    /// queue behind that cell. Balance then follows completion rather than a
    /// guess made at plan time.
    ///
    /// A smaller file is left whole and dealt to one partition. Every partition
    /// opening it to take a subset or two would cost more than it returns, and
    /// the listing has already spread these across the scan.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if !self.supports_repartitioning() || output_ordering.is_some() || target_partitions <= 1 {
            // An ordered scan cannot share: a partition holding an arbitrary
            // subset of a file cannot emit its rows in file order.
            return Ok(None);
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
        "netcdf"
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

/// Opens a single NetCDF file and streams its contents as Arrow
/// [`RecordBatch`]es.
struct NetCDFOpener {
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    cache: Option<NetcdfReaderCache>,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    /// How this opener reaches its files, and which reader opens them.
    access: FileAccess,
    /// The store the scan lists from. The `oxcdf` reader reads through it; the
    /// netcdf-c reader ignores it and opens a resolved native path instead.
    object_store: Arc<dyn object_store::ObjectStore>,
    /// The shares of this scan, so the partitions of a file find each other.
    partition_shares: FileShares,
}

impl NetCDFOpener {
    #[allow(clippy::too_many_arguments)]
    fn new(
        access: FileAccess,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Option<NetcdfReaderCache>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        object_store: Arc<dyn object_store::ObjectStore>,
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
            access,
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
        input: NetcdfInput,
        object: ObjectMeta,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        cache: Option<NetcdfReaderCache>,
        metrics: SharedReadMetrics,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let planning = metrics.clone();
        let plan = async move || {
            let dataset = Self::open_dataset(input, object, cache, read_dimensions).await?;
            SharedDataset::plan(
                dataset,
                projected_schema,
                batch_size,
                predicate,
                Some(&planning),
            )
            .await
        };

        // The first partition to arrive opens the file and fills its queue. The
        // rest wait for it, then draw from that same queue.
        let dataset = match share {
            Some(cell) => cell
                .get_or_try_init::<datafusion::error::DataFusionError, _, _>(plan)
                .await?
                .clone(),
            None => plan().await?,
        };

        Ok(dataset.stream(Some(metrics)))
    }

    async fn open_dataset(
        input: NetcdfInput,
        object: ObjectMeta,
        cache: Option<NetcdfReaderCache>,
        read_dimensions: Option<Vec<String>>,
    ) -> datafusion::error::Result<beacon_nd_array::dataset::AnyDataset> {
        let dataset = reader::open_dataset(cache.as_ref(), input, object.clone())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open NetCDF dataset {}: {e}",
                    object.location,
                ))
            })?;

        // Apply dimension projection before deriving the file schema. When no
        // explicit dimensions were requested, fall back to the dataset's
        // auto-selected default (matching `fetch_schema`). No log label here:
        // this runs per file/partition, so logging would spam.
        let read_dimensions =
            beacon_nd_array::dataset::resolve_read_dimensions(&dataset, read_dimensions, None);
        let dataset = if let Some(dims) = read_dimensions {
            let proj = DatasetProjection {
                dimension_projection: Some(dims),
                index_projection: None,
            };
            dataset.project(&proj).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project NetCDF dataset with dimensions: {e}"
                ))
            })?
        } else {
            dataset
        };
        Ok(dataset)
    }
}

impl FileOpener for NetCDFOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let metrics = SharedReadMetrics::new(&self.metrics, self.partition);
        let input = self
            .access
            .input_for(&self.object_store, &file.object_meta)?;

        // A file in the share map is in every partition's group, so it is read
        // through its share and no other way. A file that is not is this
        // partition's alone.
        let share = self
            .partition_shares
            .get(&file.object_meta.location)
            .cloned();

        Ok(Self::read(
            share,
            input,
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
