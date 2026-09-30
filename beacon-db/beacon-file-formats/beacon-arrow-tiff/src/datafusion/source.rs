use std::sync::Arc;

use beacon_nd_array::{
    arrow::{
        metrics::ReadMetrics,
        morsel::{split_files, Morselizer, NdFileOpener, NdMorselizer, NdScan, OpenFile, ScanPlans},
    },
    dataset::AnyDataset,
};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    execution::cache::cache_manager::FileMetadataCache,
    physical_expr::{conjunction, projection::ProjectionExprs, PhysicalExpr},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};

use super::reader;

#[derive(Clone)]
pub struct TiffSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// The session's file metadata cache. The parts of a split raster share
    /// one open through it.
    metadata_cache: Option<Arc<dyn FileMetadataCache>>,
    /// The chunk lists of this query's split rasters. Clones share them. See
    /// [`ScanPlans`].
    plans: Arc<ScanPlans>,
}

impl std::fmt::Debug for TiffSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiffSource")
            .field("batch_size", &self.batch_size)
            .field("predicate", &self.predicate)
            .finish_non_exhaustive()
    }
}

impl TiffSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: 128 * 1024,
            predicate: None,
            projection: None,
            metadata_cache: None,
            plans: Arc::default(),
        }
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }

    /// The same source, sharing opens through the session's file metadata
    /// cache.
    pub fn with_metadata_cache(mut self, cache: Option<Arc<dyn FileMetadataCache>>) -> Self {
        self.metadata_cache = cache;
        self
    }

    /// What one partition reads from each raster.
    fn scan(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<NdScan> {
        // Once per partition, not once per file: every call registers its
        // counters into the scan's one metrics set, behind a mutex.
        let metrics = ReadMetrics::new(&self.execution_plan_metrics, partition);
        NdScan::new(
            Arc::new(TiffRasters { object_store }),
            base_config,
            self.batch_size,
            self.predicate.clone(),
            metrics,
            self.metadata_cache.clone(),
            Arc::clone(&self.plans),
        )
    }
}

impl FileSource for TiffSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let scan = self.scan(object_store, base_config, partition)?;
        Ok(Arc::new(NdFileOpener::new(scan)))
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Box<dyn Morselizer>> {
        let scan = self.scan(object_store, base_config, partition)?;
        Ok(Box::new(NdMorselizer::new(scan)))
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

    /// Split the scan's rasters so that it fills every partition.
    ///
    /// A raster becomes several parts when the scan has fewer rasters than
    /// partitions. DataFusion's shared queue then hands the parts to whichever
    /// partition is free. See [`split_files`].
    ///
    /// An ordered scan keeps its grouping: a partition taking rasters as it
    /// finishes cannot emit them in listing order.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if output_ordering.is_some() {
            return Ok(None);
        }

        let Some(file_groups) = split_files(&config.file_groups, target_partitions) else {
            return Ok(None);
        };

        tracing::debug!(
            "TiffSource split: {} entries over {target_partitions} partitions",
            file_groups.iter().map(|group| group.len()).sum::<usize>()
        );
        let mut config = config.clone();
        config.file_groups = file_groups;
        Ok(Some(config))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "tiff"
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

/// How one GeoTIFF opens.
///
/// This is everything the nd morsel layer needs of the format.
struct TiffRasters {
    object_store: Arc<dyn object_store::ObjectStore>,
}

#[async_trait::async_trait]
impl OpenFile for TiffRasters {
    async fn open(&self, file: &PartitionedFile) -> datafusion::error::Result<AnyDataset> {
        let object = file.object_meta.clone();
        reader::open_dataset(self.object_store.clone(), object.clone())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open TIFF dataset {}: {e}",
                    object.location,
                ))
            })
    }

    fn cache_tag(&self) -> String {
        "tiff".to_string()
    }
}
