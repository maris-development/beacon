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
        table_schema::TableSchema,
    },
    execution::cache::cache_manager::FileMetadataCache,
    physical_expr::{conjunction, projection::ProjectionExprs, PhysicalExpr},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};

use super::reader::FileAccess;

/// DataFusion [`FileSource`] for NetCDF (`.nc`) files.
///
/// Integrates the `beacon_arrow_netcdf` reader with DataFusion's morsel-driven
/// file scan through an [`NdMorselizer`].
#[derive(Clone)]
pub struct NetCDFSource {
    access: FileAccess,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    read_dimensions: Option<Vec<String>>,
    /// Skip a file that does not fit `read_dimensions`, instead of failing.
    skip_unbroadcastable: bool,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// The session's file metadata cache. The parts of a split file share one
    /// open through it.
    metadata_cache: Option<Arc<dyn FileMetadataCache>>,
    /// The chunk lists of this query's split files. Clones share them. See
    /// [`ScanPlans`].
    plans: Arc<ScanPlans>,
}

impl std::fmt::Debug for NetCDFSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetCDFSource")
            .field("access", &self.access)
            .field("read_dimensions", &self.read_dimensions)
            .field("predicate", &self.predicate)
            .finish_non_exhaustive()
    }
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
            skip_unbroadcastable: false,
            batch_size: usize::MAX,
            predicate: None,
            projection: None,
            metadata_cache: None,
            plans: Arc::default(),
        }
    }

    /// The same source, skipping the files that cannot broadcast when `skip`.
    pub fn with_skip_unbroadcastable(mut self, skip: bool) -> Self {
        self.skip_unbroadcastable = skip;
        self
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

    /// What one partition reads from each file.
    fn scan(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<NdScan> {
        let files = NetCDFFiles {
            access: self.access.clone(),
            object_store,
            read_dimensions: self.read_dimensions.clone(),
            skip_unbroadcastable: self.skip_unbroadcastable,
        };
        NdScan::new(
            Arc::new(files),
            base_config,
            self.batch_size,
            self.predicate.clone(),
            ReadMetrics::new(&self.execution_plan_metrics, partition),
            self.metadata_cache.clone(),
            Arc::clone(&self.plans),
        )
    }
}

impl FileSource for NetCDFSource {
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

    fn supports_repartitioning(&self) -> bool {
        tracing::trace!(
            "NetCDFSource supports_repartitioning: access={:?}",
            self.access
        );
        matches!(self.access, FileAccess::Oxcdf)
    }

    /// Split the scan's files so that it fills every partition.
    ///
    /// A file becomes several parts when the scan has fewer files than
    /// partitions. DataFusion's shared queue then hands the parts to whichever
    /// partition is free. See [`split_files`].
    ///
    /// `repartition_file_min_size` is unused. An nd file divides on its chunk
    /// list, not on its bytes, so its size does not say what a split buys.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        tracing::trace!(
            "NetCDFSource repartitioned: access={:?}, target_partitions={target_partitions}",
            self.access,
        );
        if !self.supports_repartitioning() || output_ordering.is_some() {
            return Ok(None);
        }

        // One partition, or no files: keep the scan as it was planned.
        let Some(file_groups) = split_files(&config.file_groups, target_partitions) else {
            return Ok(None);
        };
        tracing::debug!(
            "NetCDFSource split: {} entries over {target_partitions} partitions",
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

/// How one netCDF file opens, and what this table reads on.
///
/// This is everything the nd morsel layer needs of the format.
struct NetCDFFiles {
    access: FileAccess,
    /// The store the scan lists from. The `oxcdf` reader reads through it; the
    /// netcdf-c reader ignores it and opens a resolved native path instead.
    object_store: Arc<dyn object_store::ObjectStore>,
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
}

#[async_trait::async_trait]
impl OpenFile for NetCDFFiles {
    async fn open(&self, file: &PartitionedFile) -> datafusion::error::Result<AnyDataset> {
        tracing::trace!(
            "NetCDFFiles open: access={:?}, file={:?}",
            self.access,
            file.object_meta.location
        );
        let input = self
            .access
            .input_for(&self.object_store, &file.object_meta)?;
        input.open().await.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to open NetCDF dataset {}: {e}",
                file.object_meta.location,
            ))
        })
    }

    /// Apply dimension projection before deriving the file schema. When no
    /// explicit dimensions were requested, fall back to the dataset's
    /// auto-selected default (matching `fetch_schema`).
    fn narrow(&self, dataset: AnyDataset) -> datafusion::error::Result<Option<AnyDataset>> {
        // No log label here: this runs per file, so logging would spam.
        beacon_nd_array::dataset::project_read_dimensions_or_skip(
            dataset,
            self.read_dimensions.clone(),
            self.skip_unbroadcastable,
            None,
        )
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
    }

    fn narrow_tag(&self) -> String {
        format!("{:?}/{}", self.read_dimensions, self.skip_unbroadcastable)
    }

    fn cache_tag(&self) -> String {
        format!("netcdf:{:?}", self.access.backend())
    }
}
