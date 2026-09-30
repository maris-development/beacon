//! DataFusion [`FileSource`] for HDF5 files on the Rust reader.
//!
//! The source opens an [`AnyDataset`] for the (projected) columns and streams
//! it through the shared ND engine, which handles predicate pushdown (chunk
//! pruning + row masking) via [`PushdownFilter`].
//!
//! This mirrors the netCDF source. The difference is where the bytes come from:
//! this one always reads through the scan's own object store, because its
//! reader needs no local file.

use std::sync::Arc;

use crate::ReadOptions;
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
    error::DataFusionError,
    execution::cache::cache_manager::FileMetadataCache,
    physical_expr::{conjunction, projection::ProjectionExprs, PhysicalExpr},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use object_store::ObjectStore;

/// DataFusion [`FileSource`] for HDF5 (`.h5`/`.hdf5`) files.
#[derive(Clone)]
pub struct Hdf5Source {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    read_dimensions: Option<Vec<String>>,
    /// Skip a file that does not fit `read_dimensions`, instead of failing.
    skip_unbroadcastable: bool,
    /// How this table reads one file: the naming of the invented dimensions,
    /// and the layout convention. See [`crate::ReadOptions`].
    read_options: ReadOptions,
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

impl std::fmt::Debug for Hdf5Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hdf5Source")
            .field("read_dimensions", &self.read_dimensions)
            .field("read_options", &self.read_options)
            .field("predicate", &self.predicate)
            .finish_non_exhaustive()
    }
}

impl Hdf5Source {
    pub fn new(
        read_dimensions: Option<Vec<String>>,
        read_options: ReadOptions,
        table_schema: TableSchema,
    ) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            read_dimensions,
            skip_unbroadcastable: false,
            read_options,
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
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<NdScan> {
        let files = Hdf5Files {
            object_store,
            read_dimensions: self.read_dimensions.clone(),
            skip_unbroadcastable: self.skip_unbroadcastable,
            read_options: self.read_options,
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

impl FileSource for Hdf5Source {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let scan = self.scan(object_store, base_config, partition)?;
        Ok(Arc::new(NdFileOpener::new(scan)))
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
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
    /// parts of one file run at the same time. Nothing is divided by byte
    /// range: each part reads its own slice of the chunk list. See
    /// [`beacon_nd_array::arrow::file_read`].
    fn supports_repartitioning(&self) -> bool {
        true
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
        if output_ordering.is_some() {
            // An ordered scan cannot split: a part of a file cannot emit its
            // rows in file order.
            return Ok(None);
        }

        // One partition, or no files: keep the scan as it was planned.
        let Some(file_groups) = split_files(&config.file_groups, target_partitions) else {
            return Ok(None);
        };
        tracing::debug!(
            "Hdf5Source split: {} entries over {target_partitions} partitions",
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

/// How one HDF5 file opens, and what this table reads on.
///
/// This is everything the nd morsel layer needs of the format.
///
/// A file whose statistics cannot satisfy the predicate never reaches here:
/// the plan prunes it, off the statistics the file registry already holds.
struct Hdf5Files {
    /// The store the scan lists from. The reader reads its byte ranges through
    /// it, so s3, gs and az work with no local copy.
    object_store: Arc<dyn ObjectStore>,
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
    read_options: ReadOptions,
}

#[async_trait::async_trait]
impl OpenFile for Hdf5Files {
    async fn open(&self, file: &PartitionedFile) -> datafusion::error::Result<AnyDataset> {
        crate::open::open_dataset(&self.object_store, &file.object_meta, self.read_options)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open HDF5 dataset {}: {e}",
                    file.object_meta.location
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
        .map_err(|e| DataFusionError::Execution(e.to_string()))
    }

    fn narrow_tag(&self) -> String {
        format!("{:?}/{}", self.read_dimensions, self.skip_unbroadcastable)
    }

    fn cache_tag(&self) -> String {
        format!("hdf5:{:?}", self.read_options)
    }
}
