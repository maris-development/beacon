//! DataFusion [`FileSource`] for zarr groups.
//!
//! Each opened file is one leaf zarr group's `zarr.json`. The source opens an
//! [`AnyDataset`] for the (projected) columns and streams it through the
//! shared engine, which handles predicate pushdown (chunk pruning + row
//! masking) via [`PushdownFilter`].

use std::sync::Arc;

use beacon_nd_array::{
    arrow::{
        metrics::ReadMetrics,
        morsel::{Morselizer, NdFileOpener, NdMorselizer, NdScan, OpenFile, ScanPlans, split_files},
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
    physical_expr::{conjunction, projection::ProjectionExprs},
    physical_plan::{
        PhysicalExpr,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use object_store::ObjectStore;
use zarrs::group::Group;

use crate::{
    reader::{dataset_from_group, project_read_dimensions_or_skip},
    util::{ZarrPath, ZarrStorage},
};

/// The nominal size a zarr leaf group reports.
///
/// A leaf group is not one object. It is a node with a `zarr.json` and a tree of
/// chunk files under it, so no byte count describes it. The value carries no
/// meaning of its own. A split divides a group on its chunk list, never on
/// bytes, so the value only keeps the group from looking empty.
pub(crate) const NOMINAL_GROUP_SIZE: u64 = 1 << 20;

/// DataFusion [`FileSource`] for zarr groups.
#[derive(Clone)]
pub struct ZarrSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Explicit dimensions to read, or `None` to auto-select a default.
    read_dimensions: Option<Vec<String>>,
    /// Skip a group that does not fit `read_dimensions`, instead of failing.
    skip_unbroadcastable: bool,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// Storage to open groups over, replacing the session's object store.
    /// Set by the Icechunk reader; `None` for a listed zarr store.
    storage: Option<ZarrStorage>,
    /// The session's file metadata cache. The parts of a split group share
    /// one open through it.
    metadata_cache: Option<Arc<dyn FileMetadataCache>>,
    /// The chunk lists of this query's split groups. Clones share them. See
    /// [`ScanPlans`].
    plans: Arc<ScanPlans>,
}

impl ZarrSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            predicate: None,
            read_dimensions: None,
            skip_unbroadcastable: false,
            projection: None,
            storage: None,
            metadata_cache: None,
            plans: Arc::default(),
        }
    }

    /// The same source, skipping the groups that cannot broadcast when `skip`.
    pub fn with_skip_unbroadcastable(mut self, skip: bool) -> Self {
        self.skip_unbroadcastable = skip;
        self
    }

    /// Returns a copy of this source that opens groups over `storage` instead of
    /// the session's object store.
    pub fn with_storage(mut self, storage: ZarrStorage) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Returns a copy of this source that reads only the variables belonging to
    /// `read_dimensions` (or auto-selects a default when `None`).
    pub fn with_read_dimensions(mut self, read_dimensions: Option<Vec<String>>) -> Self {
        self.read_dimensions = read_dimensions;
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
    /// cache. Use it only for a listed store: the cache keys a group by its
    /// path, and a store that replaces the object store can hold other data
    /// under the same path.
    pub fn with_metadata_cache(mut self, cache: Option<Arc<dyn FileMetadataCache>>) -> Self {
        self.metadata_cache = cache;
        self
    }

    /// What one partition reads from each group.
    fn scan(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<NdScan> {
        let storage = self
            .storage
            .clone()
            .unwrap_or_else(|| ZarrStorage::from_object_store(object_store));
        let groups = ZarrGroups {
            storage,
            read_dimensions: self.read_dimensions.clone(),
            skip_unbroadcastable: self.skip_unbroadcastable,
        };
        // Zarr refuses a partitioned table before a scan is built (see
        // `reject_partition_columns`), so a group never carries values.
        NdScan::new(
            Arc::new(groups),
            base_config,
            self.batch_size,
            self.predicate.clone(),
            ReadMetrics::new(&self.execution_plan_metrics, partition),
            self.metadata_cache.clone(),
            Arc::clone(&self.plans),
        )
    }
}

impl FileSource for ZarrSource {
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

    /// Split every group over the partitions, whatever its `zarr.json` weighs.
    ///
    /// No size threshold applies here. A zarr group's object is its
    /// `zarr.json`: a metadata document of a few KB that can front terabytes of
    /// chunks. Any threshold on it would measure the wrong thing and decline
    /// every store, however large.
    ///
    /// What makes that safe is the chunk grid. A group states its chunks in
    /// metadata the open already read, so a part takes its slice of that list,
    /// and a part that finds no chunk reads nothing.
    ///
    /// DataFusion's shared queue hands the parts to whichever partition is
    /// free. Each part slices the chunk list after the predicate prunes it,
    /// which matters most under a predicate: an nd chunk list is C-ordered, so
    /// `WHERE time > …` prunes a prefix of it.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if output_ordering.is_some() {
            // An ordered scan cannot split: a part of a group cannot emit its
            // rows in group order.
            return Ok(None);
        }

        // One partition, or no groups: keep the scan as it was planned. A
        // partitioned Zarr table never reaches here, because
        // `ZarrFormat::create_physical_plan` refuses it.
        let Some(file_groups) = split_files(&config.file_groups, target_partitions) else {
            return Ok(None);
        };
        tracing::debug!(
            "ZarrSource split: {} entries over {target_partitions} partitions",
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
        "zarr"
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(factory),
            ..self.clone()
        }))
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

/// How one Zarr group opens, and what this table reads on.
///
/// This is everything the nd morsel layer needs of the format: DataFusion's
/// queue holds the groups, and this says what opening one means.
struct ZarrGroups {
    storage: ZarrStorage,
    read_dimensions: Option<Vec<String>>,
    /// Skip a group that does not fit `read_dimensions`, instead of failing.
    skip_unbroadcastable: bool,
}

#[async_trait::async_trait]
impl OpenFile for ZarrGroups {
    async fn open(&self, file: &PartitionedFile) -> datafusion::error::Result<AnyDataset> {
        let zarr_path = ZarrPath::new_from_object_meta(file.object_meta.clone()).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create ZarrPath from object metadata: {e}"
            ))
        })?;

        let group = Group::async_open(self.storage.inner(), &zarr_path.as_zarr_path())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open Zarr group at '{}': {e}",
                    zarr_path.as_zarr_path()
                ))
            })?;

        dataset_from_group(&group, None).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to read Zarr group as dataset: {e}"))
        })
    }

    /// Apply explicit dimensions, or narrow to a broadcast-compatible default
    /// so `SELECT *` cannot fail when variables live on incompatible dimension
    /// sets.
    fn narrow(&self, dataset: AnyDataset) -> datafusion::error::Result<Option<AnyDataset>> {
        // No log label: this runs per group (logging happens in schema
        // inference).
        project_read_dimensions_or_skip(
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
        "zarr".to_string()
    }
}
