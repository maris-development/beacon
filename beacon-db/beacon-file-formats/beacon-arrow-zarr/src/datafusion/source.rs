//! DataFusion [`FileSource`]/[`FileOpener`] for zarr groups.
//!
//! Each opened file is one leaf zarr group's `zarr.json`. The opener builds an
//! [`AnyDataset`](beacon_nd_array::dataset::AnyDataset) for the (projected)
//! columns and streams it through the shared engine, which handles predicate
//! pushdown (chunk pruning + row masking) via [`PushdownFilter`].

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use beacon_nd_array::arrow::{
    metrics::SharedReadMetrics,
    share::{FileShares, SharedDataset, share_files},
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
    physical_expr::{conjunction, projection::ProjectionExprs},
    physical_plan::{
        PhysicalExpr,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{FutureExt, stream::BoxStream};
use object_store::ObjectStore;
use zarrs::group::Group;

use crate::{
    reader::{dataset_from_group, project_read_dimensions},
    util::{ZarrPath, ZarrStorage},
};

/// The nominal size a zarr leaf group reports.
///
/// A leaf group is not one object. It is a node with a `zarr.json` and a tree of
/// chunk files under it, so no byte count describes it, which is why it used to
/// report zero. Zero has a cost: DataFusion divides a file by byte range, and it
/// declines to divide a range of zero, so a group could never be split.
///
/// The value carries no meaning of its own. It only has to leave room for one
/// range per partition. [`ZarrOpener`] reads its range as a fraction of the
/// chunk list and never as bytes, so the fractions come out exact whatever this
/// is. See [`beacon_nd_array::arrow::split`].
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
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// Storage to open groups over, replacing the session's object store.
    /// Set by the Icechunk reader; `None` for a listed zarr store.
    storage: Option<ZarrStorage>,
    /// The shares of this scan. Cloned, not copied, so every partition of a
    /// group reaches the same one.
    partitions_shared_map: FileShares,
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
            projection: None,
            storage: None,
            partitions_shared_map: FileShares::default(),
        }
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

    /// Whether this scan reads `path` through a share, rather than as one
    /// partition's own whole group.
    #[cfg(test)]
    pub(crate) fn shares_group(&self, path: &object_store::path::Path) -> bool {
        self.partitions_shared_map.contains_key(path)
    }
}

impl FileSource for ZarrSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(ZarrOpener {
            storage: self
                .storage
                .clone()
                .unwrap_or_else(|| ZarrStorage::from_object_store(object_store)),
            projected_schema,
            predicate: self.predicate.clone(),
            batch_size: self.batch_size,
            read_dimensions: self.read_dimensions.clone(),
            metrics: self.execution_plan_metrics.clone(),
            partition,
            partition_shares: self.partitions_shared_map.clone(),
        }))
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

    /// Share every group with every partition, whatever its `zarr.json` weighs.
    ///
    /// No size threshold applies here, and that is the difference from netCDF
    /// and HDF5. Those two hold the session's `repartition_file_min_size` back
    /// because their object is their data, so its size says what a share would
    /// buy. A zarr group's object is its `zarr.json`: a metadata document of a
    /// few KB that can front terabytes of chunks. Any threshold on it would
    /// measure the wrong thing and decline every store, however large.
    ///
    /// What makes that safe is the chunk grid. A group states its chunks in
    /// metadata the open already read, so the queue falls out of a structure
    /// that exists whether or not the scan shares, and a partition that arrives
    /// at an empty queue simply reads nothing.
    ///
    /// Nothing is divided here. The group goes into every partition's group and
    /// gets a cell in `partitions_shared_map`, and the partitions divide it as
    /// they read it. Balance follows completion rather than a guess made at plan
    /// time, which matters most under a predicate: an nd chunk list is
    /// C-ordered, so `WHERE time > …` prunes a prefix of it.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if output_ordering.is_some() || target_partitions <= 1 {
            // An ordered scan cannot share: a partition holding an arbitrary
            // subset of a group cannot emit its rows in group order.
            return Ok(None);
        }

        // `None`: every group is shared, whatever its `zarr.json` weighs.
        Ok(
            share_files(&config.file_groups, target_partitions, None).map(|scan| {
                let mut config = config.clone();
                config.file_groups = scan.file_groups;
                // The openers are built from the config's source, so the shares
                // have to travel with it.
                config.file_source = Arc::new(Self {
                    partitions_shared_map: scan.shares,
                    ..self.clone()
                });
                config
            }),
        )
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

// ─── FileOpener ──────────────────────────────────────────────────────────────

struct ZarrOpener {
    storage: ZarrStorage,
    projected_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
    read_dimensions: Option<Vec<String>>,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    /// The shares of this scan, so the partitions of a group find each other.
    partition_shares: FileShares,
}

impl ZarrOpener {
    /// Open one group and narrow it to the dimensions this scan reads on.
    async fn open_dataset(
        storage: ZarrStorage,
        zarr_path: ZarrPath,
        read_dimensions: Option<Vec<String>>,
    ) -> datafusion::error::Result<beacon_nd_array::dataset::AnyDataset> {
        let group = Group::async_open(storage.inner(), &zarr_path.as_zarr_path())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open Zarr group at '{}': {e}",
                    zarr_path.as_zarr_path()
                ))
            })?;

        let dataset = dataset_from_group(&group, None).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to read Zarr group as dataset: {e}"))
        })?;

        // Apply explicit dimensions, or narrow to a broadcast-compatible default
        // so `SELECT *` cannot fail when variables live on incompatible
        // dimension sets. No log label: this runs per group/partition (logging
        // happens in schema inference).
        project_read_dimensions(dataset, read_dimensions, None)
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }

    /// Read one group, through its share when it has one.
    ///
    /// A shared group is opened and planned by whichever partition arrives
    /// first; the rest attach to what it built and pull from the same queue. An
    /// unshared group is planned by the one partition that holds it.
    ///
    /// Every input to the plan has to be identical in every partition of one
    /// group, or the partitions would not be reading the same group the same
    /// way. They are: the dataset, `projected_schema`, `batch_size` and
    /// `predicate`. A scan takes all four from one place, so they are.
    #[allow(clippy::too_many_arguments)]
    async fn read(
        share: Option<Arc<tokio::sync::OnceCell<Arc<SharedDataset>>>>,
        storage: ZarrStorage,
        zarr_path: ZarrPath,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        metrics: SharedReadMetrics,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let planning = metrics.clone();
        let plan = async move || {
            let dataset = Self::open_dataset(storage, zarr_path, read_dimensions).await?;
            SharedDataset::plan(
                dataset,
                projected_schema,
                batch_size,
                predicate,
                Some(&planning),
            )
            .await
        };

        // The first partition to arrive opens the group and fills its queue. The
        // rest wait for it, then draw from that same queue.
        let dataset = match share {
            Some(cell) => cell
                .get_or_try_init::<DataFusionError, _, _>(plan)
                .await?
                .clone(),
            None => plan().await?,
        };

        Ok(dataset.stream(Some(metrics)))
    }
}

impl FileOpener for ZarrOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let zarr_path = ZarrPath::new_from_object_meta(file.object_meta.clone()).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create ZarrPath from object metadata: {e}"
            ))
        })?;

        // A group in the share map is in every partition's group, so it is read
        // through its share and no other way. One that is not is this
        // partition's alone.
        let share = self
            .partition_shares
            .get(&file.object_meta.location)
            .cloned();

        let metrics = SharedReadMetrics::new(&self.metrics, self.partition);
        Ok(Self::read(
            share,
            self.storage.clone(),
            zarr_path,
            self.projected_schema.clone(),
            self.read_dimensions.clone(),
            self.batch_size,
            metrics,
            self.predicate.clone(),
        )
        .boxed())
    }
}
