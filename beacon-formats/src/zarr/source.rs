use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use beacon_arrow_zarr::{
    array_slice_pushdown::ArraySlicePushDown, reader::AsyncArrowZarrGroupReader,
};
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{
            FileGroup, FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource,
        },
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    physical_expr::conjunction,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        PhysicalExpr,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{FutureExt, StreamExt};
use object_store::{ObjectMeta, ObjectStore};
use parking_lot::Mutex;
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::{
    opener::{ZarrFileOpener, stream_share::ZarrStreamShare},
    statistics::ZarrStatisticsSelection,
    util::ZarrPath,
};

pub async fn fetch_schema(
    object_store: Arc<dyn ObjectStore>,
    zarr_path: &ZarrPath,
) -> datafusion::error::Result<SchemaRef> {
    let zarr_store = Arc::new(AsyncObjectStore::new(object_store))
        as Arc<dyn AsyncReadableListableStorageTraits>;

    let group = Group::async_open(zarr_store.clone(), &zarr_path.as_zarr_path())
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    let mut zarr_groups = Vec::new();
    recursive_groups(Arc::new(group), &mut zarr_groups).await?;

    // For each zarr group, fetch its schema and merge them.
    let mut schemas = Vec::new();
    for zarr_group in zarr_groups {
        let reader = AsyncArrowZarrGroupReader::new(zarr_group.clone())
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let schema = reader.arrow_schema();
        schemas.push(schema);
    }

    let super_schema = beacon_common::super_typing::super_type_schema(&schemas).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to compute super schema for Zarr groups at {}: {}",
            zarr_path.as_zarr_path(),
            e
        ))
    })?;

    Ok(Arc::new(super_schema))
}

pub async fn recursive_groups(
    top_level_group: Arc<Group<dyn AsyncReadableListableStorageTraits>>,
    zarr_groups: &mut Vec<Arc<Group<dyn AsyncReadableListableStorageTraits>>>,
) -> datafusion::error::Result<()> {
    let child_groups = top_level_group.async_child_groups().await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Failed to list child groups: {}", e))
    })?;

    if child_groups.is_empty() {
        // Add the top-level group itself if it has no child groups.
        zarr_groups.push(top_level_group.clone());
    } else {
        for child_group in child_groups {
            Box::pin(recursive_groups(Arc::new(child_group), zarr_groups)).await?;
        }
    }
    return Ok(());
}

#[derive(Default, Clone)]
pub struct ZarrSource {
    /// Optional schema adapter factory.
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Optional schema override.
    override_schema: Option<SchemaRef>,
    /// Optional column projection.
    projection: Option<Vec<usize>>,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Projected statistics.
    projected_statistics: Option<Statistics>,
    /// Stream Partition Share
    stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<ZarrStreamShare>>>>,
    /// Array Steps for slicing arrays based on step spans. This is utilized by the pruning predicate pushdown.
    zarr_selection_statistics: Option<Arc<ZarrStatisticsSelection>>,
    /// Pruning Predicate
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl ZarrSource {
    /// Sets the pushdown Zarr statistics selection.
    pub fn with_zarr_statistics(
        mut self,
        zarr_statistics: Option<Arc<ZarrStatisticsSelection>>,
    ) -> Self {
        self.zarr_selection_statistics = zarr_statistics;
        self
    }
}

impl FileSource for ZarrSource {
    /// Creates a file opener for the given object store and scan config.
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        let table_schema = self
            .override_schema
            .clone()
            .unwrap_or_else(|| base_config.file_schema.clone());
        let projected_schema = base_config.projected_schema();
        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));
        let schema_adapter =
            schema_adapter_factory.create(projected_schema.clone(), table_schema.clone());
        let arc_schema_adapter: Arc<dyn SchemaAdapter> = Arc::from(schema_adapter);

        Arc::new(ZarrFileOpener {
            table_schema,
            zarr_object_store: object_store.clone(),
            schema_adapter: arc_schema_adapter,
            stream_partition_shares: self.stream_partition_shares.clone(),
            pushdown_predicate_expr: self.predicate.clone(),
            pushdown_statistics: self.zarr_selection_statistics.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: Some(schema),
            projection: self.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
            stream_partition_shares: self.stream_partition_shares.clone(),
            predicate: self.predicate.clone(),
            zarr_selection_statistics: self.zarr_selection_statistics.clone(),
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: self.override_schema.clone(),
            projection: config.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
            stream_partition_shares: self.stream_partition_shares.clone(),
            predicate: self.predicate.clone(),
            zarr_selection_statistics: self.zarr_selection_statistics.clone(),
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: self.override_schema.clone(),
            projection: self.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: Some(statistics),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
            stream_partition_shares: self.stream_partition_shares.clone(),
            predicate: self.predicate.clone(),
            zarr_selection_statistics: self.zarr_selection_statistics.clone(),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        if let Some(statistics) = &self.projected_statistics {
            Ok(statistics.clone())
        } else if let Some(schema) = self.override_schema.as_ref() {
            Ok(Statistics::new_unknown(schema))
        } else {
            Err(datafusion::error::DataFusionError::Execution(
                "Schema must be set to compute statistics".to_string(),
            ))
        }
    }

    fn file_type(&self) -> &str {
        "zarr"
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            override_schema: self.override_schema.clone(),
            projection: self.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            schema_adapter_factory: Some(factory),
            stream_partition_shares: self.stream_partition_shares.clone(),
            predicate: self.predicate.clone(),
            zarr_selection_statistics: self.zarr_selection_statistics.clone(),
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        // Repartition by duplicating the file groups to reach the target number of partitions.
        let mut scan_config = config.clone();

        scan_config.file_groups = scan_config
            .file_groups
            .iter()
            .cycle()
            .take(target_partitions)
            .cloned()
            .collect();

        Ok(Some(scan_config))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let Some(file_schema) = self.override_schema.clone() else {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; filters.len()],
            ));
        };

        let predicate = match self.predicate.clone() {
            Some(predicate) => conjunction(std::iter::once(predicate).chain(filters.clone())),
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
