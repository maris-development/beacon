use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileOpener, FileScanConfig, FileSource},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory},
    },
    physical_expr::conjunction,
    physical_expr_adapter::{
        DefaultPhysicalExprAdapter, DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
    },
    physical_plan::{
        PhysicalExpr,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use object_store::ObjectStore;

use crate::datafusion::opener::{
    AtlasGlobalMetrics, AtlasOpener, SharedCollectionRegistry, SharedDatasetQueueRegistry,
};

/// DataFusion file source for Atlas partition metadata files.
///
/// This source creates `AtlasOpener` instances and keeps shared state used
/// across openers, such as dataset work queues and opened collections.
#[derive(Clone, Debug)]
pub struct AtlasSource {
    /// Optional schema adapter factory.
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Expr adapter factory.
    expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    /// Optional schema override.
    override_schema: Option<SchemaRef>,
    /// Optional column projection.
    projection: Option<Vec<usize>>,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Projected statistics.
    projected_statistics: Option<Statistics>,
    /// Shared per-partition dataset queues used by duplicate execution partitions.
    shared_dataset_queues: SharedDatasetQueueRegistry,
    /// Shared opened collections keyed by collection path.
    shared_collections: SharedCollectionRegistry,
    /// Global execution counters published via DataFusion metrics.
    global_metrics: AtlasGlobalMetrics,
    /// Filter predicates to apply during pruning.
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl AtlasSource {
    /// Create a new Atlas source with empty shared caches.
    pub fn new() -> Self {
        let base_metrics = ExecutionPlanMetricsSet::new();
        Self {
            schema_adapter_factory: None,
            override_schema: None,
            projection: None,
            execution_plan_metrics: base_metrics.clone(),
            projected_statistics: None,
            shared_dataset_queues: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            shared_collections: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            global_metrics: AtlasGlobalMetrics::new(base_metrics),
            predicate: None,
            expr_adapter_factory: None,
        }
    }
}

impl Default for AtlasSource {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSource for AtlasSource {
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
        let expr_adapter_factory = self
            .expr_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultPhysicalExprAdapterFactory));
        let schema_adapter =
            schema_adapter_factory.create(projected_schema.clone(), table_schema.clone());
        let expr_adapter = expr_adapter_factory.create(projected_schema, table_schema);

        // Openers are lightweight and share cross-file state through the source.
        Arc::new(AtlasOpener {
            object_store,
            schema_adapter: Arc::from(schema_adapter),
            shared_dataset_queues: self.shared_dataset_queues.clone(),
            shared_collections: self.shared_collections.clone(),
            metrics: self.global_metrics.clone(),
            predicate: self.predicate.clone(),
            expr_adapter,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(AtlasSource {
            override_schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(AtlasSource {
            projection: config.projection.clone(),
            ..self.clone()
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(AtlasSource {
            projected_statistics: Some(statistics),
            ..self.clone()
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
        "atlas"
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(AtlasSource {
            schema_adapter_factory: Some(factory),
            ..self.clone()
        }))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let Some(_) = self.override_schema.clone() else {
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

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}
