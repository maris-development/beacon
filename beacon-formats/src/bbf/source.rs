use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileOpener, FileScanConfig, FileSource},
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
use object_store::ObjectStore;
use parking_lot::Mutex;

use crate::bbf::{metrics::BBFGlobalMetrics, opener::BBFOpener, stream_share::StreamShare};

#[derive(Clone, Debug)]
pub struct BBFSource {
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
    /// Batch Size.
    batch_size: usize,
    /// Pruning Predicate
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// File Tracer
    file_tracer: Arc<Mutex<Arc<Mutex<Vec<String>>>>>,
    /// Stream Partition Share
    stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<StreamShare>>>>,
    /// Global Metrics
    global_metrics: BBFGlobalMetrics,
}

impl BBFSource {
    pub fn set_file_tracer(&self, tracer: Arc<Mutex<Vec<String>>>) {
        let mut file_tracer = self.file_tracer.lock();
        *file_tracer = tracer;
    }
}

impl Default for BBFSource {
    fn default() -> Self {
        // println!("Creating default BBFSource");
        let base_metrics = ExecutionPlanMetricsSet::new();
        let global_metrics = BBFGlobalMetrics::new(base_metrics.clone());
        Self {
            schema_adapter_factory: None,
            override_schema: None,
            projection: None,
            execution_plan_metrics: base_metrics,
            projected_statistics: None,
            batch_size: 32 * 1024,
            predicate: None,
            file_tracer: Arc::new(Mutex::new(Arc::new(Mutex::new(vec![])))),
            stream_partition_shares: Arc::new(Mutex::new(HashMap::new())),
            global_metrics,
        }
    }
}

impl FileSource for BBFSource {
    /// Creates a `dyn FileOpener` based on given parameters
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
        Arc::new(BBFOpener::new(
            arc_schema_adapter,
            self.predicate
                .clone()
                .map(|p| PruningPredicate::try_new(p, table_schema.clone()).unwrap()),
            object_store,
            table_schema,
            self.file_tracer.lock().clone(),
            self.stream_partition_shares.clone(),
            self.global_metrics.clone(),
        ))
        // ParquetFormat
    }

    /// Any
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(BBFSource {
            batch_size,
            ..self.clone()
        })
    }
    /// Initialize new instance with a new schema
    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(BBFSource {
            override_schema: Some(schema),
            ..self.clone()
        })
    }
    /// Initialize new instance with projection information
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(BBFSource {
            projection: config.projection.clone(),
            ..self.clone()
        })
    }
    /// Initialize new instance with projected statistics
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(BBFSource {
            projected_statistics: Some(statistics),
            ..self.clone()
        })
    }
    /// Return execution plan metrics
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }
    /// Return projected statistics
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

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn with_schema_adapter_factory(
        &self,
        _factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(BBFSource {
            schema_adapter_factory: Some(_factory),
            ..self.clone()
        }))
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

    /// String representation of file source such as "csv", "json", "parquet"
    fn file_type(&self) -> &str {
        "bbf"
    }
}
