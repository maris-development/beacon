use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    physical_expr::{conjunction, projection::ProjectionExprs},
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
    /// The table schema (file schema + partition columns).
    table_schema: TableSchema,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
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
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl BBFSource {
    pub fn new(table_schema: TableSchema) -> Self {
        let base_metrics = ExecutionPlanMetricsSet::new();
        let global_metrics = BBFGlobalMetrics::new(base_metrics.clone());
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: base_metrics,
            batch_size: 32 * 1024,
            predicate: None,
            file_tracer: Arc::new(Mutex::new(Arc::new(Mutex::new(vec![])))),
            stream_partition_shares: Arc::new(Mutex::new(HashMap::new())),
            global_metrics,
            projection: None,
        }
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }

    pub fn set_file_tracer(&self, tracer: Arc<Mutex<Vec<String>>>) {
        let mut file_tracer = self.file_tracer.lock();
        *file_tracer = tracer;
    }
}

impl FileSource for BBFSource {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let table_schema = self.table_schema.file_schema().clone();
        let projected_schema = base_config.projected_schema()?;
        let pruning_predicate = self
            .predicate
            .clone()
            .map(|p| PruningPredicate::try_new(p, table_schema.clone()))
            .transpose()?;
        Ok(Arc::new(BBFOpener::new(
            projected_schema,
            pruning_predicate,
            object_store,
            table_schema,
            self.file_tracer.lock().clone(),
            self.stream_partition_shares.clone(),
            self.global_metrics.clone(),
        )))
    }

    /// Any
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(BBFSource {
            batch_size,
            ..self.clone()
        })
    }
    /// Return execution plan metrics
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
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
        let source = BBFSource {
            projection: Some(merged),
            ..self.clone()
        };
        Ok(Some(Arc::new(source)))
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
