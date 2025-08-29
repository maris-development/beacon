use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::{SchemaAdapter, SchemaAdapterFactory},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use object_store::ObjectStore;

#[derive(Debug, Clone)]
pub struct OdvSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Optional schema override.
    override_schema: Option<SchemaRef>,
    /// Optional column projection.
    projection: Option<Vec<usize>>,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Projected statistics.
    projected_statistics: Option<Statistics>,
}

#[async_trait::async_trait]
impl FileSource for OdvSource {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        todo!()
    }
    /// Any
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(OdvSource { ..self.clone() })
    }
    /// Initialize new instance with a new schema
    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(OdvSource {
            override_schema: Some(schema),
            ..self.clone()
        })
    }
    /// Initialize new instance with projection information
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(OdvSource {
            projection: config.projection.clone(),
            ..self.clone()
        })
    }
    /// Initialize new instance with projected statistics
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(OdvSource {
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
    /// String representation of file source such as "csv", "json", "parquet"
    fn file_type(&self) -> &str {
        "txt"
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
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

struct OdvOpener {
    /// Schema adapter for reading ODV files
    schema_adapter: Arc<dyn SchemaAdapter>,
}

impl FileOpener for OdvOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        todo!()
    }
}
