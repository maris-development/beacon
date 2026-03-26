use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    datasource::{
        physical_plan::{FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use object_store::ObjectStore;

#[derive(Clone, Debug)]
pub struct AtlasSource {
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
}

impl AtlasSource {
    pub fn new() -> Self {
        Self {
            schema_adapter_factory: None,
            override_schema: None,
            projection: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
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
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_schema(&self, schema: arrow::datatypes::SchemaRef) -> Arc<dyn FileSource> {
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

    fn with_schema_adapter_factory(
        &self,
        _factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(AtlasSource {
            schema_adapter_factory: Some(_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn file_type(&self) -> &str {
        "atlas"
    }
}
