use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::{
    arrow::{
        batch::{any_dataset_as_record_batch_stream, any_dataset_as_row_size},
        pushdown_filter::PushdownFilter,
    },
    projection::DatasetProjection,
};
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileSource},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    execution::SendableRecordBatchStream,
    physical_expr::{PhysicalExpr, conjunction},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::{ExecutionPlanMetricsSet, SplitMetrics},
        stream::{BatchSplitStream, RecordBatchStreamAdapter},
    },
};
use futures::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use object_store::ObjectMeta;

use super::reader;

#[derive(Debug, Clone)]
pub struct TiffSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    override_schema: Option<SchemaRef>,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl TiffSource {
    pub fn new() -> Self {
        Self {
            schema_adapter_factory: None,
            override_schema: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            batch_size: 128 * 1024,
            predicate: None,
        }
    }
}

impl FileSource for TiffSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &datafusion::datasource::physical_plan::FileScanConfig,
        partition: usize,
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

        let schema_adapter = schema_adapter_factory.create(projected_schema, table_schema.clone());

        Arc::new(TiffOpener::new(
            table_schema,
            object_store,
            Arc::from(schema_adapter),
            self.batch_size,
            self.predicate.clone(),
            self.execution_plan_metrics.clone(),
            partition,
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_projection(
        &self,
        _config: &datafusion::datasource::physical_plan::FileScanConfig,
    ) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            projected_statistics: Some(statistics),
            ..self.clone()
        })
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        _config: &datafusion::datasource::physical_plan::FileScanConfig,
    ) -> datafusion::error::Result<Option<datafusion::datasource::physical_plan::FileScanConfig>>
    {
        Ok(None)
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
        "tiff"
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
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

struct TiffOpener {
    table_schema: SchemaRef,
    object_store: Arc<dyn object_store::ObjectStore>,
    schema_adapter: Arc<dyn SchemaAdapter>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    partition: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl TiffOpener {
    fn new(
        table_schema: SchemaRef,
        object_store: Arc<dyn object_store::ObjectStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        Self {
            table_schema,
            object_store,
            schema_adapter,
            batch_size,
            predicate,
            partition,
            metrics,
        }
    }

    async fn read_task(
        object: ObjectMeta,
        object_store: Arc<dyn object_store::ObjectStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let dataset = reader::open_dataset(object_store, object.clone())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open TIFF dataset {}: {e}",
                    object.location,
                ))
            })?;

        let file_schema: SchemaRef =
            beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to derive Arrow schema from TIFF dataset: {e}"
                    ))
                })?
                .into();

        let (schema_mapper, projection) = schema_adapter.map_schema(&file_schema)?;

        if projection.is_empty() {
            return Ok(any_dataset_as_row_size(dataset)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to compute row size for empty projection on TIFF dataset: {e}"
                    ))
                })?
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to read TIFF dataset with empty projection: {e}"
                    ))
                })
                .boxed());
        }

        let dataset = if projection.len() < file_schema.fields().len() {
            let proj = DatasetProjection {
                dimension_projection: None,
                index_projection: Some(projection),
            };
            dataset.project(&proj).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project TIFF dataset: {e}"
                ))
            })?
        } else {
            dataset
        };

        let pushdown_filter = predicate.map(PushdownFilter::new);
        let stream = any_dataset_as_record_batch_stream(dataset, batch_size, pushdown_filter)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Error reading TIFF as Arrow stream: {e}"
                ))
            })
            .and_then(move |batch| {
                let mapped = schema_mapper.map_batch(batch).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to map TIFF batch schema: {e}"
                    ))
                });
                futures::future::ready(mapped)
            })
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for TiffOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        _file: datafusion::datasource::listing::PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let fut = Self::read_task(
            file_meta.object_meta,
            self.object_store.clone(),
            self.schema_adapter.clone(),
            self.batch_size,
            self.predicate.clone(),
        )
        .boxed();

        Ok(fut)
    }
}
