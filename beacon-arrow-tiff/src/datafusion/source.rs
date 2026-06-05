use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::{
    arrow::{
        batch::{any_dataset_as_record_batch_stream, any_dataset_as_row_size},
        metrics::DatasetReadMetrics,
        pushdown_filter::PushdownFilter,
    },
    projection::DatasetProjection,
};
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    execution::SendableRecordBatchStream,
    physical_expr::{PhysicalExpr, conjunction, projection::ProjectionExprs},
    physical_expr_adapter::BatchAdapterFactory,
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
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl TiffSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: 128 * 1024,
            predicate: None,
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
}

impl FileSource for TiffSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema().clone();
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(TiffOpener::new(
            file_schema,
            object_store,
            projected_schema,
            self.batch_size,
            self.predicate.clone(),
            self.execution_plan_metrics.clone(),
            partition,
        )))
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

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        _config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        Ok(None)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "tiff"
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

struct TiffOpener {
    table_schema: SchemaRef,
    object_store: Arc<dyn object_store::ObjectStore>,
    projected_schema: SchemaRef,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    partition: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl TiffOpener {
    fn new(
        table_schema: SchemaRef,
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        Self {
            table_schema,
            object_store,
            projected_schema,
            batch_size,
            predicate,
            partition,
            metrics,
        }
    }

    async fn read_task(
        object: ObjectMeta,
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: Option<DatasetReadMetrics>,
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

        // Columns of this file that the query needs, in file order — used both
        // to prune the read and as the source schema for the batch adapter.
        let projection: Vec<usize> = file_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| projected_schema.index_of(f.name()).is_ok())
            .map(|(i, _)| i)
            .collect();

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

        // Adapt batches (read with `projection`) onto the projected output
        // schema: reorder, cast, and null-fill columns this file lacks.
        let source_schema: SchemaRef = Arc::new(file_schema.project(&projection)?);
        let adapter = BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;

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
        let stream = any_dataset_as_record_batch_stream(dataset, batch_size, pushdown_filter, metrics)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Error reading TIFF as Arrow stream: {e}"
                ))
            })
            .and_then(move |batch| {
                let mapped = adapter.adapt_batch(&batch).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to adapt TIFF batch schema: {e}"
                    ))
                });
                futures::future::ready(mapped)
            })
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for TiffOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let fut = Self::read_task(
            file.object_meta,
            self.object_store.clone(),
            self.projected_schema.clone(),
            self.batch_size,
            self.predicate.clone(),
            metrics,
        )
        .boxed();

        Ok(fut)
    }
}
