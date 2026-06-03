use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::{
    arrow::{
        batch::any_dataset_as_record_batch_stream,
        metrics::DatasetReadMetrics,
        pushdown_filter::PushdownFilter,
    },
    projection::DatasetProjection,
};
use beacon_object_storage::DatasetsStore;
use datafusion::{
    common::{pruning::PrunableStatistics, Statistics},
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
        table_schema::TableSchema,
    },
    physical_expr::{conjunction, PhysicalExpr},
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
        EmptyRecordBatchStream,
    },
};
use futures::{stream::BoxStream, FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectMeta;

use super::reader;

/// DataFusion [`FileSource`] for NetCDF (`.nc`) files.
///
/// Integrates the `beacon_arrow_netcdf` reader with DataFusion's file scan
/// pipeline via a [`FileOpener`].
#[derive(Debug, Clone)]
pub struct NetCDFSource {
    datasets_object_store: Arc<DatasetsStore>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl NetCDFSource {
    pub fn new(
        datasets_object_store: Arc<DatasetsStore>,
        read_dimensions: Option<Vec<String>>,
        table_schema: TableSchema,
    ) -> Self {
        Self {
            datasets_object_store,
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            read_dimensions,
            batch_size: usize::MAX,
            predicate: None,
        }
    }
}

impl FileSource for NetCDFSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema().clone();
        let projected_schema = base_config.projected_schema()?;
        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));

        let schema_adapter = schema_adapter_factory.create(projected_schema, file_schema.clone());

        Ok(Arc::new(NetCDFOpener::new(
            self.datasets_object_store.clone(),
            Arc::from(schema_adapter),
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            file_schema,
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
        "netcdf"
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

// ─── FileOpener ────────────────────────────────────────────────────────────

/// Opens a single NetCDF file and streams its contents as Arrow
/// [`RecordBatch`]es via [`any_dataset_as_record_batch_stream`].
struct NetCDFOpener {
    datasets_object_store: Arc<DatasetsStore>,
    schema_adapter: Arc<dyn SchemaAdapter>,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<PruningPredicate>,
    table_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
}

impl NetCDFOpener {
    fn new(
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        table_schema: SchemaRef,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        let pruning_predicate = predicate
            .as_ref()
            .and_then(|pred| PruningPredicate::try_new(pred.clone(), table_schema.clone()).ok());

        Self {
            datasets_object_store,
            schema_adapter,
            read_dimensions,
            batch_size,
            predicate,
            pruning_predicate,
            table_schema,
            metrics,
            partition,
        }
    }

    async fn read_task(
        object: ObjectMeta,
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: Option<DatasetReadMetrics>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let dataset = reader::open_dataset(datasets_object_store, object.clone())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open NetCDF dataset {}: {e}",
                    object.location,
                ))
            })?;

        // Apply dimension projection before deriving the file schema.
        let dataset = if let Some(dims) = read_dimensions {
            let proj = DatasetProjection {
                dimension_projection: Some(dims),
                index_projection: None,
            };
            dataset.project(&proj).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project NetCDF dataset with dimensions: {e}"
                ))
            })?
        } else {
            dataset
        };

        let file_schema: SchemaRef =
            beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to derive Arrow schema from NetCDF dataset: {e}"
                    ))
                })?
                .into();

        let (schema_mapper, projection) = schema_adapter.map_schema(&file_schema)?;

        if projection.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }

        let dataset = if projection.len() < file_schema.fields().len() {
            let proj = DatasetProjection {
                dimension_projection: None,
                index_projection: Some(projection),
            };
            dataset.project(&proj).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project NetCDF dataset: {e}"
                ))
            })?
        } else {
            dataset
        };

        let pushdown_filter = predicate.map(PushdownFilter::new);
        let stream = any_dataset_as_record_batch_stream(dataset, batch_size, pushdown_filter, metrics)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Error reading NetCDF as Arrow stream: {e}"
                ))
            })
            .and_then(move |batch| {
                let mapped = schema_mapper.map_batch(batch).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to map NetCDF batch schema: {e}"
                    ))
                });
                futures::future::ready(mapped)
            })
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for NetCDFOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        if let (Some(stats), Some(prune)) = (&file.statistics, &self.pruning_predicate) {
            let result = prune.prune(&PrunableStatistics::new(
                vec![Arc::clone(stats)],
                self.table_schema.clone(),
            ))?[0];
            if !result {
                tracing::debug!(
                    "Pruning NetCDF file {} based on statistics.",
                    file.object_meta.location
                );
                // File is pruned, return empty stream.
                let stream = EmptyRecordBatchStream::new(self.table_schema.clone()).boxed();

                return Ok(futures::future::ready(Ok(stream)).boxed());
            }
        };

        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let fut = Self::read_task(
            file.object_meta,
            self.datasets_object_store.clone(),
            self.schema_adapter.clone(),
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            metrics,
        )
        .boxed();
        Ok(fut)
    }
}
