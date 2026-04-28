use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::{
    arrow::batch::any_dataset_as_record_batch_stream, projection::DatasetProjection,
};
use beacon_object_storage::DatasetsStore;
use datafusion::{
    common::Statistics,
    datasource::{
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileSource},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
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
    override_schema: Option<SchemaRef>,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    read_dimensions: Option<Vec<String>>,
}

impl NetCDFSource {
    pub fn new(
        datasets_object_store: Arc<DatasetsStore>,
        read_dimensions: Option<Vec<String>>,
    ) -> Self {
        Self {
            datasets_object_store,
            schema_adapter_factory: None,
            override_schema: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            read_dimensions,
        }
    }
}

impl FileSource for NetCDFSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &datafusion::datasource::physical_plan::FileScanConfig,
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

        let schema_adapter = schema_adapter_factory.create(projected_schema, table_schema);

        Arc::new(NetCDFOpener::new(
            self.datasets_object_store.clone(),
            Arc::from(schema_adapter),
            self.read_dimensions.clone(),
        ))
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
        "netcdf"
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

// ─── FileOpener ────────────────────────────────────────────────────────────

/// Opens a single NetCDF file and streams its contents as Arrow
/// [`RecordBatch`]es via [`any_dataset_as_record_batch_stream`].
struct NetCDFOpener {
    datasets_object_store: Arc<DatasetsStore>,
    schema_adapter: Arc<dyn SchemaAdapter>,
    read_dimensions: Option<Vec<String>>,
}

impl NetCDFOpener {
    fn new(
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        read_dimensions: Option<Vec<String>>,
    ) -> Self {
        Self {
            datasets_object_store,
            schema_adapter,
            read_dimensions,
        }
    }

    async fn read_task(
        object: ObjectMeta,
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        read_dimensions: Option<Vec<String>>,
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

        let stream = any_dataset_as_record_batch_stream(dataset)
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
    fn open(
        &self,
        file_meta: FileMeta,
        _file: datafusion::datasource::listing::PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let fut = Self::read_task(
            file_meta.object_meta,
            self.datasets_object_store.clone(),
            self.schema_adapter.clone(),
            self.read_dimensions.clone(),
        )
        .boxed();
        Ok(fut)
    }
}
