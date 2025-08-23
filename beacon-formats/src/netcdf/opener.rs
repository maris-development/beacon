//! NetCDF file source and opener for DataFusion integration.
//!
//! This module provides a [`NetCDFFileSource`] for reading NetCDF files using
//! DataFusion's file source abstraction, and a [`NetCDFLocalFileOpener`] for
//! opening local NetCDF files and converting them to Arrow [`RecordBatch`]es.

use std::any::Any;
use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use datafusion::{
    catalog::memory::DataSourceExec,
    common::Statistics,
    datasource::{
        listing::{ListingTable, PartitionedFile},
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::{self, DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    error::Result,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{StreamExt, stream::BoxStream};
use object_store::{ObjectStore, local::LocalFileSystem};

/// File source for NetCDF files, supporting local file system access.
///
/// Implements [`FileSource`] for integration with DataFusion's physical plan.
#[derive(Debug, Clone)]
pub struct NetCDFFileSource {
    /// Local file system store for datasets.
    local_datasets_file_store: Arc<LocalFileSystem>,
    /// Prefix path for datasets.
    dataset_prefix: object_store::path::Path,
    /// Optional schema adapter factory.
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Optional schema override.
    override_schema: Option<SchemaRef>,
    /// Optional column projection.
    projection: Option<Vec<usize>>,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Projected statistics.
    projected_statistics: Statistics,
}

impl NetCDFFileSource {
    /// Creates a new `NetCDFFileSource`.
    ///
    /// # Arguments
    /// * `dataset_prefix` - Prefix path for datasets.
    /// * `local_datasets_file_store` - Local file system store.
    pub fn new(
        dataset_prefix: object_store::path::Path,
        local_datasets_file_store: Arc<LocalFileSystem>,
    ) -> Self {
        Self {
            dataset_prefix,
            local_datasets_file_store,
            override_schema: None,
            projection: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: Statistics::default(),
            schema_adapter_factory: None,
        }
    }
}

impl FileSource for NetCDFFileSource {
    /// Creates a file opener for the given object store and scan config.
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        if base_config.object_store_url.as_str().starts_with("file://") {
            let table_schema = self
                .override_schema
                .clone()
                .unwrap_or_else(|| base_config.file_schema.clone());
            let projected_schema = base_config.projected_schema();
            let schema_adapter_factory = self
                .schema_adapter_factory
                .clone()
                .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory::default()));
            let schema_adapter = schema_adapter_factory.create(projected_schema, table_schema);
            let arc_schema_adapter: Arc<dyn SchemaAdapter> = Arc::from(schema_adapter);

            Arc::new(NetCDFLocalFileOpener {
                store: self.local_datasets_file_store.clone(),
                schema_adapter: arc_schema_adapter,
            })
        } else {
            todo!("Implement remote file opener")
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            dataset_prefix: self.dataset_prefix.clone(),
            local_datasets_file_store: self.local_datasets_file_store.clone(),
            override_schema: Some(schema),
            projection: self.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self {
            dataset_prefix: self.dataset_prefix.clone(),
            local_datasets_file_store: self.local_datasets_file_store.clone(),
            override_schema: self.override_schema.clone(),
            projection: config.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            dataset_prefix: self.dataset_prefix.clone(),
            local_datasets_file_store: self.local_datasets_file_store.clone(),
            override_schema: self.override_schema.clone(),
            projection: self.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: statistics,
            schema_adapter_factory: self.schema_adapter_factory.clone(),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    fn file_type(&self) -> &str {
        "netcdf"
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            dataset_prefix: self.dataset_prefix.clone(),
            local_datasets_file_store: self.local_datasets_file_store.clone(),
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

/// File opener for local NetCDF files.
///
/// Converts NetCDF files to Arrow [`RecordBatch`]es using a schema adapter.
pub struct NetCDFLocalFileOpener {
    /// Local file system store.
    store: Arc<LocalFileSystem>,
    /// Schema adapter for mapping NetCDF schema to Arrow schema.
    schema_adapter: Arc<dyn SchemaAdapter>,
}

impl NetCDFLocalFileOpener {
    /// Converts an object store path to a local filesystem path.
    pub fn path_to_pathbuf(
        &self,
        path: &object_store::path::Path,
    ) -> Result<std::path::PathBuf, object_store::Error> {
        self.store.path_to_filesystem(path)
    }
}

impl FileOpener for NetCDFLocalFileOpener {
    /// Opens a NetCDF file and returns a stream of Arrow [`RecordBatch`]es.
    fn open(&self, file_meta: FileMeta, _file: PartitionedFile) -> Result<FileOpenFuture> {
        let path = self.path_to_pathbuf(file_meta.location())?;
        let file = beacon_arrow_netcdf::reader::NetCDFArrowReader::new(path)
            .expect("Failed to create NetCDFArrowReader");
        let file_schema = file.schema();
        let schema_adapter = self.schema_adapter.clone();

        Ok(Box::pin(async move {
            let (schema_mapper, projection) = schema_adapter
                .map_schema(&file_schema)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let stream = Box::pin(futures::stream::once(async move {
                let batch = file
                    .read_as_batch(Some(&projection))
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                let adapted_batch = schema_mapper
                    .map_batch(batch)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                Ok(adapted_batch)
            })) as BoxStream<'static, Result<RecordBatch, ArrowError>>;

            Ok(stream.boxed())
        }))
    }
}
