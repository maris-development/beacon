//! ODV ASCII file source implementation for DataFusion.
//!
//! This module provides an implementation of DataFusion's [`FileSource`] trait
//! for reading ODV ASCII files using the beacon-arrow-odv crate.

use std::{any::Any, sync::Arc};

use arrow::{datatypes::SchemaRef, error::ArrowError};
use beacon_arrow_odv::reader::AsyncOdvDecoder;
use datafusion::{
    common::Statistics,
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;

/// [`OdvSource`] implements [`FileSource`] for ODV ASCII files.
///
/// It supports schema overrides, column projection, statistics, and metrics.
#[derive(Debug, Clone)]
pub struct OdvSource {
    /// Optional factory for schema adapters.
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Optional schema override.
    override_schema: Option<SchemaRef>,
    /// Optional column projection.
    projection: Option<Vec<usize>>,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Optional projected statistics.
    projected_statistics: Option<Statistics>,
}

impl OdvSource {
    /// Creates a new [`OdvSource`] with default settings.
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

impl Default for OdvSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl FileSource for OdvSource {
    /// Creates a [`FileOpener`] for ODV files.
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        // Determine the schema to use (override or from config)
        let table_schema = self
            .override_schema
            .clone()
            .unwrap_or_else(|| base_config.file_schema.clone());
        let projected_schema = base_config.projected_schema();

        // Use provided schema adapter factory or default
        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));
        let schema_adapter = schema_adapter_factory.create(projected_schema, table_schema);

        Arc::new(OdvOpener {
            schema_adapter: Arc::from(schema_adapter),
            object_store,
            file_compression: base_config.file_compression_type,
        })
    }

    /// Returns a reference to self as [`Any`].
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns a new [`FileSource`] with the given batch size.
    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    /// Returns a new [`FileSource`] with the given schema.
    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(OdvSource {
            override_schema: Some(schema),
            ..self.clone()
        })
    }

    /// Returns a new [`FileSource`] with the given projection.
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(OdvSource {
            projection: config.projection.clone(),
            ..self.clone()
        })
    }

    /// Returns a new [`FileSource`] with the given statistics.
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(OdvSource {
            projected_statistics: Some(statistics),
            ..self.clone()
        })
    }

    /// Returns the execution plan metrics.
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    /// Returns the projected statistics, or computes unknown statistics if possible.
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

    /// Returns the file type string ("txt").
    fn file_type(&self) -> &str {
        "txt"
    }

    /// Returns a new [`FileSource`] with the given schema adapter factory.
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

    /// Returns the schema adapter factory, if any.
    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

/// [`OdvOpener`] implements [`FileOpener`] for ODV ASCII files.
///
/// It uses a schema adapter and handles file compression.
struct OdvOpener {
    /// Schema adapter for reading ODV files.
    schema_adapter: Arc<dyn SchemaAdapter>,
    /// Object store for file access.
    object_store: Arc<dyn ObjectStore>,
    /// Compression type for files.
    file_compression: FileCompressionType,
}

impl FileOpener for OdvOpener {
    /// Opens an ODV file and returns a stream of record batches.
    fn open(
        &self,
        file_meta: FileMeta,
        _file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let schema_adapter = self.schema_adapter.clone();
        let object_store = self.object_store.clone();
        let compression = self.file_compression;

        Ok(Box::pin(async move {
            // Open and decode the schema from the file
            let input_stream = object_store.get(file_meta.location()).await?.into_stream();
            let uncompressed_stream = compression
                .convert_to_compress_stream(Box::pin(input_stream.map_err(Into::into)))?;
            let odv_schema_mapper =
                AsyncOdvDecoder::decode_schema_mapper(uncompressed_stream.map_err(Into::into))
                    .await?;

            let file_schema = odv_schema_mapper.output_schema();

            // Map the file schema to the projected schema
            let (schema_mapper, projection) = schema_adapter
                .map_schema(&file_schema)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            // Open and decode the file body
            let body_stream = object_store.get(file_meta.location()).await?.into_stream();
            let uncompressed_body_stream = compression
                .convert_to_compress_stream(Box::pin(body_stream.map_err(Into::into)))?;

            // Decode batches and apply schema mapping
            let batch_stream = AsyncOdvDecoder::decode(
                uncompressed_body_stream.map_err(Into::into),
                Some(projection),
                Arc::new(odv_schema_mapper),
            )
            .await
            .map(move |maybe_batch| {
                maybe_batch
                    .and_then(|batch| schema_mapper.clone().map_batch(batch).map_err(Into::into))
            });

            Ok(batch_stream.boxed())
        }))
    }
}
