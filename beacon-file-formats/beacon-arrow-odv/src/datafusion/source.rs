//! ODV ASCII file source implementation for DataFusion.
//!
//! This module provides an implementation of DataFusion's [`FileSource`] trait
//! for reading ODV ASCII files using the beacon-arrow-odv crate.

use std::{any::Any, sync::Arc};

use datafusion::{
    common::exec_datafusion_err,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    physical_expr::{projection::ProjectionExprs, LexOrdering},
    physical_expr_adapter::BatchAdapterFactory,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt};

use arrow::datatypes::SchemaRef;

use crate::reader::AsyncOdvDecoder;

use super::OdvFormat;

/// [`OdvSource`] implements [`FileSource`] for ODV ASCII files.
///
/// It supports schema overrides, column projection, statistics, and metrics.
#[derive(Debug, Clone)]
pub struct OdvSource {
    /// Optional factory for schema adapters.
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// The table schema (file schema + partition columns).
    table_schema: TableSchema,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl OdvSource {
    /// Creates a new [`OdvSource`] with the given table schema.
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
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

#[async_trait::async_trait]
impl FileSource for OdvSource {
    /// Creates a [`FileOpener`] for ODV files.
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(OdvOpener {
            projected_schema,
            object_store,
        }))
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
        _config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        Ok(None)
    }

    /// Returns a reference to self as [`Any`].
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns a new [`FileSource`] with the given batch size.
    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    /// Returns the execution plan metrics.
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
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
            table_schema: self.table_schema.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            schema_adapter_factory: Some(factory),
            projection: self.projection.clone(),
        }))
    }

    /// Returns the schema adapter factory, if any.
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
}

/// [`OdvOpener`] implements [`FileOpener`] for ODV ASCII files.
///
/// It uses a schema adapter and handles file compression.
struct OdvOpener {
    /// The projected output schema each mapped batch is produced in.
    projected_schema: SchemaRef,
    /// Object store for file access.
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for OdvOpener {
    /// Opens an ODV file and returns a stream of record batches.
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let projected_schema = self.projected_schema.clone();
        let object_store = self.object_store.clone();
        let compression = OdvFormat::infer_compression(&file.object_meta);

        Ok(Box::pin(async move {
            // Open and decode the schema from the file
            let input_stream = object_store
                .get(&file.object_meta.location)
                .await?
                .into_stream();
            let uncompressed_stream =
                compression.convert_stream(Box::pin(input_stream.map_err(Into::into)))?;
            let odv_schema_mapper =
                AsyncOdvDecoder::decode_schema_mapper(uncompressed_stream.map_err(Into::into))
                    .map_err(|e| exec_datafusion_err!("Failed to decode ODV schema: {}", e))
                    .await?;

            let file_schema = odv_schema_mapper.output_schema();

            // Columns of this file that the query needs, in file order — used
            // both to prune the decode and as the source schema for the adapter.
            let projection: Vec<usize> = file_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| projected_schema.index_of(f.name()).is_ok())
                .map(|(i, _)| i)
                .collect();

            // Adapt decoded batches onto the projected output schema: reorder,
            // cast, and null-fill columns the file lacks.
            let source_schema: SchemaRef = Arc::new(file_schema.project(&projection)?);
            let adapter =
                BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;

            // Open and decode the file body
            let body_stream = object_store
                .get(&file.object_meta.location)
                .await?
                .into_stream();
            let uncompressed_body_stream =
                compression.convert_stream(Box::pin(body_stream.map_err(Into::into)))?;

            // Decode batches and apply schema mapping
            let batch_stream = AsyncOdvDecoder::decode(
                uncompressed_body_stream.map_err(Into::into),
                Some(projection),
                Arc::new(odv_schema_mapper),
            )
            .await
            .map(move |maybe_batch| {
                maybe_batch
                    .map_err(|e| exec_datafusion_err!("Failed to decode ODV batch: {}", e))
                    .and_then(|batch| adapter.adapt_batch(&batch))
            });
            let stream = batch_stream
                .map_err(|e| exec_datafusion_err!("Error reading ODV ASCII file: {}", e))
                .boxed();
            Ok(stream)
        }))
    }
}
