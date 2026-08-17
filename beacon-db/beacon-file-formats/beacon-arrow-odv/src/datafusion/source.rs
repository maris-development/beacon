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
    physical_expr::{LexOrdering, projection::ProjectionExprs},
    physical_expr_adapter::BatchAdapterFactory,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
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
    /// The projection the scan pushed down, split into the file columns the
    /// decoder reads and a remainder applied on top of them.
    ///
    /// A `FileSource` that accepts a projection must apply it in full, so this
    /// source only reads plain columns and leaves everything else — aliases,
    /// computed expressions, partition columns — to [`ProjectionOpener`].
    projection: SplitProjection,
}

impl OdvSource {
    /// Creates a new [`OdvSource`] with the given table schema.
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = match projection {
            Some(projection) => SplitProjection::new(self.table_schema.file_schema(), &projection),
            None => SplitProjection::unprojected(&self.table_schema),
        };
        self
    }
}

#[async_trait::async_trait]
impl FileSource for OdvSource {
    /// Creates a [`FileOpener`] for ODV files.
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        // The columns the decoder reads, in file order. `ProjectionOpener`
        // derives its input schema the same way, so the two always agree.
        let read_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);

        let opener = Arc::new(OdvOpener {
            read_schema,
            object_store,
        }) as Arc<dyn FileOpener>;

        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
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
            schema_adapter_factory: Some(factory),
            ..self.clone()
        }))
    }

    /// Returns the schema adapter factory, if any.
    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> datafusion::error::Result<Option<Arc<dyn FileSource>>> {
        let merged = self.projection.source.try_merge(projection)?;
        let source = Self {
            projection: SplitProjection::new(self.table_schema.file_schema(), &merged),
            ..self.clone()
        };
        Ok(Some(Arc::new(source)))
    }
}

/// [`OdvOpener`] implements [`FileOpener`] for ODV ASCII files.
///
/// It uses a schema adapter and handles file compression.
struct OdvOpener {
    /// The plain file columns to read, in file order. Every mapped batch is
    /// produced in this schema; anything else the query asks for is applied by
    /// the [`ProjectionOpener`] wrapped around this one.
    read_schema: SchemaRef,
    /// Object store for file access.
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for OdvOpener {
    /// Opens an ODV file and returns a stream of record batches.
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let read_schema = self.read_schema.clone();
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
                .filter(|(_, f)| read_schema.index_of(f.name()).is_ok())
                .map(|(i, _)| i)
                .collect();

            // Adapt decoded batches onto the read schema: reorder, cast, and
            // null-fill columns this file lacks.
            let source_schema: SchemaRef = Arc::new(file_schema.project(&projection)?);
            let adapter = BatchAdapterFactory::new(read_schema).make_adapter(&source_schema)?;

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
