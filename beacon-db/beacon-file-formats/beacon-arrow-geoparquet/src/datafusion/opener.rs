//! [`FileOpener`] that streams a single GeoParquet file as Arrow
//! [`RecordBatch`]es with geometry columns decoded to native GeoArrow.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::{DataFusionError, Result},
    physical_expr_adapter::BatchAdapterFactory,
};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use geoparquet::reader::GeoParquetRecordBatchStream;
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::reader;

/// Opens GeoParquet files and yields batches matching the projected schema.
pub struct GeoParquetOpener {
    object_store: Arc<dyn ObjectStore>,
    /// Schema of the columns the scan requested, in output order.
    projected_schema: SchemaRef,
    batch_size: usize,
}

impl GeoParquetOpener {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
    ) -> Self {
        Self {
            object_store,
            projected_schema,
            batch_size,
        }
    }

    async fn read_task(
        object: ObjectMeta,
        object_store: Arc<dyn ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let builder = reader::stream_builder(object_store, &object).await?;
        // Full GeoArrow schema of this file (geometry decoded to native types).
        let file_schema = reader::output_schema(&builder)?;

        let parquet_stream = builder
            .with_batch_size(batch_size)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Wraps the raw Parquet stream to apply GeoArrow metadata and parse
        // geometry columns onto every emitted batch.
        let geo_stream = GeoParquetRecordBatchStream::try_new(parquet_stream, file_schema.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Adapt the file's GeoArrow schema onto the projected output schema:
        // select/reorder the requested columns and null-fill any this file
        // lacks. This is how column projection is applied — we read the full
        // file schema and project in Arrow.
        let adapter = BatchAdapterFactory::new(projected_schema).make_adapter(&file_schema)?;

        let stream = geo_stream
            .map(move |batch| {
                let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;
                adapter.adapt_batch(&batch).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to adapt GeoParquet batch: {e}"))
                })
            })
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for GeoParquetOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let fut = Self::read_task(
            file.object_meta,
            self.object_store.clone(),
            self.projected_schema.clone(),
            self.batch_size,
        )
        .boxed();

        Ok(fut)
    }
}
