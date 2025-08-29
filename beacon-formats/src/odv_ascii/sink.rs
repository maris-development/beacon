use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_arrow_odv::writer::{AsyncOdvWriter, OdvOptions};
use datafusion::{
    datasource::{
        file_format::{file_compression_type::FileCompressionType, write::ObjectWriterBuilder},
        physical_plan::FileSinkConfig,
        sink::DataSink,
    },
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt;
use object_store::ObjectStore;
use tokio::io::AsyncWriteExt as _;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// A DataFusion sink for writing data in ODV ASCII format to an object store.
///
/// This struct implements the [`DataSink`] trait, allowing it to be used as a
/// target for DataFusion's physical plan output. It writes Arrow record batches
/// to the specified object store using the ODV format.
pub struct OdvSink {
    /// Configuration for file sink output.
    config: FileSinkConfig,
    /// Optional ODV writer options.
    odv_options: Option<OdvOptions>,
    /// Object store to write output files to.
    object_store: Arc<dyn ObjectStore>,
}

impl OdvSink {
    /// Creates a new `OdvSink`.
    pub fn new(
        config: FileSinkConfig,
        odv_options: Option<OdvOptions>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        OdvSink {
            config,
            odv_options,
            object_store,
        }
    }
}

impl std::fmt::Debug for OdvSink {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OdvSink")
    }
}

impl DisplayAs for OdvSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OdvSink")
    }
}

#[async_trait::async_trait]
impl DataSink for OdvSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    /// Writes all record batches from the stream to the object store in ODV format.
    ///
    /// # Arguments
    /// * `data` - Stream of Arrow record batches to write.
    /// * `_context` - Execution context (unused).
    ///
    /// # Returns
    /// Returns the total number of rows written.
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let arrow_schema = self.config.output_schema().clone();
        let mut rows_written: u64 = 0;

        // Use provided ODV options or infer from schema.
        let odv_options = self.odv_options.clone().unwrap_or(
            OdvOptions::try_from_arrow_schema(arrow_schema.clone()).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to implicitly define ODV output options: {}",
                    e
                ))
            })?,
        );

        // Get output path for writing.
        let output_path = self.config.table_paths[0].prefix();

        // Build an object writer for the output path.
        let object_writer = ObjectWriterBuilder::new(
            FileCompressionType::UNCOMPRESSED,
            output_path,
            self.object_store.clone(),
        )
        .build()
        .unwrap()
        .compat_write();

        // Create the ODV writer.
        let mut odv_writer = AsyncOdvWriter::new_from_dyn(
            Box::new(object_writer),
            arrow_schema.clone(),
            odv_options,
        )
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create ODV writer: {}",
                e
            ))
        })?;

        // Pin the stream for iteration.
        let mut stream = std::pin::pin!(data);

        // Write each batch to the ODV writer.
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            rows_written += batch.num_rows() as u64;
            odv_writer.write(batch).await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to write ODV batch: {}",
                    e
                ))
            })?;
        }

        // Finalize and flush the writer.
        let mut object_writer = odv_writer.finish().await.unwrap().into_inner();
        object_writer.flush().await.unwrap();
        object_writer.shutdown().await.unwrap();

        Ok(rows_written)
    }
}
