use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::exec_datafusion_err,
    datasource::sink::DataSink,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, metrics::MetricsSet},
};
use futures::{TryFutureExt, TryStreamExt};

use crate::arrow_stream::stream::ArrowBatchesStreamSender;

#[derive(Debug)]
pub struct ArrowBatchesStreamSink {
    pub schema: SchemaRef,
    // Sink target
    pub stream_sender: Arc<ArrowBatchesStreamSender>,
}

impl ArrowBatchesStreamSink {
    pub async fn new(
        schema: SchemaRef,
        stream_sender: Arc<ArrowBatchesStreamSender>,
    ) -> Result<Self, datafusion::error::DataFusionError> {
        stream_sender
            .schema
            .initialize(schema.clone())
            .map_err(|e| exec_datafusion_err!("Schema Channel Error: {}", e))
            .await?;
        Ok(Self {
            schema,
            stream_sender,
        })
    }
}

impl DisplayAs for ArrowBatchesStreamSink {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ArrowBatchesStreamSink")
    }
}

#[async_trait::async_trait]
impl DataSink for ArrowBatchesStreamSink {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Return a snapshot of the [MetricsSet] for this
    /// [DataSink].
    ///
    /// See [ExecutionPlan::metrics()] for more details
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Returns the sink schema
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    // TODO add desired input ordering
    // How does this sink want its input ordered?

    /// Writes the data to the sink, returns the number of values written
    ///
    /// This method will be called exactly once during each DML
    /// statement. Thus prior to return, the sink should do any commit
    /// or rollback required.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let mut total_written = 0;
        let sender = self.stream_sender.inner_sender.clone();

        while let Some(batch) = data.try_next().await? {
            let num_rows = batch.num_rows() as u64;
            total_written += num_rows;

            // Send the batch to the stream
            sender
                .send(Ok(batch))
                .map_err(|e| exec_datafusion_err!("Failed to send batch to stream: {}", e))?;
        }

        Ok(total_written)
    }
}
