use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use beacon_common::util::async_once::AsyncOnce;
use datafusion::{
    common::exec_datafusion_err, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
};

#[derive(Debug)]
pub struct DeferredBatchStream {
    pub schema: Arc<AsyncOnce<SchemaRef>>,
    pub batches_stream: flume::Receiver<datafusion::error::Result<RecordBatch>>,
}

impl DeferredBatchStream {
    pub fn new() -> Self {
        let async_once = Arc::new(AsyncOnce::new());
        let (batch_tx, batch_rx) = flume::bounded(beacon_config::CONFIG.deferred_stream_capacity);

        Self {
            schema: async_once,
            batches_stream: batch_rx,
        }
    }

    pub async fn into_stream(self) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self.schema.get().await;
        let adapter = RecordBatchStreamAdapter::new(schema, self.batches_stream.into_stream());

        Ok(Box::pin(adapter) as SendableRecordBatchStream)
    }
}

impl Default for DeferredBatchStream {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ArrowBatchesStreamSender {
    pub schema: Arc<AsyncOnce<SchemaRef>>,
    pub inner_sender: flume::Sender<datafusion::error::Result<RecordBatch>>,
}
