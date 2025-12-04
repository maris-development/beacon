use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_planner::{metrics::ConsolidatedMetrics, prelude::MetricsTracker};
use beacon_query::output::QueryOutputFile;
use datafusion::execution::SendableRecordBatchStream;
use futures::Stream;
use parking_lot::Mutex;

pub struct QueryResult {
    pub query_output: QueryOutput,
    pub query_id: uuid::Uuid,
}

pub enum QueryOutput {
    File(QueryOutputFile),
    Stream(ArrowOutputStream),
}

pub struct ArrowOutputStream {
    pub stream: SendableRecordBatchStream,
    pub metrics: Arc<MetricsTracker>,
    pub all_consolidated_metrics: Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>,
}

impl ArrowOutputStream {
    pub fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl Stream for ArrowOutputStream {
    type Item = datafusion::error::Result<arrow::record_batch::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let poll = std::pin::Pin::new(&mut self.stream).poll_next(cx);
        match &poll {
            // On receiving a batch, update the output metrics.
            std::task::Poll::Ready(Some(Ok(batch))) => {
                self.metrics.add_output_rows(batch.num_rows() as u64);
                self.metrics
                    .add_output_bytes(batch.get_array_memory_size() as u64);
            }
            // When the stream is finished, store the consolidated metrics.
            std::task::Poll::Ready(None) => {
                let consolidated = self.metrics.get_consolidated_metrics();
                tracing::info!(
                    "Stream output size in bytes: {}",
                    consolidated.result_size_in_bytes
                );
                tracing::info!("Stream output rows: {}", consolidated.result_num_rows);
                self.all_consolidated_metrics
                    .lock()
                    .insert(self.metrics.query_id, consolidated);
            }
            _ => {}
        }
        poll
    }
}
