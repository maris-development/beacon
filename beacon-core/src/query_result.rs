use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_planner::{metrics::ConsolidatedMetrics, prelude::MetricsTracker};
use beacon_query::output::QueryOutputFile;
use datafusion::execution::SendableRecordBatchStream;
use futures::Stream;
use parking_lot::Mutex;
use sysinfo::{ProcessesToUpdate, System};

use crate::telemetry;

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
                let batch_memory_bytes = batch.get_array_memory_size() as u64;
                self.metrics.add_output_rows(batch.num_rows() as u64);
                self.metrics.add_output_bytes(batch_memory_bytes);
                self.metrics.observe_output_batch_bytes(batch_memory_bytes);
            }
            // When the stream is finished, store the consolidated metrics.
            std::task::Poll::Ready(None) => {
                if let Some(end_memory_bytes) = current_process_memory_bytes() {
                    self.metrics.set_process_memory_end_bytes(end_memory_bytes);
                }

                let consolidated = self.metrics.get_consolidated_metrics();
                telemetry::record_query_completion("stream", &consolidated);

                tracing::info!(
                    telemetry_event = "query.end",
                    query_id = %self.metrics.query_id,
                    result_size_in_bytes = consolidated.result_size_in_bytes,
                    result_rows = consolidated.result_num_rows,
                    peak_output_batch_bytes = consolidated.peak_output_batch_bytes,
                    process_memory_start_bytes = consolidated.process_memory_start_bytes,
                    process_memory_end_bytes = consolidated.process_memory_end_bytes,
                    process_memory_delta_bytes = consolidated.process_memory_delta_bytes,
                    execution_time_ms = consolidated.execution_time_ms,
                    "Query stream completed"
                );
                self.all_consolidated_metrics
                    .lock()
                    .insert(self.metrics.query_id, consolidated);
            }
            std::task::Poll::Ready(Some(Err(error))) => {
                telemetry::record_query_failure("stream", "poll_next");
                tracing::error!(
                    query_id = %self.metrics.query_id,
                    error = %error,
                    "Query stream failed"
                );
            }
            _ => {}
        }
        poll
    }
}

fn current_process_memory_bytes() -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map(|process| process.memory())
}
