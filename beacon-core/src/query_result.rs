use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_planner::{metrics::ConsolidatedMetrics, prelude::MetricsTracker};
use datafusion::execution::SendableRecordBatchStream;
use futures::Stream;
use parking_lot::Mutex;
use tempfile::NamedTempFile;

pub struct QueryResult {
    pub query_output: QueryOutput,
    pub query_id: uuid::Uuid,
}

pub enum QueryOutput {
    File(QueryOutputFile),
    Stream(ArrowOutputStream),
}

#[derive(Debug)]
pub enum QueryOutputFile {
    Csv(NamedTempFile),
    Ipc(NamedTempFile),
    Json(NamedTempFile),
    Parquet(NamedTempFile),
    NetCDF(NamedTempFile),
    Odv(NamedTempFile),
    GeoParquet(NamedTempFile),
}

impl QueryOutputFile {
    pub fn size(&self) -> anyhow::Result<u64> {
        match self {
            QueryOutputFile::Csv(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::Ipc(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::Json(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::Parquet(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::NetCDF(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::Odv(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::GeoParquet(file) => Ok(file.path().metadata()?.len()),
        }
    }

    pub fn path(&self) -> &std::path::Path {
        match self {
            QueryOutputFile::Csv(file) => file.path(),
            QueryOutputFile::Ipc(file) => file.path(),
            QueryOutputFile::Json(file) => file.path(),
            QueryOutputFile::Parquet(file) => file.path(),
            QueryOutputFile::NetCDF(file) => file.path(),
            QueryOutputFile::Odv(file) => file.path(),
            QueryOutputFile::GeoParquet(file) => file.path(),
        }
    }
}

impl From<beacon_query::output::QueryOutputFile> for QueryOutputFile {
    fn from(value: beacon_query::output::QueryOutputFile) -> Self {
        match value {
            beacon_query::output::QueryOutputFile::Csv(file) => Self::Csv(file),
            beacon_query::output::QueryOutputFile::Ipc(file) => Self::Ipc(file),
            beacon_query::output::QueryOutputFile::Json(file) => Self::Json(file),
            beacon_query::output::QueryOutputFile::Parquet(file) => Self::Parquet(file),
            beacon_query::output::QueryOutputFile::NetCDF(file) => Self::NetCDF(file),
            beacon_query::output::QueryOutputFile::Odv(file) => Self::Odv(file),
            beacon_query::output::QueryOutputFile::GeoParquet(file) => Self::GeoParquet(file),
        }
    }
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
