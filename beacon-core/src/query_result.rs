use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use crate::metrics::{ConsolidatedMetrics, MetricsTracker};
use crate::query::temp_object::TempObject;
use datafusion::execution::SendableRecordBatchStream;
use futures::Stream;
use parking_lot::Mutex;

pub struct QueryResult {
    pub query_output: QueryOutput,
    pub query_id: uuid::Uuid,
}

impl QueryResult {
    /// Extract the (metrics-tracked) record-batch stream, erroring if the result
    /// is file-backed. Used by transports that only stream results (e.g. Flight SQL).
    pub fn into_record_stream(self) -> anyhow::Result<ArrowOutputStream> {
        match self.query_output {
            QueryOutput::Stream(stream) => Ok(stream),
            QueryOutput::File(_) => {
                anyhow::bail!("expected a streamed query result, got a file output")
            }
        }
    }
}

pub enum QueryOutput {
    File(QueryOutputFile),
    Stream(ArrowOutputStream),
}

/// The concrete format a [`QueryOutputFile`] was written in. Carried for the
/// transport (MIME type / download filename); the path and size are format-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFileKind {
    Csv,
    Ipc,
    Json,
    Parquet,
    GeoParquet,
    NetCDF,
    Odv,
}

impl OutputFileKind {
    /// The MIME type a download transport reports for this format.
    pub fn content_type(&self) -> &'static str {
        match self {
            OutputFileKind::Csv => "text/csv",
            OutputFileKind::Ipc => "application/vnd.apache.arrow.file",
            OutputFileKind::Json => "application/json",
            OutputFileKind::Parquet => "application/vnd.apache.parquet",
            OutputFileKind::GeoParquet => "application/vnd.apache.parquet",
            OutputFileKind::NetCDF => "application/x-netcdf",
            OutputFileKind::Odv => "application/zip",
        }
    }

    /// The file extension (including the leading dot) for this format's output.
    pub fn suggested_extension(&self) -> &'static str {
        match self {
            OutputFileKind::Csv => ".csv",
            OutputFileKind::Ipc => ".arrow",
            OutputFileKind::Json => ".json",
            OutputFileKind::Parquet => ".parquet",
            OutputFileKind::GeoParquet => ".parquet",
            OutputFileKind::NetCDF => ".nc",
            OutputFileKind::Odv => ".zip",
        }
    }
}

/// A query result written to a temporary file, tagged by its format.
///
/// The [`TempObject`] owns the file's lifetime (removed on drop) and reconciles the
/// COPY write location with the read-back path; `kind` is metadata for the transport.
#[derive(Debug)]
pub struct QueryOutputFile {
    kind: OutputFileKind,
    temp: TempObject,
}

impl QueryOutputFile {
    pub fn new(kind: OutputFileKind, temp: TempObject) -> Self {
        Self { kind, temp }
    }

    pub fn kind(&self) -> OutputFileKind {
        self.kind
    }

    pub fn size(&self) -> std::io::Result<u64> {
        Ok(self.temp.path().metadata()?.len())
    }

    pub fn path(&self) -> &std::path::Path {
        self.temp.path()
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
