use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use crate::metrics::MetricsTracker;
use crate::query::temp_object::TempObject;
use datafusion::execution::SendableRecordBatchStream;
use futures::{future::BoxFuture, Future, Stream};

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

/// The result stream a client drains, counting rows and bytes as they go past and
/// recording the query's metrics when it ends.
pub struct ArrowOutputStream {
    stream: SendableRecordBatchStream,
    metrics: Arc<MetricsTracker>,
    store: Arc<crate::query_metrics_store::QueryMetricsStore>,
    /// The in-flight metrics write, once the inner stream has ended.
    ///
    /// Recording is a write to a managed table, so it cannot happen inline in a
    /// `poll`. Holding the future here — and polling it before reporting the end
    /// of the stream — means the metrics are durable by the time a caller sees
    /// the stream finish, which is what makes them readable immediately after.
    recording: Option<BoxFuture<'static, ()>>,
}

impl ArrowOutputStream {
    /// Built by `Runtime::run_query`, which owns the metrics store this records
    /// into — hence crate-visible, though the stream itself is public (the
    /// embedded Python client drains one).
    pub(crate) fn new(
        stream: SendableRecordBatchStream,
        metrics: Arc<MetricsTracker>,
        store: Arc<crate::query_metrics_store::QueryMetricsStore>,
    ) -> Self {
        Self {
            stream,
            metrics,
            store,
            recording: None,
        }
    }

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
        // The inner stream has ended; finish recording before ending this one.
        if let Some(recording) = self.recording.as_mut() {
            return match recording.as_mut().poll(cx) {
                std::task::Poll::Ready(()) => {
                    self.recording = None;
                    std::task::Poll::Ready(None)
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            };
        }

        let poll = std::pin::Pin::new(&mut self.stream).poll_next(cx);
        match &poll {
            // On receiving a batch, update the output metrics.
            std::task::Poll::Ready(Some(Ok(batch))) => {
                self.metrics.add_output_rows(batch.num_rows() as u64);
                self.metrics
                    .add_output_bytes(batch.get_array_memory_size() as u64);
            }
            // When the stream is finished, record the consolidated metrics. The
            // write is polled on the next turn (below), so this poll yields
            // rather than reporting the end straight away.
            std::task::Poll::Ready(None) => {
                let consolidated = self.metrics.get_consolidated_metrics();
                tracing::info!(
                    "Stream output size in bytes: {}",
                    consolidated.result_size_in_bytes
                );
                tracing::info!("Stream output rows: {}", consolidated.result_num_rows);
                let store = self.store.clone();
                self.recording = Some(Box::pin(async move { store.record(consolidated).await }));
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
            _ => {}
        }
        poll
    }
}
