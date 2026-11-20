//! The DataFusion sink that writes through a plugin format.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use beacon_plugin_sdk::file_format::{FormatReader, WriteRequest};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::{JoinSet, SpawnedTask};
use datafusion::datasource::file_format::write::demux::DemuxedStreamReceiver;
use datafusion::datasource::file_format::write::get_writer_schema;
use datafusion::datasource::physical_plan::{FileSink, FileSinkConfig};
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use object_store::ObjectStore;
use tokio::sync::mpsc::Receiver;

/// Writes the output of a `COPY` with one plugin `write` call for each output file.
pub(crate) struct PluginFileSink {
    config: FileSinkConfig,
    name: String,
    reader: Arc<dyn FormatReader>,
    /// `false` if one output must stay in one file.
    split_output: bool,
}

impl PluginFileSink {
    pub(crate) fn new(
        config: FileSinkConfig,
        name: String,
        reader: Arc<dyn FormatReader>,
        split_output: bool,
    ) -> Self {
        Self {
            config,
            name,
            reader,
            split_output,
        }
    }
}

impl fmt::Debug for PluginFileSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginFileSink")
            .field("name", &self.name)
            .finish()
    }
}

impl DisplayAs for PluginFileSink {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PluginFileSink({})", self.name)
    }
}

/// `context` with the demux settings that give one file for each output.
fn one_file_per_output(context: &TaskContext) -> TaskContext {
    let mut config = context.session_config().clone();
    let execution = &mut config.options_mut().execution;
    execution.minimum_parallel_output_files = 1;
    execution.soft_max_rows_per_output_file = usize::MAX;
    TaskContext::new(
        context.task_id(),
        context.session_id(),
        config,
        context.scalar_functions().clone(),
        context.higher_order_functions().clone(),
        context.aggregate_functions().clone(),
        context.window_functions().clone(),
        context.runtime_env(),
    )
}

/// The batches of one demuxed output file as a stream.
fn receiver_stream(schema: SchemaRef, rx: Receiver<RecordBatch>) -> SendableRecordBatchStream {
    let batches = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|batch| (Ok(batch), rx))
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, batches))
}

#[async_trait]
impl DataSink for PluginFileSink {
    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        if self.split_output {
            return FileSink::write_all(self, data, context).await;
        }
        let context = Arc::new(one_file_per_output(context));
        FileSink::write_all(self, data, &context).await
    }
}

#[async_trait]
impl FileSink for PluginFileSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        _context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let schema = get_writer_schema(&self.config);
        let mut tasks: JoinSet<Result<u64>> = JoinSet::new();
        while let Some((path, rx)) = file_stream_rx.recv().await {
            let reader = Arc::clone(&self.reader);
            let store = Arc::clone(&object_store);
            let input = receiver_stream(Arc::clone(&schema), rx);
            let request = WriteRequest {
                path: path.clone(),
                schema: Arc::clone(&schema),
                local_path: None,
            };
            let name = self.name.clone();
            tasks.spawn(async move {
                reader
                    .write(store, request, input)
                    .await
                    .map_err(|e| e.context(format!("file format {name}: cannot write {path}")))
            });
        }

        let mut rows = 0;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(written) => rows += written?,
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(error) => return Err(DataFusionError::ExecutionJoin(Box::new(error))),
            }
        }
        demux_task
            .join_unwind()
            .await
            .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;
        Ok(rows)
    }
}
