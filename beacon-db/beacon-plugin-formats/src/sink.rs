//! The DataFusion sink that writes through a plugin format.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use beacon_datafusion_ext::listing_factory::LocalStoreRoots;
use beacon_plugin_sdk::file_format::{FormatReader, WriteRequest};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::{JoinSet, SpawnedTask};
use datafusion::datasource::file_format::write::demux::DemuxedStreamReceiver;
use datafusion::datasource::file_format::write::get_writer_schema;
use datafusion::datasource::physical_plan::{FileSink, FileSinkConfig};
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::Receiver;

/// The bytes that an upload reads from a local output file at one time.
const UPLOAD_CHUNK: usize = 8 * 1024 * 1024;
/// The parts of one upload that may be in flight at the same time.
const UPLOAD_PARTS: usize = 4;

/// How a writer that wants a local file gets one.
#[derive(Debug, Clone)]
pub(crate) struct LocalWrite {
    pub(crate) roots: LocalStoreRoots,
    /// One output is a folder, not one file.
    pub(crate) directory: bool,
}

/// Writes the output of a `COPY` with one plugin `write` call for each output file.
pub(crate) struct PluginFileSink {
    config: FileSinkConfig,
    name: String,
    reader: Arc<dyn FormatReader>,
    /// `false` if one output must stay in one file.
    split_output: bool,
    /// `None` if the writer writes through the store.
    local: Option<LocalWrite>,
}

impl PluginFileSink {
    pub(crate) fn new(
        config: FileSinkConfig,
        name: String,
        reader: Arc<dyn FormatReader>,
        split_output: bool,
        local: Option<LocalWrite>,
    ) -> Self {
        Self {
            config,
            name,
            reader,
            split_output,
            local,
        }
    }
}

/// One output file of a `COPY`.
struct OutputFile {
    reader: Arc<dyn FormatReader>,
    store: Arc<dyn ObjectStore>,
    store_url: ObjectStoreUrl,
    path: ObjectPath,
    schema: SchemaRef,
}

impl OutputFile {
    async fn call(
        &self,
        input: SendableRecordBatchStream,
        local_path: Option<PathBuf>,
    ) -> Result<u64> {
        let request = WriteRequest {
            path: self.path.clone(),
            schema: Arc::clone(&self.schema),
            local_path,
        };
        self.reader
            .write(Arc::clone(&self.store), request, input)
            .await
    }

    /// Writes the file: through the store, to its local file, or to a temp file and then up.
    async fn write(
        self,
        input: SendableRecordBatchStream,
        local: Option<LocalWrite>,
    ) -> Result<u64> {
        let Some(local) = local else {
            return self.call(input, None).await;
        };
        if let Some(target) = local.roots.local_path(&self.store_url, &self.path) {
            let folder = match local.directory {
                true => Some(target.as_path()),
                false => target.parent(),
            };
            if let Some(folder) = folder {
                tokio::fs::create_dir_all(folder).await?;
            }
            return self.call(input, Some(target)).await;
        }
        // The temp file or folder goes away on drop: after the upload, on error and on cancel.
        let mut temp = tempfile::Builder::new();
        temp.prefix("beacon-plugin-write-");
        let tmp_dir = local.roots.tmp_dir();
        if local.directory {
            let folder = temp.tempdir_in(&tmp_dir)?;
            let rows = self.call(input, Some(folder.path().to_path_buf())).await?;
            for (file, parts) in files_under(folder.path())? {
                let target = parts
                    .iter()
                    .fold(self.path.clone(), |path, part| path.join(part.as_str()));
                upload(&self.store, &file, &target).await?;
            }
            Ok(rows)
        } else {
            let file = temp.tempfile_in(&tmp_dir)?.into_temp_path();
            let rows = self.call(input, Some(file.to_path_buf())).await?;
            upload(&self.store, &file, &self.path).await?;
            Ok(rows)
        }
    }
}

/// Every file under `root`, with the names of its path below `root`.
fn files_under(root: &Path) -> Result<Vec<(PathBuf, Vec<String>)>> {
    let mut files = Vec::new();
    let mut folders = vec![(root.to_path_buf(), Vec::new())];
    while let Some((folder, parts)) = folders.pop() {
        for entry in std::fs::read_dir(&folder)? {
            let entry = entry?;
            let mut entry_parts = parts.clone();
            entry_parts.push(entry.file_name().to_string_lossy().into_owned());
            if entry.file_type()?.is_dir() {
                folders.push((entry.path(), entry_parts));
            } else {
                files.push((entry.path(), entry_parts));
            }
        }
    }
    Ok(files)
}

/// Uploads the local `file` to `path` in `store`.
async fn upload(store: &Arc<dyn ObjectStore>, file: &Path, path: &ObjectPath) -> Result<()> {
    let mut upload =
        WriteMultipart::new_with_chunk_size(store.put_multipart(path).await?, UPLOAD_CHUNK);
    match copy_file(file, &mut upload).await {
        Ok(()) => {
            upload.finish().await?;
            Ok(())
        }
        Err(error) => {
            // The copy error is the one to report.
            let _ = upload.abort().await;
            Err(error)
        }
    }
}

async fn copy_file(file: &Path, upload: &mut WriteMultipart) -> Result<()> {
    let mut file = tokio::fs::File::open(file).await?;
    let mut buffer = vec![0; UPLOAD_CHUNK];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        upload.wait_for_capacity(UPLOAD_PARTS).await?;
        upload.write(&buffer[..read]);
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
            let file = OutputFile {
                reader: Arc::clone(&self.reader),
                store: Arc::clone(&object_store),
                store_url: self.config.object_store_url.clone(),
                path: path.clone(),
                schema: Arc::clone(&schema),
            };
            let input = receiver_stream(Arc::clone(&schema), rx);
            let local = self.local.clone();
            let name = self.name.clone();
            tasks.spawn(async move {
                file.write(input, local)
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
