//! DataFusion sink that writes query results as a flat Zarr v3 store.
//!
//! [`ZarrSink`] writes each column as a 1-D array over the `obs` dimension
//! (see [`crate::writer`]). The store is a directory. With
//! [`ZarrWriteOptions::zip_output`] set, the sink builds it in a scratch
//! directory and packs it into one zip file at the output path.

use std::any::Any;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::{
    common::exec_datafusion_err,
    datasource::{physical_plan::FileSinkConfig, sink::DataSink},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt;

use crate::writer::{ZarrTableWriter, ZarrWriteOptions, archive_store, unsupported_columns};

/// Prefix of the scratch store directory. The runtime sweeps leftovers with
/// this prefix at startup.
const SCRATCH_PREFIX: &str = "beacon_out_";

/// Batches the input side may queue ahead of the writer thread.
const QUEUED_BATCHES: usize = 2;

/// [`DataSink`] that writes batches to a flat zarr store.
#[derive(Debug, Clone)]
pub struct ZarrSink {
    sink_config: FileSinkConfig,
    options: ZarrWriteOptions,
    /// Directory the output is written to: the configured tmp store root.
    output_dir: PathBuf,
}

impl ZarrSink {
    pub fn new(
        sink_config: FileSinkConfig,
        options: ZarrWriteOptions,
        output_dir: PathBuf,
    ) -> Self {
        Self {
            sink_config,
            options,
            output_dir,
        }
    }
}

impl DisplayAs for ZarrSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ZarrSink")
    }
}

#[async_trait::async_trait]
impl DataSink for ZarrSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.sink_config.output_schema()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let schema = self.sink_config.output_schema().clone();
        let unsupported = unsupported_columns(&schema);
        if !unsupported.is_empty() {
            return Err(exec_datafusion_err!(
                "Zarr output does not support these columns: {}",
                unsupported.join(", ")
            ));
        }

        let target = self
            .output_dir
            .join(self.sink_config.table_paths[0].prefix().as_ref());
        // The scratch directory is removed when this guard drops, also on error.
        let scratch = if self.options.zip_output {
            Some(
                tempfile::Builder::new()
                    .prefix(SCRATCH_PREFIX)
                    .suffix(".zarr")
                    .tempdir_in(&self.output_dir)
                    .map_err(|e| exec_datafusion_err!("create zarr scratch directory: {e}"))?,
            )
        } else {
            if target.exists() {
                return Err(exec_datafusion_err!(
                    "Zarr output target {} already exists",
                    target.display()
                ));
            }
            None
        };
        let store_dir = scratch
            .as_ref()
            .map_or_else(|| target.clone(), |dir| dir.path().to_path_buf());
        let zip_target = scratch.as_ref().map(|_| target.clone());
        tracing::info!(store = %store_dir.display(), "Writing flat Zarr store");

        // zarrs writes through blocking file I/O and compresses each chunk, so
        // the writer runs on a blocking thread fed through a bounded channel.
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<RecordBatch>(QUEUED_BATCHES);
        let input_failed = Arc::new(AtomicBool::new(false));
        let writer_input_failed = input_failed.clone();
        let chunk_size = self.options.chunk_size;
        let writer = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
            let mut writer = ZarrTableWriter::try_new(&store_dir, schema, chunk_size)?;
            while let Some(batch) = receiver.blocking_recv() {
                writer.write_batch(batch)?;
            }
            // A failed input must not leave a store that looks complete.
            if writer_input_failed.load(Ordering::Acquire) {
                anyhow::bail!("the query input failed");
            }
            let rows = writer.finish()?;
            if let Some(zip_target) = zip_target {
                let file = std::fs::File::create(&zip_target)?;
                archive_store(&store_dir, std::io::BufWriter::new(file))?;
            }
            Ok(rows)
        });

        let mut input = std::pin::pin!(data);
        let mut input_error = None;
        while let Some(batch) = input.next().await {
            match batch {
                Ok(batch) => {
                    // A closed channel means the writer failed; its error follows.
                    if sender.send(batch).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    input_failed.store(true, Ordering::Release);
                    input_error = Some(error);
                    break;
                }
            }
        }
        drop(sender);

        let written = writer
            .await
            .map_err(|e| exec_datafusion_err!("zarr writer task failed: {e}"))?;
        if let Some(error) = input_error {
            return Err(error);
        }
        let rows = written.map_err(|e| exec_datafusion_err!("write zarr store: {e:#}"))?;
        drop(scratch);
        Ok(rows)
    }
}
