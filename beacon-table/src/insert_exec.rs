use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    datasource::sink::{DataSink, DataSinkExec},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, metrics::MetricsSet},
};
use object_store::ObjectStore;
use tokio::sync::Mutex;

use crate::manifest::{self, DataFile};

#[derive(Debug)]
pub struct InsertSink {
    store: Arc<dyn ObjectStore>,
    manifest_path: object_store::path::Path,
    mutation_handle: Arc<Mutex<()>>,
    manifest_handle: Arc<Mutex<manifest::TableManifest>>,
    initial_manifest: manifest::TableManifest,
    data_file: DataFile, // The new data file being written to by this insert operation. This should be added to the manifest once the insert is complete.
    parquet_sink: Arc<dyn DataSink>,
}

impl InsertSink {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        manifest_path: object_store::path::Path,
        mutation_handle: Arc<Mutex<()>>,
        manifest_handle: Arc<Mutex<manifest::TableManifest>>,
        data_file: DataFile,
        parquet_sink: Arc<dyn DataSink>,
    ) -> Self {
        let initial_manifest =
            tokio::task::block_in_place(|| manifest_handle.blocking_lock().clone());
        Self {
            store,
            manifest_path,
            mutation_handle,
            manifest_handle,
            initial_manifest,
            data_file,
            parquet_sink,
        }
    }

    /// Get the initial manifest state at the time of sink creation. This is used to determine which files were added during the insert operation.
    pub fn initial_manifest(&self) -> manifest::TableManifest {
        self.initial_manifest.clone()
    }
}

impl DisplayAs for InsertSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "InsertSink"),
            _ => write!(f, "InsertSink"),
        }
    }
}

#[async_trait::async_trait]
impl DataSink for InsertSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return a snapshot of the [MetricsSet] for this
    /// [DataSink].
    ///
    /// See [ExecutionPlan::metrics()] for more details
    fn metrics(&self) -> Option<MetricsSet> {
        self.parquet_sink.metrics()
    }

    /// Returns the sink schema
    fn schema(&self) -> &SchemaRef {
        self.parquet_sink.schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let _mutation_guard = self.mutation_handle.lock().await;
        // Verify the manifest state hasn't changed since the sink was created, which would indicate a concurrent mutation has occurred. If it has changed, return an error to prevent data corruption.
        let current_manifest = self.manifest_handle.lock().await;
        if current_manifest.schema_version != self.initial_manifest.schema_version {
            return Err(datafusion::error::DataFusionError::Execution(
                "Concurrent mutation detected. Please retry the operation.".into(),
            ));
        }
        drop(current_manifest); // Release the manifest lock before writing to allow other operations to read the manifest while the insert is in progress

        let num_rows = self.parquet_sink.write_all(data, context).await;

        match num_rows {
            Ok(num) => {
                let mut manifest_guard = self.manifest_handle.lock().await;

                // Update the manifest with the new file added by this insert operation. The new file(s) should be determined by comparing the current manifest state with the initial manifest state captured when the sink was created.
                manifest_guard.data_files.push(self.data_file.clone());

                // Persist the updated manifest to the object store
                manifest::flush_table_manifest(
                    self.store.clone(),
                    &self.manifest_path,
                    &manifest_guard,
                )
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to flush manifest: {}",
                        e
                    ))
                })?;

                Ok(num)
            }
            Err(e) => Err(e),
        }
    }
}
