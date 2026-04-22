//! Insert execution for Beacon tables.
//!
//! Provides [`InsertSink`], a DataFusion [`DataSink`] implementation that writes
//! incoming record batches to a new Parquet file and atomically updates the table
//! manifest. Concurrency is controlled via a mutation lock and optimistic
//! schema-version checking.

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

/// A [`DataSink`] that appends data to a Beacon table.
///
/// `InsertSink` wraps an inner Parquet sink and adds manifest management on top.
/// When [`write_all`](DataSink::write_all) is called it:
///
/// 1. Acquires the table-level mutation lock to serialize concurrent writes.
/// 2. Validates that the manifest `schema_version` has not changed since the
///    sink was created (optimistic concurrency check).
/// 3. Delegates the actual Parquet write to the inner `parquet_sink`.
/// 4. On success, appends the new [`DataFile`] to the manifest and flushes it
///    to the object store.
#[derive(Debug)]
pub struct InsertSink {
    /// Object store used for manifest persistence.
    store: Arc<dyn ObjectStore>,
    /// Path to the `manifest.json` file in the object store.
    manifest_path: object_store::path::Path,
    /// Table-level mutex that serializes all mutating operations.
    mutation_handle: Arc<Mutex<()>>,
    /// Shared handle to the in-memory manifest, protected by a mutex.
    manifest_handle: Arc<Mutex<manifest::TableManifest>>,
    /// Snapshot of the manifest captured at sink creation time, used for
    /// optimistic concurrency detection.
    initial_manifest: manifest::TableManifest,
    /// Metadata for the new Parquet file being written by this insert.
    data_file: DataFile,
    /// The underlying Parquet sink that performs the actual file write.
    parquet_sink: Arc<dyn DataSink>,
}

impl InsertSink {
    /// Creates a new `InsertSink`.
    ///
    /// Captures a snapshot of the current manifest state for later concurrency
    /// validation. This uses [`tokio::task::block_in_place`] to synchronously
    /// lock the manifest, so it must be called from a multi-threaded Tokio runtime.
    ///
    /// # Parameters
    ///
    /// * `store` - Object store for manifest persistence.
    /// * `manifest_path` - Path to the `manifest.json` file.
    /// * `mutation_handle` - Shared mutex for serializing mutations.
    /// * `manifest_handle` - Shared handle to the in-memory manifest.
    /// * `data_file` - Metadata for the new Parquet file to be written.
    /// * `parquet_sink` - The underlying Parquet sink for the actual write.
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

    /// Returns the manifest state captured when this sink was created.
    ///
    /// Useful for determining which files were added during the insert operation
    /// by comparing against the current manifest.
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
