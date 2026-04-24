//! Vacuum execution for Beacon tables.
//!
//! Provides [`VacuumExec`], a DataFusion [`ExecutionPlan`] that compacts all
//! existing data files in a Beacon table into a single Parquet file. After
//! writing the merged output, the manifest is updated to reference only the new
//! file, and the old data files (and their deletion vectors) are deleted from
//! the object store on a best-effort basis.

use std::any::Any;
use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use object_store::ObjectStore;
use tokio::sync::Mutex;

use crate::manifest::{self, DataFile};

/// Returns the output schema for a vacuum operation: a single `count` column
/// of type `UInt64` representing the number of rows written to the merged file.
fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// A physical execution plan that compacts (vacuums) a Beacon table.
///
/// `VacuumExec` reads all rows from the table via a child plan, writes them
/// into a single new Parquet file, atomically updates the manifest to reference
/// only the new file, and then deletes the old data files.
///
/// # Concurrency Safety
///
/// The vacuum operation holds the table's mutation lock for its entire duration
/// and validates the manifest `schema_version` has not changed since the plan
/// was created, preventing concurrent mutations from causing data corruption.
///
/// # Output
///
/// Produces a single `RecordBatch` with one `UInt64` column named `count`
/// containing the total number of rows in the compacted output.
#[derive(Debug)]
pub struct VacuumExec {
    /// Object store for file I/O and manifest persistence.
    store: Arc<dyn ObjectStore>,
    /// Path to the `manifest.json` file.
    manifest_path: object_store::path::Path,
    /// Table-level mutex that serializes all mutating operations.
    mutation_handle: Arc<Mutex<()>>,
    /// Shared handle to the in-memory manifest.
    manifest_handle: Arc<Mutex<manifest::TableManifest>>,
    /// Snapshot of the manifest at plan creation time for concurrency checks.
    initial_manifest: manifest::TableManifest,
    /// The old data files to delete after the merge completes.
    data_files_to_delete: Vec<DataFile>,
    /// Metadata for the new merged Parquet file.
    output_file: DataFile,
    /// The Parquet sink that writes the merged output.
    parquet_sink: Arc<dyn DataSink>,
    /// Child execution plan that scans all existing table data.
    child: Arc<dyn ExecutionPlan>,
    /// Cached plan properties (single partition, bounded, final emission).
    props: PlanProperties,
}

impl VacuumExec {
    /// Creates a new `VacuumExec` plan.
    ///
    /// Captures a snapshot of the current manifest for later concurrency
    /// validation. Must be called from a multi-threaded Tokio runtime due to
    /// internal use of [`tokio::task::block_in_place`].
    ///
    /// # Parameters
    ///
    /// * `store` - Object store for file I/O.
    /// * `manifest_path` - Path to the manifest JSON file.
    /// * `mutation_handle` - Shared mutex for serializing mutations.
    /// * `manifest_handle` - Shared handle to the in-memory manifest.
    /// * `data_files_to_delete` - Files to remove after the merge.
    /// * `output_file` - Metadata for the new merged output file.
    /// * `parquet_sink` - Sink that writes the merged Parquet file.
    /// * `child` - Execution plan that scans all existing table data.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        manifest_path: object_store::path::Path,
        mutation_handle: Arc<Mutex<()>>,
        manifest_handle: Arc<Mutex<manifest::TableManifest>>,
        data_files_to_delete: Vec<DataFile>,
        output_file: DataFile,
        parquet_sink: Arc<dyn DataSink>,
        child: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let initial_manifest =
            tokio::task::block_in_place(|| manifest_handle.blocking_lock().clone());

        let schema = count_schema();
        let eq = EquivalenceProperties::new(schema);
        let props = PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            store,
            manifest_path,
            mutation_handle,
            manifest_handle,
            initial_manifest,
            data_files_to_delete,
            output_file,
            parquet_sink,
            child,
            props,
        }
    }
}

impl DisplayAs for VacuumExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "VacuumExec"),
            _ => write!(f, "VacuumExec"),
        }
    }
}

impl ExecutionPlan for VacuumExec {
    fn name(&self) -> &str {
        "VacuumExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        count_schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "VacuumExec expects exactly 1 child".into(),
            ));
        }
        Ok(Arc::new(Self {
            store: self.store.clone(),
            manifest_path: self.manifest_path.clone(),
            mutation_handle: self.mutation_handle.clone(),
            manifest_handle: self.manifest_handle.clone(),
            initial_manifest: self.initial_manifest.clone(),
            data_files_to_delete: self.data_files_to_delete.clone(),
            output_file: self.output_file.clone(),
            parquet_sink: self.parquet_sink.clone(),
            child: children.into_iter().next().unwrap(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(
                "VacuumExec has exactly 1 output partition".into(),
            ));
        }

        let store = self.store.clone();
        let manifest_path = self.manifest_path.clone();
        let mutation_handle = self.mutation_handle.clone();
        let manifest_handle = self.manifest_handle.clone();
        let initial_manifest = self.initial_manifest.clone();
        let data_files_to_delete = self.data_files_to_delete.clone();
        let output_file = self.output_file.clone();
        let parquet_sink = self.parquet_sink.clone();
        let child = self.child.clone();
        let schema = count_schema();

        let stream = futures::stream::once(async move {
            // Acquire mutation lock for entire vacuum operation
            let _mutation_guard = mutation_handle.lock().await;

            // Check for concurrent mutations since plan was created
            {
                let current_manifest = manifest_handle.lock().await;
                if current_manifest.schema_version != initial_manifest.schema_version {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Concurrent mutation detected. Please retry the operation.".into(),
                    ));
                }
            }

            // Execute child plan to get all existing data as a single stream
            let child_stream = child.execute(0, context.clone())?;

            // Write all data to new single parquet file
            let num_rows = parquet_sink.write_all(child_stream, &context).await?;

            // Update manifest: replace all old files with single new file
            {
                let mut manifest_guard = manifest_handle.lock().await;
                manifest_guard.data_files = vec![output_file];

                manifest::flush_table_manifest(store.clone(), &manifest_path, &manifest_guard)
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to flush manifest: {}",
                            e
                        ))
                    })?;
            }

            // Delete old parquet files (best-effort, log warnings on failure)
            for old_file in &data_files_to_delete {
                let path = object_store::path::Path::from(old_file.parquet_file.as_str());
                if let Err(e) = store.delete(&path).await {
                    tracing::warn!(
                        "Failed to delete old data file {}: {}",
                        old_file.parquet_file,
                        e
                    );
                }
                for dv_file in &old_file.deletion_vector_files {
                    let dv_path = object_store::path::Path::from(dv_file.as_str());
                    if let Err(e) = store.delete(&dv_path).await {
                        tracing::warn!("Failed to delete deletion vector file {}: {}", dv_file, e);
                    }
                }
            }

            // Return count of rows vacuumed
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![num_rows]))])?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema(),
            stream,
        )))
    }
}
