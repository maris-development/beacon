//! Beacon Table — a manifest-managed, append-only table format backed by Parquet.
//!
//! `beacon-table` provides [`BeaconTable`], a DataFusion [`TableProvider`] that
//! stores data as a collection of Parquet files managed by a JSON manifest. It
//! supports:
//!
//! - **Append-only inserts** via the standard DataFusion `INSERT INTO` path.
//! - **Full-table scans** with optional projection and limit push-down.
//! - **Vacuum / compaction** to merge all data files into a single Parquet file.
//! - **Optimistic concurrency control** using a schema-version check and a
//!   table-level mutation lock.
//!
//! # Storage Layout
//!
//! ```text
//! <table_directory>/
//!     manifest.json          # Table metadata (schema, file list)
//!     data/
//!         <uuid>.parquet     # Data files (one per insert)
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use beacon_table::BeaconTable;
//! use datafusion::execution::object_store::ObjectStoreUrl;
//! use object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let store = Arc::new(InMemory::new());
//! let url = ObjectStoreUrl::parse("memory://")?;
//! let table = BeaconTable::new(
//!     store, url, "my_table".into(),
//!     "my_table".into(), None,
//! ).await?;
//! # Ok(())
//! # }
//! ```

use arrow::datatypes::DataType;
use arrow_schema::SchemaRef;
use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::physical_expr::LexOrdering;
use datafusion::{
    catalog::{Session, TableProvider, memory::DataSourceExec},
    common::not_impl_err,
    config::TableParquetOptions,
    datasource::{
        TableType,
        file_format::{
            FileFormat,
            parquet::{ParquetFormat, ParquetSink},
        },
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::{FileGroup, FileScanConfigBuilder, FileSinkConfig},
        sink::DataSinkExec,
    },
    execution::{SessionState, object_store::ObjectStoreUrl},
    logical_expr::{TableProviderFilterPushDown, dml::InsertOp},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, coalesce_partitions::CoalescePartitionsExec,
        empty::EmptyExec, limit::GlobalLimitExec, sorts::sort::SortExec, union::UnionExec,
    },
    prelude::{Expr, SessionContext},
};
use object_store::{ObjectMeta, ObjectStore};
use std::{any::Any, sync::Arc};
use tokio::sync::Mutex;

use crate::{
    append_exec::AppendExec,
    index_exec::{CreateIndexExec, TableIndex, ZOrderSortExec},
    insert_exec::InsertSink,
    manifest::DataFile,
    vacuum_exec::VacuumExec,
};

pub use crate::index_exec::{
    ClusteredIndex, ClusteredIndexColumn, TableIndex as BeaconTableIndex, ZOrderIndex,
};

mod append_exec;
mod deletion_vector_exec;
mod index_exec;
mod insert_exec;
mod manifest;
mod vacuum_exec;

/// Creates a [`DataFusionError::Execution`](datafusion::error::DataFusionError::Execution)
/// from any string-like type.
fn execution_error(message: impl Into<String>) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(message.into())
}

/// A manifest-managed, append-only table backed by Parquet files.
///
/// `BeaconTable` implements DataFusion's [`TableProvider`] trait, enabling it to
/// be registered with a [`SessionContext`] and queried with SQL. Data is stored
/// as one or more Parquet files in an object store, tracked by a JSON manifest.
///
/// Mutations (inserts, vacuums) are serialized by `mutation_handle` and guarded
/// by optimistic concurrency checks on the manifest's `schema_version`.
#[derive(Debug, Clone)]
pub struct BeaconTable {
    /// The object store where data files and the manifest are persisted.
    store: Arc<dyn ObjectStore>,
    /// The base URL of the object store (e.g. `memory://`, `s3://bucket`).
    store_url: ObjectStoreUrl,
    /// Human-readable name for this table.
    table_name: String,
    /// Root directory path for this table in the object store.
    table_directory: object_store::path::Path,
    /// Path to the `manifest.json` file.
    manifest_path: object_store::path::Path,
    /// Shared, mutex-protected in-memory copy of the table manifest.
    manifest: Arc<Mutex<manifest::TableManifest>>,
    /// Mutex that serializes all mutating operations (insert, vacuum).
    mutation_handle: Arc<Mutex<()>>,
}

/// Serializable definition of a Beacon table, used for catalog persistence.
///
/// Contains the minimal information needed to reconstruct a [`BeaconTable`]
/// via [`TableDefinition::build_provider`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BeaconTableDefinition {
    /// Root directory of the table in the object store.
    pub table_directory: String,
    /// Human-readable table name.
    pub table_name: String,
}

#[async_trait::async_trait]
#[typetag::serde(name = "beacon_table")]
impl TableDefinition for BeaconTableDefinition {
    /// Builds a concrete [`TableProvider`] from this definition.
    ///
    /// Implementations use the provided session context and store URL to resolve
    /// formats, schemas, and physical locations.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let store = context.runtime_env().object_store(data_store_url)?;
        let table = BeaconTable::new(
            store,
            data_store_url.clone(),
            self.table_name.clone(),
            object_store::path::Path::from(self.table_directory.clone()),
            None,
        )
        .await?;
        Ok(Arc::new(table))
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

impl BeaconTable {
    /// Creates a new `BeaconTable`.
    ///
    /// If a manifest already exists at `<table_directory>/manifest.json`, it is
    /// loaded from the object store. Otherwise a fresh manifest is created with
    /// the provided `schema` (or an empty schema if `None`) and flushed to the
    /// store.
    ///
    /// # Parameters
    ///
    /// * `store` - Object store for all I/O.
    /// * `store_url` - Base URL of the object store.
    /// * `table_name` - Human-readable name.
    /// * `table_directory` - Root directory path for this table.
    /// * `schema` - Optional Arrow schema; used only when creating a new table.
    pub async fn new(
        store: Arc<dyn ObjectStore>,
        store_url: ObjectStoreUrl,
        table_name: String,
        table_directory: object_store::path::Path,
        schema: Option<SchemaRef>,
    ) -> anyhow::Result<Self> {
        let manifest_path = table_directory.child("manifest.json");
        let manifest = if store.get(&manifest_path).await.is_ok() {
            manifest::load_table_manifest(store.clone(), &manifest_path).await?
        } else {
            let manifest = manifest::TableManifest {
                schema: schema.unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty())),
                schema_version: 1,
                data_files: vec![],
                index: None,
            };

            // Flush the initial manifest to ensure it exists for future readers and writers.
            manifest::flush_table_manifest(store.clone(), &manifest_path, &manifest).await?;

            manifest
        };

        Ok(Self {
            store,
            store_url,
            table_name,
            table_directory,
            manifest_path,
            manifest: Arc::new(Mutex::new(manifest)),
            mutation_handle: Arc::new(Mutex::new(())),
        })
    }

    /// Returns a serializable [`BeaconTableDefinition`] for catalog persistence.
    pub fn definition(&self) -> BeaconTableDefinition {
        BeaconTableDefinition {
            table_directory: self.table_directory.to_string(),
            table_name: self.table_name.clone(),
        }
    }

    /// Returns the table schema, optionally projected to the given column indices.
    fn projected_schema(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> datafusion::error::Result<SchemaRef> {
        let manifest = tokio::task::block_in_place(|| self.manifest.blocking_lock().clone());
        match projection {
            Some(projection) => Ok(Arc::new(manifest.schema.project(projection)?)),
            None => Ok(manifest.schema.clone()),
        }
    }

    /// Resolves a relative file path against the table directory.
    ///
    /// If `relative_path` already starts with the table directory prefix, it is
    /// used as-is. Otherwise the table directory is prepended.
    fn resolve_table_path(&self, relative_path: &str) -> object_store::path::Path {
        let relative_path = relative_path.trim_start_matches('/');
        let table_directory = self.table_directory.as_ref();

        if table_directory.is_empty() || relative_path.starts_with(table_directory) {
            object_store::path::Path::from(relative_path)
        } else {
            object_store::path::Path::from(format!("{table_directory}/{relative_path}"))
        }
    }

    /// Fetches object metadata (size, last-modified, etc.) for a path.
    async fn object_meta(
        &self,
        path: &object_store::path::Path,
    ) -> datafusion::error::Result<ObjectMeta> {
        self.store.head(path).await.map_err(|error| {
            execution_error(format!(
                "failed to read object metadata for {path}: {error}"
            ))
        })
    }

    /// Creates a [`DataSourceExec`] plan that reads a single Parquet file.
    async fn create_parquet_scan(
        &self,
        _state: &dyn Session,
        object_meta: ObjectMeta,
        file_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let parquet_format = ParquetFormat::default();
        let partitioned_file = PartitionedFile::from(object_meta.clone());
        let scan_config = FileScanConfigBuilder::new(
            self.store_url.clone(),
            file_schema,
            parquet_format.file_source(),
        )
        .with_file(partitioned_file)
        .with_projection(projection)
        .build();

        Ok(DataSourceExec::from_data_source(scan_config))
    }

    /// Creates a scan plan for a data Parquet file with optional projection.
    async fn create_data_scan(
        &self,
        state: &dyn Session,
        parquet_path: &object_store::path::Path,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let object_meta = self.object_meta(parquet_path).await?;
        self.create_parquet_scan(state, object_meta, table_schema, projection.cloned())
            .await
    }

    /// Creates a scan plan for a deletion vector Parquet file.
    ///
    /// Validates that the file contains exactly one boolean column.
    async fn create_deletion_vector_scan(
        &self,
        state: &dyn Session,
        parquet_path: &object_store::path::Path,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let object_meta = self.object_meta(parquet_path).await?;
        let parquet_format = ParquetFormat::default();
        let inferred_schema = parquet_format
            .infer_schema(state, &self.store, &[object_meta.clone()])
            .await?;

        let inferred_fields = inferred_schema.fields();
        if inferred_fields.len() != 1 {
            return Err(execution_error(format!(
                "deletion vector file {parquet_path} must contain exactly one column, found {}",
                inferred_fields.len()
            )));
        }

        if inferred_fields[0].data_type() != &DataType::Boolean {
            return Err(execution_error(format!(
                "deletion vector file {parquet_path} must contain a single boolean column, found {}",
                inferred_fields[0].data_type()
            )));
        }

        self.create_parquet_scan(state, object_meta, inferred_schema, None)
            .await
    }

    /// Builds the full execution plan for a single [`DataFile`](manifest::DataFile).
    ///
    /// If the data file has no deletion vectors, returns a plain Parquet scan.
    /// Deletion vector support is not yet implemented.
    async fn create_file_plan(
        &self,
        state: &dyn Session,
        table_schema: SchemaRef,
        data_file: &manifest::DataFile,
        projection: Option<&Vec<usize>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let data_path = self.resolve_table_path(&data_file.parquet_file);
        let data_scan = self
            .create_data_scan(state, &data_path, table_schema, projection)
            .await?;

        if data_file.deletion_vector_files.is_empty() {
            return Ok(data_scan);
        }

        not_impl_err!("deletion vector support is not yet implemented")
    }

    /// Returns `true` if the table's mutation lock is currently held.
    pub fn is_locked(&self) -> bool {
        self.mutation_handle.try_lock().is_err()
    }

    /// Generates a new unique path for a data file under `<table_dir>/data/<uuid>.parquet`.
    fn insert_path(&self) -> object_store::path::Path {
        let uuid = uuid::Uuid::new_v4();
        self.table_directory
            .child("data")
            .child(format!("{uuid}.parquet"))
    }

    /// Wraps an execution plan with index-appropriate sorting if an index is configured.
    ///
    /// - `None` → returns `plan` unchanged.
    /// - `Clustered` → wraps with `CoalescePartitionsExec` + `SortExec`.
    /// - `ZOrder` → wraps with `CoalescePartitionsExec` + `ZOrderSortExec`.
    fn apply_index_sort(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        index: &Option<TableIndex>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let index = match index {
            Some(idx) => idx,
            None => return Ok(plan),
        };

        // Coalesce to single partition first so sorting is global
        let coalesced: Arc<dyn ExecutionPlan> =
            if plan.properties().output_partitioning().partition_count() > 1 {
                Arc::new(CoalescePartitionsExec::new(plan))
            } else {
                plan
            };

        match index {
            TableIndex::Clustered(clustered) => {
                let schema = coalesced.schema();
                let mut sort_exprs = Vec::with_capacity(clustered.columns.len());
                for col in &clustered.columns {
                    sort_exprs.push(PhysicalSortExpr::new(
                        datafusion::physical_expr::expressions::col(&col.name, &schema)?,
                        arrow::compute::SortOptions {
                            descending: !col.ascending,
                            nulls_first: true,
                        },
                    ));
                }
                let lex_order = LexOrdering::new(sort_exprs).ok_or_else(|| {
                    execution_error("failed to create LexOrdering for clustered index")
                })?;
                Ok(Arc::new(SortExec::new(lex_order, coalesced)))
            }
            TableIndex::ZOrder(z) => {
                Ok(Arc::new(ZOrderSortExec::new(z.columns.clone(), coalesced)))
            }
        }
    }

    /// Creates an execution plan that builds or replaces a table index.
    ///
    /// Reads all existing data, sorts it according to `index`, writes a single
    /// sorted Parquet file, stores the index configuration in the manifest, and
    /// deletes the old data files.
    ///
    /// # Errors
    ///
    /// Returns an error if any referenced column does not exist in the table schema.
    pub async fn create_index(
        &self,
        state: &dyn Session,
        index: TableIndex,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Validate all referenced columns exist in schema
        let schema = self.schema();
        for col_name in index.column_names() {
            if schema.column_with_name(col_name).is_none() {
                return Err(execution_error(format!(
                    "index column '{col_name}' does not exist in table schema"
                )));
            }
        }

        let manifest = self.manifest.lock().await.clone();
        let data_files_to_delete = manifest.data_files.clone();

        // Scan all existing data
        let scan = self.scan(state, None, &[], None).await?;

        // Apply index sorting
        let sorted = self.apply_index_sort(scan, &Some(index.clone()))?;

        // Coalesce sorted output to single partition
        let coalesced: Arc<dyn ExecutionPlan> =
            if sorted.properties().output_partitioning().partition_count() > 1 {
                Arc::new(CoalescePartitionsExec::new(sorted))
            } else {
                sorted
            };

        // Create output path and parquet sink
        let new_path = self.insert_path();
        let original_url = format!("{}{}", self.store_url, new_path);
        let parsed_url = ListingTableUrl::parse(&original_url)?;

        let sink_config = FileSinkConfig {
            original_url: original_url.clone(),
            object_store_url: self.store_url.clone(),
            file_group: FileGroup::default(),
            table_paths: vec![parsed_url],
            output_schema: manifest.schema.clone(),
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "parquet".to_string(),
        };

        let sink = Arc::new(ParquetSink::new(
            sink_config,
            TableParquetOptions::default(),
        ));

        let output_file = DataFile {
            parquet_file: new_path.to_string(),
            deletion_vector_files: vec![],
        };

        let exec = CreateIndexExec::new(
            self.store.clone(),
            self.manifest_path.clone(),
            self.mutation_handle.clone(),
            self.manifest.clone(),
            data_files_to_delete,
            output_file,
            sink,
            coalesced,
            index,
        );

        Ok(Arc::new(exec))
    }

    /// Removes the current index configuration from the manifest.
    ///
    /// Does **not** rewrite the data files — data remains physically sorted but
    /// future inserts and vacuums will no longer enforce the sort order.
    pub async fn remove_index(&self) -> datafusion::error::Result<()> {
        let _mutation_guard = self.mutation_handle.lock().await;
        let mut manifest = self.manifest.lock().await;
        manifest.index = None;
        manifest::flush_table_manifest(self.store.clone(), &self.manifest_path, &manifest)
            .await
            .map_err(|e| execution_error(format!("failed to flush manifest: {e}")))?;
        Ok(())
    }

    /// Creates an execution plan that merges all existing data files into a single
    /// parquet file, updates the manifest, and deletes the old files.
    pub async fn vacuum(
        &self,
        state: &dyn Session,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let manifest = self.manifest.lock().await.clone();
        let schema = manifest.schema.clone();
        let data_files_to_delete = manifest.data_files.clone();

        // Scan all existing data
        let scan = self.scan(state, None, &[], None).await?;

        // Coalesce to single partition for writing
        let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(scan));

        // Apply index sorting if configured
        let sorted = self.apply_index_sort(coalesced, &manifest.index)?;

        // Create output path and parquet sink
        let new_path = self.insert_path();
        let original_url = format!("{}{}", self.store_url, new_path);
        let parsed_url = ListingTableUrl::parse(&original_url)?;

        let sink_config = FileSinkConfig {
            original_url: original_url.clone(),
            object_store_url: self.store_url.clone(),
            file_group: FileGroup::default(),
            table_paths: vec![parsed_url],
            output_schema: schema,
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "parquet".to_string(),
        };

        let sink = Arc::new(ParquetSink::new(
            sink_config,
            TableParquetOptions::default(),
        ));

        let output_file = DataFile {
            parquet_file: new_path.to_string(),
            deletion_vector_files: vec![],
        };

        let vacuum_exec = VacuumExec::new(
            self.store.clone(),
            self.manifest_path.clone(),
            self.mutation_handle.clone(),
            self.manifest.clone(),
            data_files_to_delete,
            output_file,
            sink,
            sorted,
        );

        Ok(Arc::new(vacuum_exec))
    }
}

#[async_trait::async_trait]
impl TableProvider for BeaconTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        tokio::task::block_in_place(|| self.manifest.blocking_lock().schema.clone())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let _ = filters;

        let scan_schema = self.projected_schema(projection)?;
        let manifest = self.manifest.lock().await;
        if manifest.data_files.is_empty() {
            return Ok(Arc::new(EmptyExec::new(scan_schema)));
        }
        let schema = manifest.schema.clone();

        let mut file_plans = Vec::with_capacity(manifest.data_files.len());
        for data_file in &manifest.data_files {
            file_plans.push(
                self.create_file_plan(state, schema.clone(), data_file, projection)
                    .await?,
            );
        }

        let mut plan: Arc<dyn ExecutionPlan> = if file_plans.len() == 1 {
            file_plans.pop().expect("single file plan should exist")
        } else {
            let coalesced_execs: Vec<Arc<dyn ExecutionPlan>> = file_plans
                .into_iter()
                .map(|file_plan| {
                    Arc::new(CoalescePartitionsExec::new(file_plan)) as Arc<dyn ExecutionPlan>
                })
                .collect();

            Arc::new(AppendExec::try_new(coalesced_execs)?)
        };

        if let Some(limit) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(limit)));
        }

        Ok(plan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if !matches!(insert_op, InsertOp::Append) {
            return Err(execution_error(format!(
                "only append insert operations are supported, found {insert_op:?}"
            )));
        }
        tracing::debug!("Inserting into table '{}'", self.table_name);

        let manifest = self.manifest.lock().await.clone();
        let schema = manifest.schema.clone();

        if input.schema() != schema {
            return Err(execution_error(format!(
                "insert input schema does not match table schema. expected {}, found {}",
                schema,
                input.schema()
            )));
        }
        // Apply index sorting if configured (ensures each parquet file is sorted)
        let sorted_input = self.apply_index_sort(input, &manifest.index)?;

        let new_path = self.insert_path();
        let original_url = format!("{}{}", self.store_url, new_path);
        let parsed_url = ListingTableUrl::parse(&original_url)?;

        // Create a parquet sink and write the input data to it. Then update the manifest to include the new file and return an plan with amount of rows inserted.

        let sink_config = FileSinkConfig {
            original_url: original_url.clone(),
            object_store_url: self.store_url.clone(),
            file_group: FileGroup::default(),
            table_paths: vec![parsed_url],
            output_schema: schema,
            table_partition_cols: vec![],
            insert_op,
            keep_partition_by_columns: false,
            file_extension: "parquet".to_string(),
        };

        let sink = Arc::new(ParquetSink::new(
            sink_config,
            TableParquetOptions::default(),
        ));
        let data_file = DataFile {
            parquet_file: new_path.to_string(),
            deletion_vector_files: vec![],
        };
        let insert_sink = InsertSink::new(
            self.store.clone(),
            self.manifest_path.clone(),
            self.mutation_handle.clone(),
            self.manifest.clone(),
            data_file,
            sink,
        );

        let sink_exec = DataSinkExec::new(sorted_input, Arc::new(insert_sink), None);

        Ok(Arc::new(sink_exec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::catalog::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    fn test_batch(ids: &[i64], names: &[&str], values: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    names.iter().map(|s| *s).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    async fn setup() -> (Arc<InMemory>, SessionContext, BeaconTable) {
        let store = Arc::new(InMemory::new());
        let ctx = SessionContext::new();
        let store_url = ObjectStoreUrl::parse("memory://").unwrap();
        ctx.runtime_env()
            .register_object_store(store_url.as_ref(), store.clone());

        let table = BeaconTable::new(
            store.clone(),
            store_url,
            "test_table".to_string(),
            object_store::path::Path::from("test_table"),
            Some(test_schema()),
        )
        .await
        .unwrap();

        (store, ctx, table)
    }

    async fn insert_batch(ctx: &SessionContext, table: &BeaconTable, batch: RecordBatch) -> u64 {
        let state = ctx.state();
        let mem_exec =
            MemorySourceConfig::try_new_exec(&[vec![batch]], test_schema(), None).unwrap();

        let plan = table
            .insert_into(&state, mem_exec, InsertOp::Append)
            .await
            .unwrap();

        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        // DataSinkExec returns a single batch with count column
        let count_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        count_array.value(0)
    }

    async fn collect_scan(ctx: &SessionContext, table: &BeaconTable) -> Vec<RecordBatch> {
        let state = ctx.state();
        let plan = table.scan(&state, None, &[], None).await.unwrap();
        collect(plan, ctx.task_ctx()).await.unwrap()
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_empty_table() {
        let (_store, ctx, table) = setup().await;
        assert_eq!(table.schema(), test_schema());
        assert_eq!(table.table_type(), TableType::Base);

        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_and_scan() {
        let (_store, ctx, table) = setup().await;
        let batch = test_batch(&[1, 2, 3], &["a", "b", "c"], &[1.0, 2.0, 3.0]);

        let rows = insert_batch(&ctx, &table, batch).await;
        assert_eq!(rows, 3);

        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_inserts() {
        let (_store, ctx, table) = setup().await;

        let batch1 = test_batch(&[1, 2], &["a", "b"], &[1.0, 2.0]);
        let batch2 = test_batch(&[3, 4, 5], &["c", "d", "e"], &[3.0, 4.0, 5.0]);

        insert_batch(&ctx, &table, batch1).await;
        insert_batch(&ctx, &table, batch2).await;

        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 5);

        // Manifest should have 2 data files
        let manifest = table.manifest.lock().await;
        assert_eq!(manifest.data_files.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_with_projection() {
        let (_store, ctx, table) = setup().await;
        let batch = test_batch(&[1, 2], &["a", "b"], &[10.0, 20.0]);
        insert_batch(&ctx, &table, batch).await;

        let state = ctx.state();
        let projection = vec![0_usize, 2]; // id + value only
        let plan = table
            .scan(&state, Some(&projection), &[], None)
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();

        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "id");
        assert_eq!(batches[0].schema().field(1).name(), "value");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_with_limit() {
        let (_store, ctx, table) = setup().await;
        let batch = test_batch(
            &[1, 2, 3, 4, 5],
            &["a", "b", "c", "d", "e"],
            &[1.0, 2.0, 3.0, 4.0, 5.0],
        );
        insert_batch(&ctx, &table, batch).await;

        let state = ctx.state();
        let plan = table.scan(&state, None, &[], Some(2)).await.unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();

        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_schema_mismatch() {
        let (_store, ctx, table) = setup().await;
        let wrong_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            wrong_schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();

        let state = ctx.state();
        let mem_exec =
            MemorySourceConfig::try_new_exec(&[vec![batch]], wrong_schema, None).unwrap();

        let result = table.insert_into(&state, mem_exec, InsertOp::Append).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("does not match table schema")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_overwrite_rejected() {
        let (_store, ctx, table) = setup().await;
        let batch = test_batch(&[1], &["a"], &[1.0]);

        let state = ctx.state();
        let mem_exec =
            MemorySourceConfig::try_new_exec(&[vec![batch]], test_schema(), None).unwrap();

        let result = table
            .insert_into(&state, mem_exec, InsertOp::Overwrite)
            .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("only append insert operations are supported")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vacuum_merges_files() {
        let (_store, ctx, table) = setup().await;

        // Insert 3 separate batches → 3 data files
        insert_batch(&ctx, &table, test_batch(&[1, 2], &["a", "b"], &[1.0, 2.0])).await;
        insert_batch(&ctx, &table, test_batch(&[3, 4], &["c", "d"], &[3.0, 4.0])).await;
        insert_batch(&ctx, &table, test_batch(&[5], &["e"], &[5.0])).await;

        {
            let manifest = table.manifest.lock().await;
            assert_eq!(manifest.data_files.len(), 3);
        }

        // Vacuum
        let state = ctx.state();
        let vacuum_plan = table.vacuum(&state).await.unwrap();
        let result_batches = collect(vacuum_plan, ctx.task_ctx()).await.unwrap();

        // VacuumExec returns count of rows
        let count = result_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 5);

        // Manifest should now have exactly 1 data file
        {
            let manifest = table.manifest.lock().await;
            assert_eq!(manifest.data_files.len(), 1);
        }

        // All data still readable
        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vacuum_empty_table() {
        let (_store, ctx, table) = setup().await;

        let state = ctx.state();
        let vacuum_plan = table.vacuum(&state).await.unwrap();
        let result_batches = collect(vacuum_plan, ctx.task_ctx()).await.unwrap();

        let count = result_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0);

        let manifest = table.manifest.lock().await;
        assert_eq!(manifest.data_files.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_after_vacuum() {
        let (_store, ctx, table) = setup().await;

        insert_batch(&ctx, &table, test_batch(&[1, 2], &["a", "b"], &[1.0, 2.0])).await;
        insert_batch(&ctx, &table, test_batch(&[3], &["c"], &[3.0])).await;

        // Vacuum
        let state = ctx.state();
        let vacuum_plan = table.vacuum(&state).await.unwrap();
        collect(vacuum_plan, ctx.task_ctx()).await.unwrap();

        // Insert more after vacuum
        insert_batch(&ctx, &table, test_batch(&[4, 5], &["d", "e"], &[4.0, 5.0])).await;

        let manifest = table.manifest.lock().await;
        assert_eq!(manifest.data_files.len(), 2); // 1 vacuumed + 1 new

        drop(manifest);
        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_is_locked() {
        let (_store, _ctx, table) = setup().await;
        // Not locked initially
        assert!(!table.is_locked());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_manifest_persisted() {
        let (store, ctx, table) = setup().await;
        let batch = test_batch(&[1, 2, 3], &["a", "b", "c"], &[1.0, 2.0, 3.0]);
        insert_batch(&ctx, &table, batch).await;

        // Load manifest from store directly
        let loaded = manifest::load_table_manifest(
            store.clone(),
            &object_store::path::Path::from("test_table/manifest.json"),
        )
        .await
        .unwrap();

        assert_eq!(loaded.data_files.len(), 1);
        assert_eq!(loaded.schema, test_schema());
        assert_eq!(loaded.schema_version, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vacuum_deletes_old_files() {
        let (store, ctx, table) = setup().await;

        insert_batch(&ctx, &table, test_batch(&[1], &["a"], &[1.0])).await;
        insert_batch(&ctx, &table, test_batch(&[2], &["b"], &[2.0])).await;

        // Record old file paths
        let old_paths: Vec<String> = {
            let manifest = table.manifest.lock().await;
            manifest
                .data_files
                .iter()
                .map(|f| f.parquet_file.clone())
                .collect()
        };
        assert_eq!(old_paths.len(), 2);

        // Verify old files exist
        for path in &old_paths {
            let p = object_store::path::Path::from(path.as_str());
            assert!(store.head(&p).await.is_ok());
        }

        // Vacuum
        let state = ctx.state();
        let vacuum_plan = table.vacuum(&state).await.unwrap();
        collect(vacuum_plan, ctx.task_ctx()).await.unwrap();

        // Old files should be deleted
        for path in &old_paths {
            let p = object_store::path::Path::from(path.as_str());
            assert!(store.head(&p).await.is_err());
        }

        // New file should exist
        let manifest = table.manifest.lock().await;
        assert_eq!(manifest.data_files.len(), 1);
        let new_path = object_store::path::Path::from(manifest.data_files[0].parquet_file.as_str());
        assert!(store.head(&new_path).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_table_reload_from_existing_manifest() {
        let (store, ctx, _table) = setup().await;

        // Insert via first table handle
        let batch = test_batch(&[10, 20], &["x", "y"], &[100.0, 200.0]);
        insert_batch(&ctx, &_table, batch).await;

        // Create new table handle pointing to same directory
        let store_url = ObjectStoreUrl::parse("memory://").unwrap();
        let table2 = BeaconTable::new(
            store.clone(),
            store_url,
            "test_table".to_string(),
            object_store::path::Path::from("test_table"),
            None,
        )
        .await
        .unwrap();

        assert_eq!(table2.schema(), test_schema());

        let batches = collect_scan(&ctx, &table2).await;
        assert_eq!(total_rows(&batches), 2);
    }

    // -- Index integration tests --

    use crate::index_exec::{ClusteredIndex, ClusteredIndexColumn, TableIndex, ZOrderIndex};

    async fn setup_with_index(index: TableIndex) -> (Arc<InMemory>, SessionContext, BeaconTable) {
        let (store, ctx, table) = setup().await;
        // Set the index in the manifest directly
        {
            let mut manifest = table.manifest.lock().await;
            manifest.index = Some(index);
            manifest::flush_table_manifest(store.clone(), &table.manifest_path, &manifest)
                .await
                .unwrap();
        }
        (store, ctx, table)
    }

    fn clustered_id_asc() -> TableIndex {
        TableIndex::Clustered(ClusteredIndex {
            columns: vec![ClusteredIndexColumn {
                name: "id".to_string(),
                ascending: true,
            }],
        })
    }

    fn zorder_id_value() -> TableIndex {
        TableIndex::ZOrder(ZOrderIndex {
            columns: vec!["id".to_string(), "value".to_string()],
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_with_clustered_index() {
        let (_store, ctx, table) = setup_with_index(clustered_id_asc()).await;

        // Insert unsorted data: ids [3, 1, 2]
        let batch = test_batch(&[3, 1, 2], &["c", "a", "b"], &[3.0, 1.0, 2.0]);
        insert_batch(&ctx, &table, batch).await;

        // Scan back — should be sorted by id ascending
        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 3);

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3], "rows should be sorted by id");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_with_zorder_index() {
        let (_store, ctx, table) = setup_with_index(zorder_id_value()).await;

        // Insert data — order should change due to z-order sorting
        let batch = test_batch(&[3, 1, 2], &["c", "a", "b"], &[30.0, 10.0, 20.0]);
        insert_batch(&ctx, &table, batch).await;

        // Scan back — all rows present, just possibly reordered
        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vacuum_with_clustered_index() {
        let (_store, ctx, table) = setup_with_index(clustered_id_asc()).await;

        // Insert 3 unsorted batches
        insert_batch(&ctx, &table, test_batch(&[5, 3], &["e", "c"], &[5.0, 3.0])).await;
        insert_batch(&ctx, &table, test_batch(&[1, 4], &["a", "d"], &[1.0, 4.0])).await;
        insert_batch(&ctx, &table, test_batch(&[2], &["b"], &[2.0])).await;

        // Vacuum — merges all files into one sorted file
        let state = ctx.state();
        let vacuum_plan = table.vacuum(&state).await.unwrap();
        collect(vacuum_plan, ctx.task_ctx()).await.unwrap();

        // Single file, all data present and globally sorted
        {
            let manifest = table.manifest.lock().await;
            assert_eq!(manifest.data_files.len(), 1);
        }

        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 5);

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 2, 3, 4, 5],
            "vacuum should globally sort by id"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vacuum_with_zorder_index() {
        let (_store, ctx, table) = setup_with_index(zorder_id_value()).await;

        insert_batch(
            &ctx,
            &table,
            test_batch(&[3, 1], &["c", "a"], &[30.0, 10.0]),
        )
        .await;
        insert_batch(&ctx, &table, test_batch(&[2], &["b"], &[20.0])).await;

        let state = ctx.state();
        let vacuum_plan = table.vacuum(&state).await.unwrap();
        let result_batches = collect(vacuum_plan, ctx.task_ctx()).await.unwrap();

        let count = result_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 3);

        let manifest = table.manifest.lock().await;
        assert_eq!(manifest.data_files.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_index_rewrites_files() {
        let (_store, ctx, table) = setup().await;

        // Insert 2 unsorted batches (no index yet)
        insert_batch(&ctx, &table, test_batch(&[5, 3], &["e", "c"], &[5.0, 3.0])).await;
        insert_batch(
            &ctx,
            &table,
            test_batch(&[1, 4, 2], &["a", "d", "b"], &[1.0, 4.0, 2.0]),
        )
        .await;

        {
            let manifest = table.manifest.lock().await;
            assert_eq!(manifest.data_files.len(), 2);
            assert!(manifest.index.is_none());
        }

        // Create clustered index
        let state = ctx.state();
        let plan = table
            .create_index(&state, clustered_id_asc())
            .await
            .unwrap();
        collect(plan, ctx.task_ctx()).await.unwrap();

        // Manifest: 1 file, index is set
        {
            let manifest = table.manifest.lock().await;
            assert_eq!(manifest.data_files.len(), 1);
            assert!(manifest.index.is_some());
            assert!(matches!(manifest.index, Some(TableIndex::Clustered(_))));
        }

        // Data is globally sorted
        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 5);

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_zorder_index() {
        let (_store, ctx, table) = setup().await;

        insert_batch(
            &ctx,
            &table,
            test_batch(&[1, 2], &["a", "b"], &[10.0, 20.0]),
        )
        .await;

        let state = ctx.state();
        let plan = table.create_index(&state, zorder_id_value()).await.unwrap();
        collect(plan, ctx.task_ctx()).await.unwrap();

        let manifest = table.manifest.lock().await;
        assert_eq!(manifest.data_files.len(), 1);
        assert!(matches!(manifest.index, Some(TableIndex::ZOrder(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_index_validates_columns() {
        let (_store, ctx, table) = setup().await;

        let state = ctx.state();
        let result = table
            .create_index(
                &state,
                TableIndex::Clustered(ClusteredIndex {
                    columns: vec![ClusteredIndexColumn {
                        name: "nonexistent".to_string(),
                        ascending: true,
                    }],
                }),
            )
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_index() {
        let (_store, ctx, table) = setup().await;

        insert_batch(&ctx, &table, test_batch(&[1, 2], &["a", "b"], &[1.0, 2.0])).await;

        // Create index
        let state = ctx.state();
        let plan = table
            .create_index(&state, clustered_id_asc())
            .await
            .unwrap();
        collect(plan, ctx.task_ctx()).await.unwrap();

        {
            let manifest = table.manifest.lock().await;
            assert!(manifest.index.is_some());
        }

        // Remove index
        table.remove_index().await.unwrap();

        {
            let manifest = table.manifest.lock().await;
            assert!(manifest.index.is_none());
        }

        // Data still readable
        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_after_create_index() {
        let (_store, ctx, table) = setup().await;

        insert_batch(&ctx, &table, test_batch(&[3, 1], &["c", "a"], &[3.0, 1.0])).await;

        // Create clustered index
        let state = ctx.state();
        let plan = table
            .create_index(&state, clustered_id_asc())
            .await
            .unwrap();
        collect(plan, ctx.task_ctx()).await.unwrap();

        // Insert more unsorted data — should be written sorted because index is active
        insert_batch(
            &ctx,
            &table,
            test_batch(&[5, 2, 4], &["e", "b", "d"], &[5.0, 2.0, 4.0]),
        )
        .await;

        {
            let manifest = table.manifest.lock().await;
            assert_eq!(manifest.data_files.len(), 2); // 1 from index + 1 new
        }

        // Each file is independently sorted; reading across files gives 2 sorted runs
        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_index_replaces_existing() {
        let (_store, ctx, table) = setup().await;

        insert_batch(&ctx, &table, test_batch(&[1, 2], &["a", "b"], &[1.0, 2.0])).await;

        // Create clustered index first
        let state = ctx.state();
        let plan = table
            .create_index(&state, clustered_id_asc())
            .await
            .unwrap();
        collect(plan, ctx.task_ctx()).await.unwrap();

        {
            let manifest = table.manifest.lock().await;
            assert!(matches!(manifest.index, Some(TableIndex::Clustered(_))));
        }

        // Replace with z-order index
        let plan = table.create_index(&state, zorder_id_value()).await.unwrap();
        collect(plan, ctx.task_ctx()).await.unwrap();

        {
            let manifest = table.manifest.lock().await;
            assert!(matches!(manifest.index, Some(TableIndex::ZOrder(_))));
            assert_eq!(manifest.data_files.len(), 1);
        }

        // Data still readable
        let batches = collect_scan(&ctx, &table).await;
        assert_eq!(total_rows(&batches), 2);
    }
}
