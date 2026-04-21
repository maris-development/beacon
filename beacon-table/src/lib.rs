use arrow::datatypes::DataType;
use arrow_schema::SchemaRef;
use beacon_datafusion_ext::table_ext::TableDefinition;
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
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, coalesce_partitions::CoalescePartitionsExec,
        empty::EmptyExec, limit::GlobalLimitExec, union::UnionExec,
    },
    prelude::{Expr, SessionContext},
};
use object_store::{ObjectMeta, ObjectStore};
use std::{any::Any, sync::Arc};
use tokio::sync::Mutex;

use crate::{append_exec::AppendExec, insert_exec::InsertSink, manifest::DataFile};

mod append_exec;
mod deletion_vector_exec;
mod insert_exec;
mod manifest;

fn execution_error(message: impl Into<String>) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(message.into())
}

#[derive(Debug, Clone)]
pub struct BeaconTable {
    store: Arc<dyn ObjectStore>,
    store_url: ObjectStoreUrl,
    table_name: String,
    table_directory: object_store::path::Path,
    manifest_path: object_store::path::Path,
    manifest: Arc<Mutex<manifest::TableManifest>>,
    mutation_handle: Arc<Mutex<()>>, // Mutex to ensure only one mutating operation can happen at a time
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BeaconTableDefinition {
    pub table_directory: String,
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

    pub fn definition(&self) -> BeaconTableDefinition {
        BeaconTableDefinition {
            table_directory: self.table_directory.to_string(),
            table_name: self.table_name.clone(),
        }
    }

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

    fn resolve_table_path(&self, relative_path: &str) -> object_store::path::Path {
        let relative_path = relative_path.trim_start_matches('/');
        let table_directory = self.table_directory.as_ref();

        if table_directory.is_empty() || relative_path.starts_with(table_directory) {
            object_store::path::Path::from(relative_path)
        } else {
            object_store::path::Path::from(format!("{table_directory}/{relative_path}"))
        }
    }

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
        // let mut deletion_vector_scans = Vec::with_capacity(data_file.deletion_vector_files.len());
        // for deletion_vector_file in &data_file.deletion_vector_files {
        //     let deletion_vector_path = self.resolve_table_path(deletion_vector_file);
        //     deletion_vector_scans.push(
        //         self.create_deletion_vector_scan(state, &deletion_vector_path)
        //             .await?,
        //     );
        // }

        // Ok(Arc::new(DeletionVectorExec::try_new(
        //     data_scan,
        //     deletion_vector_scans,
        // )?))
    }

    pub fn is_locked(&self) -> bool {
        self.mutation_handle.try_lock().is_err()
    }

    fn insert_path(&self) -> object_store::path::Path {
        let uuid = uuid::Uuid::new_v4();
        self.table_directory
            .child("data")
            .child(format!("{uuid}.parquet"))
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

        let sink_exec = DataSinkExec::new(input, Arc::new(insert_sink), None);

        Ok(Arc::new(sink_exec))
    }
}
