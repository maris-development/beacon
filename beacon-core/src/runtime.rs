//! High-level Beacon runtime shared by the API transports.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::AsArray,
    datatypes::{SchemaRef, UInt64Type},
};
use beacon_data_lake::{DataLake, FileManager, TableManager};
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use beacon_functions::{file_formats::BeaconTableFunctionImpl, function_doc::FunctionDoc};
use beacon_planner::{metrics::ConsolidatedMetrics, plan::BeaconQueryPlan};
use datafusion::{
    catalog::TableFunctionImpl,
    execution::{
        disk_manager::DiskManagerBuilder, memory_pool::FairSpillPool,
        runtime_env::RuntimeEnvBuilder, SendableRecordBatchStream, SessionStateBuilder,
    },
    prelude::{SQLOptions, SessionConfig, SessionContext},
};
use futures::{stream::BoxStream, StreamExt};
use parking_lot::Mutex;

use crate::{
    api::{DatasetInfo, FunctionInfo, QueryMetricsView, QueryRequest, SchemaView, TableConfigView},
    parser::{beacon_parser::BeaconParser, statement::BeaconStatement},
    query_result::{ArrowOutputStream, QueryOutput, QueryOutputFile, QueryResult},
    statement_handlers::SqlStatementExecutor,
    sys::{self, SystemInfo},
};

/// Beacon's single execution layer: startup, catalog access, queries, SQL, and files.
pub struct Runtime {
    table_functions: Vec<Arc<dyn BeaconTableFunctionImpl>>,
    session_ctx: Arc<SessionContext>,
    table_manager: Arc<TableManager>,
    file_manager: Arc<FileManager>,
    query_metrics: Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>,
}

impl Runtime {
    /// Boots the Beacon execution environment and initializes runtime-local state.
    pub async fn new() -> anyhow::Result<Self> {
        let memory_pool = Arc::new(FairSpillPool::new(
            beacon_config::CONFIG.vm_memory_size * 1024 * 1024,
        ));

        let session_ctx = Self::init_ctx(memory_pool)?;
        let data_lake = Arc::new(DataLake::new(session_ctx.clone()).await);
        let table_manager = data_lake.table_manager();
        let file_manager = data_lake.file_manager();

        session_ctx
            .catalog("datafusion")
            .unwrap()
            .register_schema("public", table_manager.clone())?;

        table_manager.init_tables().await?;

        geodatafusion::register(&session_ctx);

        for udf in beacon_functions::geo::geo_udfs() {
            session_ctx.register_udf(udf);
        }

        for udf in beacon_functions::util::util_udfs() {
            session_ctx.register_udf(udf);
        }

        for udf in beacon_functions::blue_cloud::blue_cloud_udfs() {
            session_ctx.register_udf(udf);
        }

        let table_functions = beacon_functions::file_formats::register_table_functions(
            tokio::runtime::Handle::current(),
            session_ctx.clone(),
            file_manager.data_object_store_url(),
            beacon_object_storage::get_datasets_object_store().await,
        );

        for table_function in table_functions.iter() {
            session_ctx.register_udtf(
                table_function.name().as_str(),
                Arc::clone(table_function) as Arc<dyn TableFunctionImpl>,
            );
        }

        let refresh_table_manager = table_manager.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                beacon_config::CONFIG.table_sync_interval_secs,
            ));

            interval.tick().await;
            loop {
                interval.tick().await;
                tracing::info!("Refreshing tables...");
                if let Err(error) = refresh_table_manager.init_tables().await {
                    tracing::error!("Failed to refresh tables: {}", error);
                }
            }
        });

        Ok(Self {
            table_functions,
            session_ctx,
            table_manager,
            file_manager,
            query_metrics: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn init_ctx(memory_pool: Arc<FairSpillPool>) -> anyhow::Result<Arc<SessionContext>> {
        let mut config = SessionConfig::new()
            .with_batch_size(beacon_config::CONFIG.beacon_batch_size)
            .with_coalesce_batches(true)
            .with_information_schema(true)
            .with_collect_statistics(true);

        config.options_mut().sql_parser.enable_ident_normalization = false;
        config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = false;
        config
            .options_mut()
            .execution
            .parquet
            .allow_single_file_parallelism = true;

        let runtime_env = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(DiskManagerBuilder::default())
            .with_memory_pool(memory_pool)
            .build_arc()?;

        let session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .build();

        Ok(Arc::new(SessionContext::new_with_state(session_state)))
    }

    pub async fn run_client_query(&self, query: QueryRequest) -> anyhow::Result<QueryResult> {
        let plan = beacon_planner::prelude::plan_query(
            self.session_ctx.clone(),
            self.table_manager.as_ref(),
            self.file_manager.as_ref(),
            query.into_query()?,
        )
        .await?;

        self.run_plan(plan).await
    }

    pub fn system_info(&self) -> SystemInfo {
        sys::SystemInfo::new()
    }

    pub fn get_query_metrics(&self, query_id: uuid::Uuid) -> Option<QueryMetricsView> {
        self.query_metrics
            .lock()
            .get(&query_id)
            .cloned()
            .and_then(|metrics| match QueryMetricsView::try_from(metrics) {
                Ok(metrics) => Some(metrics),
                Err(error) => {
                    tracing::error!(%query_id, ?error, "failed to map query metrics into API contract");
                    None
                }
            })
    }

    pub async fn explain_client_query(&self, query: QueryRequest) -> anyhow::Result<String> {
        let plan = beacon_query::parser::Parser::parse(
            self.session_ctx.as_ref(),
            self.table_manager.as_ref(),
            self.file_manager.as_ref(),
            query.into_query()?,
        )
        .await?;
        let json = plan.datafusion_plan.display_pg_json().to_string();
        Ok(json)
    }

    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        self.list_runtime_functions()
            .into_iter()
            .filter_map(|function| match FunctionInfo::try_from(function) {
                Ok(function) => Some(function),
                Err(error) => {
                    tracing::error!(?error, "failed to map function metadata into API contract");
                    None
                }
            })
            .collect()
    }

    pub fn list_table_functions(&self) -> Vec<FunctionInfo> {
        self.list_runtime_table_functions()
            .into_iter()
            .filter_map(|function| match FunctionInfo::try_from(function) {
                Ok(function) => Some(function),
                Err(error) => {
                    tracing::error!(
                        ?error,
                        "failed to map table function metadata into API contract"
                    );
                    None
                }
            })
            .collect()
    }

    fn list_runtime_functions(&self) -> Vec<FunctionDoc> {
        let mut functions: Vec<FunctionDoc> = self
            .session_ctx
            .state()
            .scalar_functions()
            .values()
            .flat_map(|function| FunctionDoc::from_scalar(function))
            .collect();

        functions.sort_by(|left, right| left.function_name.cmp(&right.function_name));
        functions.dedup_by(|left, right| left.function_name == right.function_name);
        functions
    }

    fn list_runtime_table_functions(&self) -> Vec<FunctionDoc> {
        let mut table_functions: Vec<FunctionDoc> = self
            .table_functions
            .iter()
            .map(|table_function| FunctionDoc::from_beacon_table_function(table_function.as_ref()))
            .collect();

        table_functions.sort_by(|left, right| left.function_name.cmp(&right.function_name));
        table_functions.dedup_by(|left, right| left.function_name == right.function_name);
        table_functions
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.table_manager.table_names()
    }

    /// Lists SQL catalogs visible to Flight SQL and other SQL-based clients.
    pub fn list_sql_catalogs(&self) -> Vec<String> {
        let mut catalog_names = self.session_ctx.catalog_names();
        catalog_names.sort();
        catalog_names.dedup();
        catalog_names
    }

    /// Lists SQL schemas visible to Flight SQL and other SQL-based clients.
    pub fn list_sql_schemas(&self) -> Vec<(String, String)> {
        let mut schemas = Vec::new();

        for catalog_name in self.list_sql_catalogs() {
            let Some(catalog) = self.session_ctx.catalog(&catalog_name) else {
                continue;
            };

            let mut schema_names = catalog.schema_names();
            schema_names.sort();
            schema_names.dedup();

            schemas.extend(
                schema_names
                    .into_iter()
                    .map(|schema_name| (catalog_name.clone(), schema_name)),
            );
        }

        schemas.sort();
        schemas.dedup();
        schemas
    }

    /// Lists SQL tables visible to Flight SQL and other SQL-based clients.
    pub fn list_sql_tables(&self) -> Vec<(String, String, String)> {
        let mut tables = Vec::new();

        for (catalog_name, schema_name) in self.list_sql_schemas() {
            let Some(catalog) = self.session_ctx.catalog(&catalog_name) else {
                continue;
            };
            let Some(schema) = catalog.schema(&schema_name) else {
                continue;
            };

            let mut table_names = schema.table_names();
            table_names.sort();
            table_names.dedup();

            tables.extend(
                table_names
                    .into_iter()
                    .map(|table_name| (catalog_name.clone(), schema_name.clone(), table_name)),
            );
        }

        tables.sort();
        tables.dedup();
        tables
    }

    pub fn default_table(&self) -> String {
        beacon_config::CONFIG.default_table.clone()
    }

    pub async fn list_table_config(&self, table_name: String) -> Option<TableConfigView> {
        self.table_manager.list_table(&table_name).and_then(
            |config| match TableConfigView::try_from(config) {
                Ok(config) => Some(config),
                Err(error) => {
                    tracing::error!(?error, "failed to map table config into API contract");
                    None
                }
            },
        )
    }

    pub async fn list_table_schema(&self, table_name: String) -> Option<SchemaRef> {
        self.session_ctx
            .table(table_name)
            .await
            .map(|table| Arc::new(table.schema().as_arrow().to_owned()))
            .ok()
    }

    pub async fn list_table_schema_view(&self, table_name: String) -> Option<SchemaView> {
        self.list_table_schema(table_name)
            .await
            .map(|schema| SchemaView::from(schema.as_ref()))
    }

    pub async fn list_default_table_schema(&self) -> SchemaRef {
        let table = self
            .session_ctx
            .table(beacon_config::CONFIG.default_table.as_str())
            .await
            .expect("Default table not found");
        Arc::new(table.schema().as_arrow().to_owned())
    }

    pub async fn list_default_table_schema_view(&self) -> SchemaView {
        let schema = self.list_default_table_schema().await;
        SchemaView::from(schema.as_ref())
    }

    pub async fn list_datasets(
        &self,
        pattern: Option<String>,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<DatasetInfo>> {
        Ok(self
            .list_runtime_datasets(pattern, offset, limit)
            .await?
            .into_iter()
            .map(DatasetInfo::from)
            .collect())
    }

    async fn list_runtime_datasets(
        &self,
        pattern: Option<String>,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<DatasetMetadata>> {
        Ok(self
            .file_manager
            .list_datasets(offset, limit, pattern)
            .await?)
    }

    pub async fn total_datasets(&self) -> anyhow::Result<usize> {
        self.list_runtime_datasets(None, None, None)
            .await
            .map(|datasets| datasets.len())
    }

    pub async fn list_dataset_schema(&self, file: String) -> anyhow::Result<SchemaRef> {
        Ok(self.file_manager.list_dataset_schema(&file).await?)
    }

    pub async fn list_dataset_schema_view(&self, file: String) -> anyhow::Result<SchemaView> {
        let schema = self.list_dataset_schema(file).await?;
        Ok(SchemaView::from(schema.as_ref()))
    }

    pub async fn upload_file<S>(
        &self,
        file_path: &str,
        stream: S,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>
            + Unpin,
    {
        self.file_manager.upload_file(file_path, stream).await
    }

    pub async fn download_file(
        &self,
        file_path: &str,
    ) -> Result<
        BoxStream<'static, Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        self.file_manager.download_file(file_path).await
    }

    pub async fn delete_file(
        &self,
        file_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.file_manager.delete_file(file_path).await
    }

    pub async fn run_sql(
        &self,
        sql: String,
        is_super_user: bool,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = Self::parse_beacon_statement(&sql)?;
        if !is_super_user {
            Self::ensure_anonymous_statement_allowed(&statement)?;
        }

        let sql_options = if is_super_user {
            SQLOptions::new()
                .with_allow_ddl(true)
                .with_allow_dml(true)
                .with_allow_statements(true)
        } else {
            SQLOptions::new()
                .with_allow_ddl(false)
                .with_allow_dml(false)
                .with_allow_statements(false)
        };

        let statement_executor =
            SqlStatementExecutor::new(self.session_ctx.clone(), self.file_manager.clone());

        statement_executor.execute(statement, &sql_options).await
    }

    #[tracing::instrument(skip(self, beacon_plan))]
    async fn run_plan(&self, beacon_plan: BeaconQueryPlan) -> anyhow::Result<QueryResult> {
        let task_ctx = self.session_ctx.task_ctx();

        match beacon_plan.output_file {
            Some(output_file) => {
                let output_file = QueryOutputFile::from(output_file);
                let mut stream = datafusion::physical_plan::execute_stream(
                    beacon_plan.physical_plan.clone(),
                    task_ctx,
                )?;

                let mut total_rows: u64 = 0;
                while let Some(maybe_batch) = stream.next().await {
                    let batch = maybe_batch?;
                    let num_rows_array = batch.column(0).as_primitive::<UInt64Type>();
                    if !num_rows_array.is_empty() {
                        let num_rows = num_rows_array.value(0);
                        beacon_plan.metrics_tracker.add_output_rows(num_rows);
                        total_rows += num_rows;
                    }
                }

                tracing::info!("Query Returned {} rows", total_rows);
                tracing::info!("Query result size in bytes: {:?}", output_file.size());

                beacon_plan
                    .metrics_tracker
                    .add_output_bytes(output_file.size()?);

                let consolidated_metrics = beacon_plan.metrics_tracker.get_consolidated_metrics();
                self.query_metrics
                    .lock()
                    .insert(beacon_plan.query_id, consolidated_metrics);

                Ok(QueryResult {
                    query_output: QueryOutput::File(output_file),
                    query_id: beacon_plan.query_id,
                })
            }
            None => {
                let stream = datafusion::physical_plan::execute_stream(
                    beacon_plan.physical_plan.clone(),
                    task_ctx,
                )?;

                let output_stream = ArrowOutputStream {
                    stream,
                    metrics: beacon_plan.metrics_tracker.clone(),
                    all_consolidated_metrics: self.query_metrics.clone(),
                };

                Ok(QueryResult {
                    query_output: QueryOutput::Stream(output_stream),
                    query_id: beacon_plan.query_id,
                })
            }
        }
    }

    fn parse_beacon_statement(sql: &str) -> anyhow::Result<BeaconStatement> {
        let mut parser = BeaconParser::new(sql)?;
        parser.parse_statement().map_err(Into::into)
    }

    fn ensure_anonymous_statement_allowed(statement: &BeaconStatement) -> anyhow::Result<()> {
        match statement {
            BeaconStatement::DFStatement(_) => Ok(()),
            BeaconStatement::Ingest(_)
            | BeaconStatement::DeleteAtlasDatasets(_)
            | BeaconStatement::CreateAtlasTable(_)
            | BeaconStatement::AlterAtlas(_) => Err(anyhow::anyhow!(
                "anonymous Flight SQL access only supports metadata and read-only SELECT queries"
            )),
        }
    }
}
