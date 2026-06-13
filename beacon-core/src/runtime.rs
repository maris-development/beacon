//! High-level Beacon runtime shared by the API transports.

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_data_lake::{DataLake, FileManager, TableManager};
use beacon_datafusion_ext::{
    format_ext::DatasetMetadata, listing_table_factory_ext::ListingTableFactoryExt,
    stats_cache::beacon_file_statistics_cache,
};
use beacon_functions::function_doc::FunctionDoc;
use beacon_planner::metrics::{ConsolidatedMetrics, MetricsTracker};
use datafusion::{
    catalog::TableFunctionImpl,
    execution::{
        disk_manager::DiskManagerBuilder, memory_pool::FairSpillPool,
        runtime_env::RuntimeEnvBuilder, SendableRecordBatchStream, SessionStateBuilder,
    },
    prelude::{SQLOptions, SessionConfig, SessionContext},
};
use futures::stream::BoxStream;
use parking_lot::Mutex;

use crate::{
    api::{DatasetInfo, FunctionInfo, QueryMetricsView, QueryRequest, SchemaView, TableConfigView},
    parser::{beacon_parser::BeaconParser, statement::BeaconStatement},
    query_result::{ArrowOutputStream, QueryOutput, QueryResult},
    sys::{self, SystemInfo},
};

/// Beacon's single execution layer: startup, catalog access, queries, SQL, and files.
pub struct Runtime {
    session_ctx: Arc<SessionContext>,
    table_manager: Arc<TableManager>,
    file_manager: Arc<FileManager>,
    listing_table_factory: Arc<ListingTableFactoryExt>,
    query_metrics: Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>,
}

impl Runtime {
    /// Boots the Beacon execution environment and initializes runtime-local state.
    pub async fn new() -> anyhow::Result<Self> {
        let memory_pool = Arc::new(FairSpillPool::new(
            beacon_config::CONFIG.runtime.vm_memory_size * 1024 * 1024,
        ));

        let session_ctx = Self::init_ctx(memory_pool)?;
        let data_lake = Arc::new(DataLake::new(session_ctx.clone()).await);

        let table_manager = data_lake.table_manager();
        let file_manager = data_lake.file_manager();

        let mut table_functions = vec![];
        table_functions.extend(beacon_functions::file_formats::register_table_functions(
            tokio::runtime::Handle::current(),
            session_ctx.clone(),
            file_manager.data_object_store_url(),
            beacon_object_storage::get_datasets_object_store().await,
            file_manager.file_formats().to_vec(),
        ));
        table_functions.extend(beacon_functions::metadata::register_metadata_functions(
            session_ctx.clone(),
            tokio::runtime::Handle::current(),
        ));

        for table_function in table_functions.iter() {
            session_ctx.register_udtf(
                table_function.name().as_str(),
                Arc::clone(table_function) as Arc<dyn TableFunctionImpl>,
            );
        }

        session_ctx
            .catalog("beacon")
            .unwrap()
            .register_schema("public", table_manager.clone())?;

        // Build the shared Iceberg catalog before discovering tables: startup
        // table discovery rebuilds Iceberg providers via the catalog, and the
        // CREATE/DROP handlers need it too. The warehouse lives in the datasets
        // store's internal area (`__beacon__/iceberg`), so it follows the same
        // local/S3 backend as the datasets.
        beacon_iceberg::catalog::init_datasets_warehouse().await?;

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

        table_manager.init_tables().await?;

        let listing_table_factory = Arc::new(ListingTableFactoryExt::new(
            file_manager.data_object_store_url(),
            Arc::downgrade(&session_ctx),
        ));

        Ok(Self {
            session_ctx,
            table_manager,
            listing_table_factory,
            file_manager,
            query_metrics: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn init_ctx(memory_pool: Arc<FairSpillPool>) -> anyhow::Result<Arc<SessionContext>> {
        let mut config = SessionConfig::new()
            .with_batch_size(beacon_config::CONFIG.runtime.batch_size)
            .with_coalesce_batches(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema("beacon", "public")
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
            .with_cache_manager(
                datafusion::execution::cache::cache_manager::CacheManagerConfig {
                    table_files_statistics_cache: Some(beacon_file_statistics_cache()),
                    ..Default::default()
                },
            )
            .build_arc()?;

        // Register beacon's custom query planner so statements can be lowered to
        // physical plan nodes and executed through the same pipeline as queries.
        // The planner needs a handle back to the context, which only exists once
        // the state is built, so it is created with an empty cell that is filled
        // immediately afterwards (a `Weak`, to avoid a reference cycle).
        let session_cell = crate::statement_plan::new_session_cell();
        let session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_query_planner(Arc::new(crate::statement_plan::BeaconQueryPlanner::new(
                session_cell.clone(),
            )))
            .build();

        let session_ctx = Arc::new(SessionContext::new_with_state(session_state));
        let _ = session_cell.set(Arc::downgrade(&session_ctx));

        Ok(session_ctx)
    }

    /// Execute a client query (JSON or SQL) and stream the results.
    ///
    /// The single entry point for query execution: the JSON form is compiled by
    /// beacon's query compiler, the SQL form goes through beacon's SQL parser, and
    /// both are lowered to a `LogicalPlan` and run through the unified
    /// `create_physical_plan -> execute_stream` pipeline. `is_super_user` gates the
    /// SQL surface (DDL/DML and beacon's custom statements) for non-super-users.
    #[tracing::instrument(skip(self, query))]
    pub async fn run_query(
        &self,
        query: crate::query::Query,
        is_super_user: bool,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        if query.output.is_some() {
            anyhow::bail!("output formats are not yet supported on the unified query path");
        }
        let plan = self.lower_query(query, is_super_user).await?;
        crate::statement_plan::execute_statement_plan(&self.session_ctx, plan).await
    }

    /// HTTP `/query` adapter: runs a client query through [`Self::run_query`] and
    /// wraps the stream with metrics tracking (so `query_metrics` is populated for
    /// the metrics endpoint). HTTP client queries are read-only.
    pub async fn run_client_query(&self, query: QueryRequest) -> anyhow::Result<QueryResult> {
        let query = query.into_query()?;

        // Client SQL is gated by the `sql.enable` config; JSON queries are always
        // allowed (Flight SQL has its own `flight_sql.enable`).
        if matches!(query.inner, crate::query::InnerQuery::Sql(_)) && !beacon_config::CONFIG.sql.enable
        {
            anyhow::bail!("SQL queries are not enabled");
        }

        let query_id = uuid::Uuid::new_v4();
        let query_json = serde_json::to_value(&query)?;

        let stream = self.run_query(query, false).await?;

        let metrics = MetricsTracker::new(query_json, query_id);
        let output_stream = ArrowOutputStream {
            stream,
            metrics,
            all_consolidated_metrics: self.query_metrics.clone(),
        };

        Ok(QueryResult {
            query_output: QueryOutput::Stream(output_stream),
            query_id,
        })
    }

    /// Lower a client query (JSON or SQL) to a `LogicalPlan` without executing it.
    async fn lower_query(
        &self,
        query: crate::query::Query,
        is_super_user: bool,
    ) -> anyhow::Result<datafusion::logical_expr::LogicalPlan> {
        match query.inner {
            crate::query::InnerQuery::Sql(sql) => self.lower_sql(&sql, is_super_user).await,
            crate::query::InnerQuery::Json(body) => {
                crate::query::compile_json_query(
                    body,
                    self.session_ctx.as_ref(),
                    self.table_manager.as_ref(),
                    self.file_manager.as_ref(),
                )
                .await
            }
        }
    }

    /// Lower a SQL statement (SELECT, DDL/DML, or a beacon custom statement) to a
    /// `LogicalPlan`, applying the anonymous-access gating for non-super-users.
    async fn lower_sql(
        &self,
        sql: &str,
        is_super_user: bool,
    ) -> anyhow::Result<datafusion::logical_expr::LogicalPlan> {
        let statement = Self::parse_beacon_statement(sql)?;
        if !is_super_user {
            Self::ensure_anonymous_statement_allowed(&statement)?;
        }

        match statement {
            BeaconStatement::CreateMaterializedView(statement) => {
                Ok(crate::statement_plan::create_materialized_view_plan(statement))
            }
            BeaconStatement::Refresh(statement) => {
                Ok(crate::statement_plan::refresh_plan(statement))
            }
            BeaconStatement::DFStatement(statement) => {
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
                crate::statement_plan::lower_df_statement(&self.session_ctx, *statement, &sql_options)
                    .await
            }
        }
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
        let plan = self.lower_query(query.into_query()?, false).await?;
        let json = plan.display_pg_json().to_string();
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

    /// ToDo: implement listing of table functions with proper metadata instead of returning an empty list
    fn list_runtime_table_functions(&self) -> Vec<FunctionDoc> {
        vec![]
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
        beacon_config::CONFIG.sql.default_table.clone()
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
            .table(beacon_config::CONFIG.sql.default_table.as_str())
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

    fn parse_beacon_statement(sql: &str) -> anyhow::Result<BeaconStatement> {
        let mut parser = BeaconParser::new(sql)?;
        parser.parse_statement().map_err(Into::into)
    }

    fn ensure_anonymous_statement_allowed(statement: &BeaconStatement) -> anyhow::Result<()> {
        let deny = || {
            anyhow::anyhow!("anonymous SQL access only supports metadata and read-only SELECT queries")
        };
        match statement {
            // DDL/DML carried by a DFStatement is rejected downstream by
            // `sql_options.verify_plan`, except `ALTER TABLE`, which beacon
            // handles before planning and so must be gated here explicitly.
            BeaconStatement::DFStatement(df) => {
                if let datafusion::sql::parser::Statement::Statement(sql) = df.as_ref() {
                    if matches!(
                        sql.as_ref(),
                        datafusion::sql::sqlparser::ast::Statement::AlterTable(_)
                    ) {
                        return Err(deny());
                    }
                }
                Ok(())
            }
            _ => Err(deny()),
        }
    }
}

#[cfg(test)]
mod materialized_view_tests {
    use super::Runtime;
    use futures::TryStreamExt;

    async fn collect_sql(
        runtime: &Runtime,
        sql: &str,
    ) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        let stream = runtime
            .run_query(crate::query::Query::sql(sql.to_string()), true)
            .await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(batches)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn materialized_view_create_query_refresh_and_drop() {
        let runtime = Runtime::new().await.expect("runtime should start");

        let suffix = uuid::Uuid::new_v4().simple();
        let mv = format!("mv_test_{suffix}");
        let rv = format!("rv_test_{suffix}");

        // CREATE
        collect_sql(
            &runtime,
            &format!("CREATE MATERIALIZED VIEW {mv} AS SELECT 1 AS a, 2 AS b"),
        )
        .await
        .expect("create materialized view should succeed");

        // QUERY reads from the persisted Parquet result.
        let batches = collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
            .await
            .expect("query of materialized view should succeed");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
        assert_eq!(batches[0].num_columns(), 2);

        // REFRESH recomputes and replaces the stored data.
        collect_sql(&runtime, &format!("REFRESH {mv}"))
            .await
            .expect("refresh should succeed");
        let rows: usize = collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
            .await
            .expect("query after refresh should succeed")
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 1);

        // REFRESH of an unknown view fails clearly.
        let err = collect_sql(&runtime, &format!("REFRESH {mv}_missing"))
            .await
            .expect_err("refresh of unknown view should fail");
        assert!(
            err.to_string().contains("does not exist"),
            "unexpected error: {err}"
        );

        // REFRESH of a regular view fails clearly.
        collect_sql(&runtime, &format!("CREATE VIEW {rv} AS SELECT 1 AS a"))
            .await
            .expect("create regular view should succeed");
        let err = collect_sql(&runtime, &format!("REFRESH {rv}"))
            .await
            .expect_err("refresh of a regular view should fail");
        assert!(
            err.to_string().contains("is not a materialized view"),
            "unexpected error: {err}"
        );

        // DROP removes the materialized view; it is no longer queryable.
        collect_sql(&runtime, &format!("DROP TABLE {mv}"))
            .await
            .expect("drop should succeed");
        assert!(
            collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
                .await
                .is_err(),
            "materialized view should not be queryable after drop"
        );

        // Cleanup the regular view.
        let _ = collect_sql(&runtime, &format!("DROP VIEW {rv}")).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn materialized_view_handles_zero_row_result() {
        let runtime = Runtime::new().await.expect("runtime should start");
        let mv = format!("mv_empty_{}", uuid::Uuid::new_v4().simple());

        // A query with no rows must still create a queryable, Parquet-backed view.
        collect_sql(
            &runtime,
            &format!("CREATE MATERIALIZED VIEW {mv} AS SELECT 1 AS a WHERE false"),
        )
        .await
        .expect("create materialized view with empty result should succeed");

        let batches = collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
            .await
            .expect("query of empty materialized view should succeed");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0);

        // Refresh of an empty result must also succeed and stay empty.
        collect_sql(&runtime, &format!("REFRESH {mv}"))
            .await
            .expect("refresh of empty materialized view should succeed");
        let rows: usize = collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
            .await
            .expect("query after refresh should succeed")
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 0);

        collect_sql(&runtime, &format!("DROP TABLE {mv}"))
            .await
            .expect("drop should succeed");
    }
}

#[cfg(test)]
mod client_query_tests {
    use super::Runtime;
    use crate::{api::QueryRequest, query_result::QueryOutput};
    use futures::TryStreamExt;

    async fn run_sql(runtime: &Runtime, sql: &str) {
        let stream = runtime
            .run_query(crate::query::Query::sql(sql.to_string()), true)
            .await
            .expect("sql should run");
        stream
            .try_collect::<Vec<_>>()
            .await
            .expect("sql should drain");
    }

    fn request(value: serde_json::Value) -> QueryRequest {
        serde_json::from_value(value).expect("query request should deserialize")
    }

    /// A JSON client query is lowered to a `LogicalPlan` and executed through the
    /// same pipeline as `run_sql`, returning a streamed result.
    #[tokio::test(flavor = "multi_thread")]
    async fn json_query_runs_through_unified_path() {
        let runtime = Runtime::new().await.expect("runtime should start");
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("json_q_{suffix}");

        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT, b BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1, 2), (3, 4)")).await;

        let result = runtime
            .run_client_query(request(serde_json::json!({
                "from": table,
                "select": ["a", "b"],
            })))
            .await
            .expect("json query should run");

        let batches = match result.query_output {
            QueryOutput::Stream(stream) => stream
                .try_collect::<Vec<_>>()
                .await
                .expect("stream should drain"),
            QueryOutput::File(_) => panic!("expected a streamed result"),
        };

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "json query should return the two inserted rows");
        assert_eq!(batches[0].num_columns(), 2);
    }

    /// Output formats are deferred on the unified path and rejected up front.
    #[tokio::test(flavor = "multi_thread")]
    async fn json_query_with_output_format_is_rejected() {
        let runtime = Runtime::new().await.expect("runtime should start");
        let err = runtime
            .run_client_query(request(serde_json::json!({
                "sql": "SELECT 1 AS a",
                "output": { "format": "csv" },
            })))
            .await
            .err()
            .expect("output formats should be rejected on the unified path");
        assert!(
            err.to_string().contains("output formats are not yet supported"),
            "unexpected error: {err}"
        );
    }
}
