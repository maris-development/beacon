//! High-level Beacon runtime shared by the API transports.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::AsArray,
    datatypes::{SchemaRef, UInt64Type},
};
use beacon_data_lake::{
    PersistentSchemaProvider, DATASETS_OBJECT_STORE_URL, TABLES_OBJECT_STORE_URL,
};
use beacon_datafusion_ext::{
    format_ext::{DatasetMetadata, FileFormatFactoryExt},
    listing_table_factory_ext::ListingTableFactoryExt,
    stats_cache::beacon_file_statistics_cache,
};
use beacon_functions::function_doc::FunctionDoc;
use crate::metrics::{ConsolidatedMetrics, MetricsTracker};
use datafusion::{
    catalog::TableFunctionImpl,
    execution::{
        disk_manager::DiskManagerBuilder, memory_pool::FairSpillPool,
        runtime_env::RuntimeEnvBuilder, SessionStateBuilder,
    },
    prelude::{SessionConfig, SessionContext},
};
use futures::TryStreamExt;
use parking_lot::Mutex;

use crate::{
    api::{DatasetInfo, FunctionInfo, QueryMetricsView, QueryRequest, SchemaView, TableConfigView},
    parser::{beacon_parser::BeaconParser, statement::BeaconStatement},
    query_result::{ArrowOutputStream, QueryOutput, QueryOutputFile, QueryResult},
    sys::{self, SystemInfo},
};

/// Beacon's single execution layer: startup, catalog access, queries, SQL, and files.
pub struct Runtime {
    session_ctx: Arc<SessionContext>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
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
        beacon_data_lake::register_object_stores(&session_ctx).await?;

        let file_formats = beacon_formats::file_formats(
            session_ctx.clone(),
            beacon_object_storage::get_datasets_object_store().await,
        )?;

        let mut table_functions = vec![];
        table_functions.extend(beacon_functions::file_formats::register_table_functions(
            tokio::runtime::Handle::current(),
            session_ctx.clone(),
            DATASETS_OBJECT_STORE_URL.clone(),
            beacon_object_storage::get_datasets_object_store().await,
            file_formats.clone(),
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

        let schema_provider = Arc::new(PersistentSchemaProvider::new(
            tokio::runtime::Handle::current(),
            session_ctx.clone(),
            TABLES_OBJECT_STORE_URL.clone(),
        ));
        session_ctx
            .catalog("beacon")
            .unwrap()
            .register_schema("public", schema_provider.clone())?;

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

        beacon_data_lake::init_tables(&session_ctx, &schema_provider).await?;

        let listing_table_factory = Arc::new(ListingTableFactoryExt::new(
            DATASETS_OBJECT_STORE_URL.clone(),
            Arc::downgrade(&session_ctx),
        ));

        Ok(Self {
            session_ctx,
            file_formats,
            listing_table_factory,
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

    /// Execute a client query (JSON or SQL) and return a metrics-tracked result.
    ///
    /// The single entry point for query execution: the JSON form is compiled by
    /// beacon's query compiler, the SQL form goes through beacon's SQL parser, and
    /// both are lowered to a `LogicalPlan` and validated against the caller's
    /// privileges (`is_super_user`). The plan then runs through the unified
    /// `create_physical_plan -> execute_stream` pipeline. Without an `output`
    /// format the result is streamed (metrics recorded as the client drains);
    /// with one, the result is written to a temporary file in that format and
    /// returned as a file download.
    #[tracing::instrument(skip(self, query))]
    pub async fn run_query(
        &self,
        query: crate::query::Query,
        is_super_user: bool,
    ) -> anyhow::Result<QueryResult> {
        let query_id = uuid::Uuid::new_v4();
        let query_json = serde_json::to_value(&query)?;
        let crate::query::Query { inner, output } = query;

        let plan = self.lower_query(inner).await?;
        crate::statement_plan::validate_query_plan(&plan, is_super_user)?;

        match output {
            Some(output) => self.run_query_to_file(plan, output, query_id, query_json).await,
            None => self.run_query_to_stream(plan, query_id, query_json).await,
        }
    }

    /// Stream the plan's results, wrapping the stream so output rows/bytes are
    /// recorded under `query_id` as the client drains it.
    async fn run_query_to_stream(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
        query_id: uuid::Uuid,
        query_json: serde_json::Value,
    ) -> anyhow::Result<QueryResult> {
        let stream = crate::statement_plan::execute_statement_plan(&self.session_ctx, plan).await?;
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

    /// Write the plan's results to a temporary file in the requested `output`
    /// format (via a `COPY TO`) and return a file download. The file write is
    /// driven to completion here and its metrics recorded under `query_id`.
    async fn run_query_to_file(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
        output: crate::query::output::Output,
        query_id: uuid::Uuid,
        query_json: serde_json::Value,
    ) -> anyhow::Result<QueryResult> {
        // `Output::parse` wraps the (already validated) plan in a `COPY TO` the
        // temp file; this COPY is beacon-generated, so it is not re-validated.
        let (copy_plan, output_file) = output.parse(self.session_ctx.as_ref(), plan).await?;
        let output_file = QueryOutputFile::from(output_file);

        let metrics = MetricsTracker::new(query_json, query_id);
        let mut stream =
            crate::statement_plan::execute_statement_plan(&self.session_ctx, copy_plan).await?;
        // The COPY emits a single `count` row; drain it to complete the write.
        while let Some(batch) = stream.try_next().await? {
            let counts = batch.column(0).as_primitive::<UInt64Type>();
            if !counts.is_empty() {
                metrics.add_output_rows(counts.value(0));
            }
        }
        metrics.add_output_bytes(output_file.size()?);
        self.query_metrics
            .lock()
            .insert(query_id, metrics.get_consolidated_metrics());

        Ok(QueryResult {
            query_output: QueryOutput::File(output_file),
            query_id,
        })
    }

    /// Lower the body of a client query (JSON or SQL) to a `LogicalPlan` without
    /// executing or validating it (permission checks and output formatting happen
    /// in `run_query` on the lowered plan).
    async fn lower_query(
        &self,
        inner: crate::query::InnerQuery,
    ) -> anyhow::Result<datafusion::logical_expr::LogicalPlan> {
        match inner {
            crate::query::InnerQuery::Sql(sql) => self.lower_sql(&sql).await,
            crate::query::InnerQuery::Json(body) => {
                crate::query::compile_json_query(body, self.session_ctx.as_ref()).await
            }
        }
    }

    /// Lower a SQL statement (SELECT, DDL/DML, or a beacon custom statement) to a
    /// `LogicalPlan`. The result is validated in `run_query` before execution.
    async fn lower_sql(
        &self,
        sql: &str,
    ) -> anyhow::Result<datafusion::logical_expr::LogicalPlan> {
        match Self::parse_beacon_statement(sql)? {
            BeaconStatement::CreateMaterializedView(statement) => {
                Ok(crate::statement_plan::create_materialized_view_plan(statement))
            }
            BeaconStatement::Refresh(statement) => {
                Ok(crate::statement_plan::refresh_plan(statement))
            }
            BeaconStatement::DFStatement(statement) => {
                crate::statement_plan::lower_df_statement(&self.session_ctx, *statement).await
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
        let plan = self.lower_query(query.into_query()?.inner).await?;
        // EXPLAIN is read-only: reject plans the anonymous client could not run.
        crate::statement_plan::validate_query_plan(&plan, false)?;
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
        self.session_ctx
            .catalog("beacon")
            .and_then(|catalog| catalog.schema("public"))
            .map(|schema| schema.table_names())
            .unwrap_or_default()
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
        let provider = self.session_ctx.table_provider(table_name.as_str()).await.ok()?;
        let config = beacon_data_lake::definition_from_provider(&table_name, provider.as_ref()).ok()?;
        match TableConfigView::try_from(config) {
            Ok(config) => Some(config),
            Err(error) => {
                tracing::error!(?error, "failed to map table config into API contract");
                None
            }
        }
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
        Ok(
            beacon_data_lake::list_datasets(&self.session_ctx, &self.file_formats, offset, limit, pattern)
                .await?,
        )
    }

    pub async fn total_datasets(&self) -> anyhow::Result<usize> {
        self.list_runtime_datasets(None, None, None)
            .await
            .map(|datasets| datasets.len())
    }

    pub async fn list_dataset_schema(&self, file: String) -> anyhow::Result<SchemaRef> {
        Ok(beacon_data_lake::list_dataset_schema(&self.session_ctx, &file).await?)
    }

    pub async fn list_dataset_schema_view(&self, file: String) -> anyhow::Result<SchemaView> {
        let schema = self.list_dataset_schema(file).await?;
        Ok(SchemaView::from(schema.as_ref()))
    }

    fn parse_beacon_statement(sql: &str) -> anyhow::Result<BeaconStatement> {
        let mut parser = BeaconParser::new(sql)?;
        parser.parse_statement().map_err(Into::into)
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
        let batches = runtime
            .run_query(crate::query::Query::sql(sql.to_string()), true)
            .await?
            .into_record_stream()?
            .try_collect::<Vec<_>>()
            .await?;
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
        runtime
            .run_query(crate::query::Query::sql(sql.to_string()), true)
            .await
            .expect("sql should run")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("sql should drain");
    }

    fn query(value: serde_json::Value) -> crate::query::Query {
        serde_json::from_value::<QueryRequest>(value)
            .expect("query request should deserialize")
            .into_query()
            .expect("query request should convert")
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

        let batches = runtime
            .run_query(
                query(serde_json::json!({ "from": table, "select": ["a", "b"] })),
                false,
            )
            .await
            .expect("json query should run")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should drain");

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "json query should return the two inserted rows");
        assert_eq!(batches[0].num_columns(), 2);
    }

    /// A query with an `output` format is written to a file and returned as a
    /// file download.
    #[tokio::test(flavor = "multi_thread")]
    async fn query_with_output_format_produces_file() {
        let runtime = Runtime::new().await.expect("runtime should start");
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("out_{suffix}");

        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1), (2)")).await;

        let result = runtime
            .run_query(
                query(serde_json::json!({
                    "from": table,
                    "select": ["a"],
                    "output": { "format": "csv" },
                })),
                false,
            )
            .await
            .expect("query with output should run");

        match result.query_output {
            QueryOutput::File(file) => {
                assert!(
                    file.size().expect("file size") > 0,
                    "output file should contain data"
                );
            }
            QueryOutput::Stream(_) => panic!("expected a file output"),
        }
    }

    /// `validate_query_plan` is the single permission gate: non-super-users may run
    /// read-only SELECTs but not DDL/DML (standard nodes) nor any beacon extension
    /// operation (super-user-only).
    #[tokio::test(flavor = "multi_thread")]
    async fn non_super_user_is_gated_by_validation() {
        let runtime = Runtime::new().await.expect("runtime should start");
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("val_{suffix}");

        // Super-user seeds a table.
        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;

        // Non-super-user: read-only SELECT is allowed.
        runtime
            .run_query(crate::query::Query::sql(format!("SELECT * FROM {table}")), false)
            .await
            .expect("non-super SELECT should be allowed")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("SELECT should drain");

        // Non-super-user: standard DDL is rejected (by verify_plan).
        runtime
            .run_query(
                crate::query::Query::sql(format!("CREATE TABLE {table}_2 (a BIGINT)")),
                false,
            )
            .await
            .err()
            .expect("non-super CREATE TABLE should be rejected");

        // Non-super-user: a beacon extension operation is rejected (super-user only).
        let err = runtime
            .run_query(
                crate::query::Query::sql(format!(
                    "CREATE MATERIALIZED VIEW {table}_mv AS SELECT 1 AS a"
                )),
                false,
            )
            .await
            .err()
            .expect("non-super CREATE MATERIALIZED VIEW should be rejected");
        assert!(
            err.to_string().contains("super-user"),
            "unexpected error: {err}"
        );
    }
}

#[cfg(test)]
mod restart_tests {
    use super::Runtime;
    use futures::TryStreamExt;

    async fn run_sql(runtime: &Runtime, sql: &str) {
        runtime
            .run_query(crate::query::Query::sql(sql.to_string()), true)
            .await
            .expect("sql should run")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("sql should drain");
    }

    async fn count_rows(runtime: &Runtime, sql: &str) -> usize {
        runtime
            .run_query(crate::query::Query::sql(sql.to_string()), true)
            .await
            .expect("sql should run")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("sql should drain")
            .iter()
            .map(|batch| batch.num_rows())
            .sum()
    }

    /// Tables persisted by one runtime are rebuilt by a fresh runtime via
    /// `init_tables`: the base table's data survives, and the dependent view is
    /// rebuilt in dependency order so it resolves the base table registered ahead
    /// of it. This exercises the persist-on-register + startup-recovery round trip
    /// that replaces the old `TableManager` registry.
    #[tokio::test(flavor = "multi_thread")]
    async fn persisted_tables_survive_a_restart() {
        let suffix = uuid::Uuid::new_v4().simple();
        let base = format!("restart_base_{suffix}");
        let view = format!("restart_view_{suffix}");

        // First runtime: create a base table with data and a view over it.
        let runtime = Runtime::new().await.expect("runtime should start");
        run_sql(&runtime, &format!("CREATE TABLE {base} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {base} VALUES (1), (2)")).await;
        run_sql(&runtime, &format!("CREATE VIEW {view} AS SELECT a FROM {base}")).await;
        drop(runtime);

        // A fresh runtime rebuilds the catalog purely from the persisted
        // `tables://<name>/table.json` definitions.
        let restarted = Runtime::new().await.expect("runtime should restart");

        assert_eq!(
            count_rows(&restarted, &format!("SELECT * FROM {base}")).await,
            2,
            "base table data should persist across a restart"
        );
        assert_eq!(
            count_rows(&restarted, &format!("SELECT * FROM {view}")).await,
            2,
            "the view should be rebuilt and resolve its dependency after a restart"
        );

        // Cleanup so the shared on-disk tables store does not leak into other
        // tests (best-effort; `DROP TABLE` deregisters either provider type).
        let _ = restarted
            .run_query(crate::query::Query::sql(format!("DROP TABLE {view}")), true)
            .await;
        let _ = restarted
            .run_query(crate::query::Query::sql(format!("DROP TABLE {base}")), true)
            .await;
    }
}
