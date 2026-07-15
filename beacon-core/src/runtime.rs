//! High-level Beacon runtime shared by the API transports.

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use crate::metrics::{ConsolidatedMetrics, MetricsTracker};
use arrow::{
    array::AsArray,
    datatypes::{SchemaRef, UInt64Type},
};
use beacon_datafusion_ext::{
    format_ext::{DatasetMetadata, FileFormatFactoryExt},
    listing_table_factory_ext::ListingTableFactoryExt,
};
use beacon_functions::function_doc::FunctionDoc;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use parking_lot::Mutex;

use crate::{
    api::{
        CrawlReportView, CrawlerView, CreateCrawlerRequest, CreateExternalTableRequest,
        DatasetInfo, FunctionInfo, QueryMetricsView, QueryRequest, SchemaView, TableConfigView,
    },
    parser::{beacon_parser::BeaconParser, statement::BeaconStatement},
    query_result::{ArrowOutputStream, QueryOutput, QueryOutputFile, QueryResult},
};

/// Beacon's single execution layer: startup, catalog access, queries, SQL, and files.
pub struct Runtime {
    pub(crate) session_ctx: Arc<SessionContext>,
    pub(crate) table_function_docs: Vec<FunctionDoc>,
    pub(crate) query_metrics: Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>,
    pub(crate) crawler_manager: Option<Arc<beacon_data_lake::crawler::CrawlerManager>>,
    /// Documentation for the registered table-valued functions, captured at
    /// startup (DataFusion's UDTF registry is not enumerable with metadata).
    /// Authentication + authorization context (users, roles, grants). Shared, owned by the runtime.
    pub(crate) auth: Arc<beacon_auth::AuthContext>,

    /// tmp directory for storing temporary files (e.g. for query output)
    pub(crate) tmp_dir: PathBuf,
}

impl Runtime {
    pub fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
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
    #[tracing::instrument(skip(self, query, identity))]
    pub async fn run_query(
        &self,
        query: crate::query::Query,
        identity: beacon_auth::AuthIdentity,
    ) -> anyhow::Result<QueryResult> {
        let query_id = uuid::Uuid::new_v4();
        let query_json = serde_json::to_value(&query)?;
        let crate::query::Query { inner, output } = query;

        let plan = self.lower_query(inner).await?;
        crate::statement_plan::validate_query_plan(&plan, identity.is_super_user)?;
        crate::statement_plan::authorize_logical_plan(
            &plan,
            &self.session_ctx,
            &self.auth,
            &identity,
            true, //ToDo - make this configurable, adjust the implementation.
        )?;

        match output {
            Some(output) => {
                // An output format wraps the plan in a `COPY TO`, which only
                // accepts a row-producing input. Reject side-effecting statements
                // (DDL/DML, `SET`, ...) here with a clear message instead of
                // letting the `COPY TO` builder fail with a cryptic planner error.
                if !crate::statement_plan::plan_produces_result_set(&plan) {
                    anyhow::bail!(
                        "an output format can only be applied to queries that return rows \
                         (e.g. SELECT); this statement produces no result set to export"
                    );
                }
                self.run_query_to_file(plan, output, query_id, query_json)
                    .await
            }
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
        let (copy_plan, output_file) = output
            .parse(self.session_ctx.as_ref(), &self.tmp_dir, plan)
            .await?;
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
    async fn lower_sql(&self, sql: &str) -> anyhow::Result<datafusion::logical_expr::LogicalPlan> {
        match Self::parse_beacon_statement(sql)? {
            BeaconStatement::Auth(statement) => Ok(crate::statement_plan::auth_plan(statement)),
            BeaconStatement::CreateMaterializedView(statement) => Ok(
                crate::statement_plan::create_materialized_view_plan(statement),
            ),
            BeaconStatement::Refresh(statement) => {
                Ok(crate::statement_plan::refresh_plan(statement))
            }
            BeaconStatement::CreateCrawler(statement) => {
                Ok(crate::statement_plan::create_crawler_plan(statement))
            }
            BeaconStatement::RunCrawler(statement) => {
                Ok(crate::statement_plan::run_crawler_plan(statement))
            }
            BeaconStatement::DropCrawler(statement) => {
                Ok(crate::statement_plan::drop_crawler_plan(statement))
            }
            BeaconStatement::ShowCrawlers => Ok(crate::statement_plan::show_crawlers_plan()),
            BeaconStatement::SetExtension(statement) => {
                Ok(crate::statement_plan::set_extension_plan(statement))
            }
            BeaconStatement::DropExtension(statement) => {
                Ok(crate::statement_plan::drop_extension_plan(statement))
            }
            BeaconStatement::ShowExtensions(statement) => {
                Ok(crate::statement_plan::show_extensions_plan(statement))
            }
            BeaconStatement::CreateIndex(statement) => {
                Ok(crate::statement_plan::create_index_plan(statement))
            }
            BeaconStatement::DropIndex(statement) => {
                Ok(crate::statement_plan::drop_index_plan(statement))
            }
            BeaconStatement::ShowIndexes(statement) => {
                Ok(crate::statement_plan::show_indexes_plan(statement))
            }
            BeaconStatement::DFStatement(statement) => {
                crate::statement_plan::lower_df_statement(&self.session_ctx, *statement).await
            }
        }
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

    pub async fn explain_client_query(
        &self,
        query: QueryRequest,
        identity: beacon_auth::AuthIdentity,
    ) -> anyhow::Result<String> {
        let plan = self.lower_query(query.into_query()?.inner).await?;
        // EXPLAIN is read-only: reject plans the anonymous client could not run.
        crate::statement_plan::validate_query_plan(&plan, false)?;
        crate::statement_plan::authorize_logical_plan(
            &plan,
            &self.session_ctx,
            &self.auth,
            &identity,
            true, //ToDo - make this configurable, adjust the implementation.
        )?;
        let json = plan.display_pg_json().to_string();
        Ok(json)
    }

    /// Run the query and return its physical plan as pgjson annotated with
    /// per-node runtime metrics (the `EXPLAIN ANALYZE` analog of
    /// [`Self::explain_client_query`]).
    ///
    /// Unlike `explain_client_query`, this executes the plan to completion to
    /// populate the metrics, so it applies the same permission gate the client
    /// query path uses: anonymous callers are read-only, while a super-user
    /// (valid admin basic auth) may also analyze DDL/DML — which, since this
    /// runs the plan, has the same side effects as `/api/query`.
    pub async fn explain_analyze_client_query(
        &self,
        query: crate::query::Query,
        identity: beacon_auth::AuthIdentity,
    ) -> anyhow::Result<String> {
        // `output` (file format) is meaningless for EXPLAIN ANALYZE — only the
        // query body is planned and executed for its metrics.
        let plan = self.lower_query(query.inner).await?;
        crate::statement_plan::validate_query_plan(&plan, identity.is_super_user)?;
        crate::statement_plan::authorize_logical_plan(
            &plan,
            &self.session_ctx,
            &self.auth,
            &identity,
            true, //ToDo - make this configurable, adjust the implementation.
        )?;

        // Build the physical plan and keep the `Arc` so its metrics can be read
        // after the stream drains. `execute_statement_plan` discards the plan,
        // so the create/execute steps are inlined here.
        let physical_plan = self.session_ctx.state().create_physical_plan(&plan).await?;
        let mut stream = datafusion::physical_plan::execute_stream(
            physical_plan.clone(),
            self.session_ctx.task_ctx(),
        )?;
        // Drain to completion so each node's metrics are fully recorded.
        while stream.try_next().await?.is_some() {}

        Ok(crate::metrics::explain_analyze_pg_json(physical_plan.as_ref()).to_string())
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
        let mut functions = self.table_function_docs.clone();
        functions.sort_by(|left, right| left.function_name.cmp(&right.function_name));
        functions.dedup_by(|left, right| left.function_name == right.function_name);
        functions
    }

    fn parse_beacon_statement(sql: &str) -> anyhow::Result<BeaconStatement> {
        let mut parser = BeaconParser::new(sql)?;
        parser.parse_statement().map_err(Into::into)
    }
}

// /// A unique temp path for a test's `beacon.db`. Each call returns a fresh
// /// path (process id + an atomic counter), so parallel tests never share the
// /// exclusively-locked tables store. A test that deliberately restarts a runtime
// /// should capture one path and reuse it across both boots.
// #[cfg(any(test, feature = "test-util"))]
// pub fn test_db_path() -> std::path::PathBuf {
//     use std::sync::atomic::{AtomicU64, Ordering};
//     static SEQ: AtomicU64 = AtomicU64::new(0);
//     std::env::temp_dir().join(format!(
//         "beacon-test-db-{}-{}.db",
//         std::process::id(),
//         SEQ.fetch_add(1, Ordering::Relaxed)
//     ))
// }

// #[cfg(test)]
// mod materialized_view_tests {
//     use super::Runtime;
//     use futures::TryStreamExt;

//     async fn collect_sql(
//         runtime: &Runtime,
//         sql: &str,
//     ) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
//         let batches = runtime
//             .run_query(
//                 crate::query::Query::sql(sql.to_string()),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await?
//             .into_record_stream()?
//             .try_collect::<Vec<_>>()
//             .await?;
//         Ok(batches)
//     }

//     #[tokio::test(flavor = "multi_thread")]
//     async fn materialized_view_create_query_refresh_and_drop() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");

//         let suffix = uuid::Uuid::new_v4().simple();
//         let mv = format!("mv_test_{suffix}");
//         let rv = format!("rv_test_{suffix}");

//         // CREATE
//         collect_sql(
//             &runtime,
//             &format!("CREATE MATERIALIZED VIEW {mv} AS SELECT 1 AS a, 2 AS b"),
//         )
//         .await
//         .expect("create materialized view should succeed");

//         // QUERY reads from the persisted Parquet result.
//         let batches = collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
//             .await
//             .expect("query of materialized view should succeed");
//         let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//         assert_eq!(rows, 1);
//         assert_eq!(batches[0].num_columns(), 2);

//         // REFRESH recomputes and replaces the stored data.
//         collect_sql(&runtime, &format!("REFRESH {mv}"))
//             .await
//             .expect("refresh should succeed");
//         let rows: usize = collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
//             .await
//             .expect("query after refresh should succeed")
//             .iter()
//             .map(|b| b.num_rows())
//             .sum();
//         assert_eq!(rows, 1);

//         // REFRESH of an unknown view fails clearly.
//         let err = collect_sql(&runtime, &format!("REFRESH {mv}_missing"))
//             .await
//             .expect_err("refresh of unknown view should fail");
//         assert!(
//             err.to_string().contains("does not exist"),
//             "unexpected error: {err}"
//         );

//         // REFRESH of a regular view fails clearly.
//         collect_sql(&runtime, &format!("CREATE VIEW {rv} AS SELECT 1 AS a"))
//             .await
//             .expect("create regular view should succeed");
//         let err = collect_sql(&runtime, &format!("REFRESH {rv}"))
//             .await
//             .expect_err("refresh of a regular view should fail");
//         assert!(
//             err.to_string().contains("is not a materialized view"),
//             "unexpected error: {err}"
//         );

//         // DROP removes the materialized view; it is no longer queryable.
//         collect_sql(&runtime, &format!("DROP TABLE {mv}"))
//             .await
//             .expect("drop should succeed");
//         assert!(
//             collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
//                 .await
//                 .is_err(),
//             "materialized view should not be queryable after drop"
//         );

//         // Cleanup the regular view.
//         let _ = collect_sql(&runtime, &format!("DROP VIEW {rv}")).await;
//     }

//     #[tokio::test(flavor = "multi_thread")]
//     async fn materialized_view_handles_zero_row_result() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");
//         let mv = format!("mv_empty_{}", uuid::Uuid::new_v4().simple());

//         // A query with no rows must still create a queryable, Parquet-backed view.
//         collect_sql(
//             &runtime,
//             &format!("CREATE MATERIALIZED VIEW {mv} AS SELECT 1 AS a WHERE false"),
//         )
//         .await
//         .expect("create materialized view with empty result should succeed");

//         let batches = collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
//             .await
//             .expect("query of empty materialized view should succeed");
//         let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//         assert_eq!(rows, 0);

//         // Refresh of an empty result must also succeed and stay empty.
//         collect_sql(&runtime, &format!("REFRESH {mv}"))
//             .await
//             .expect("refresh of empty materialized view should succeed");
//         let rows: usize = collect_sql(&runtime, &format!("SELECT * FROM {mv}"))
//             .await
//             .expect("query after refresh should succeed")
//             .iter()
//             .map(|b| b.num_rows())
//             .sum();
//         assert_eq!(rows, 0);

//         collect_sql(&runtime, &format!("DROP TABLE {mv}"))
//             .await
//             .expect("drop should succeed");
//     }
// }

// #[cfg(test)]
// mod client_query_tests {
//     use super::Runtime;
//     use crate::{api::QueryRequest, query_result::QueryOutput};
//     use futures::TryStreamExt;

//     async fn run_sql(runtime: &Runtime, sql: &str) {
//         runtime
//             .run_query(
//                 crate::query::Query::sql(sql.to_string()),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await
//             .expect("sql should run")
//             .into_record_stream()
//             .expect("streamed result")
//             .try_collect::<Vec<_>>()
//             .await
//             .expect("sql should drain");
//     }

//     fn query(value: serde_json::Value) -> crate::query::Query {
//         serde_json::from_value::<QueryRequest>(value)
//             .expect("query request should deserialize")
//             .into_query()
//             .expect("query request should convert")
//     }

//     /// A JSON client query is lowered to a `LogicalPlan` and executed through the
//     /// same pipeline as `run_sql`, returning a streamed result.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn json_query_runs_through_unified_path() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");
//         let suffix = uuid::Uuid::new_v4().simple();
//         let table = format!("json_q_{suffix}");

//         run_sql(
//             &runtime,
//             &format!("CREATE TABLE {table} (a BIGINT, b BIGINT)"),
//         )
//         .await;
//         run_sql(
//             &runtime,
//             &format!("INSERT INTO {table} VALUES (1, 2), (3, 4)"),
//         )
//         .await;

//         let batches = runtime
//             .run_query(
//                 query(serde_json::json!({ "from": table, "select": ["a", "b"] })),
//                 beacon_auth::AuthIdentity::empty(),
//             )
//             .await
//             .expect("json query should run")
//             .into_record_stream()
//             .expect("streamed result")
//             .try_collect::<Vec<_>>()
//             .await
//             .expect("stream should drain");

//         let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//         assert_eq!(rows, 2, "json query should return the two inserted rows");
//         assert_eq!(batches[0].num_columns(), 2);
//     }

//     /// `SET EXTENSION` / `SHOW EXTENSIONS` / `DROP EXTENSION` round-trip end to
//     /// end, and an extension referencing a missing column is rejected.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn table_extensions_sql_round_trip() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");
//         let suffix = uuid::Uuid::new_v4().simple();
//         let table = format!("ext_{suffix}");

//         run_sql(
//             &runtime,
//             &format!("CREATE TABLE {table} (lat BIGINT, depth BIGINT)"),
//         )
//         .await;

//         // SET a preset via SQL, then read it back through the typed API.
//         run_sql(
//             &runtime,
//             &format!(
//                 "SET EXTENSION 'preset' FOR {table} TO '{{\"presets\":[{{\"name\":\"shallow\",\"filters\":[{{\"column\":\"depth\",\"op\":\"<=\",\"value\":10}}]}}]}}'"
//             ),
//         )
//         .await;

//         let ext = runtime
//             .get_table_extensions(table.clone())
//             .await
//             .expect("extensions should load");
//         let preset = ext.preset.expect("preset extension should be set");
//         assert_eq!(preset.presets[0].name, "shallow");

//         // SHOW EXTENSIONS returns one JSON row mentioning the preset.
//         let batches = runtime
//             .run_query(
//                 crate::query::Query::sql(format!("SHOW EXTENSIONS FOR {table}")),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await
//             .expect("show extensions should run")
//             .into_record_stream()
//             .expect("streamed result")
//             .try_collect::<Vec<_>>()
//             .await
//             .expect("stream should drain");
//         let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//         assert_eq!(rows, 1, "SHOW EXTENSIONS returns one row");
//         let json = batches[0]
//             .column(0)
//             .as_any()
//             .downcast_ref::<arrow::array::StringArray>()
//             .expect("extensions column is Utf8")
//             .value(0);
//         assert!(
//             json.contains("shallow"),
//             "SHOW output should include the preset: {json}"
//         );

//         // An extension over a non-existent column is rejected by validation.
//         let rejected = try_run_sql(
//             &runtime,
//             &format!(
//                 "SET EXTENSION 'preset' FOR {table} TO '{{\"presets\":[{{\"name\":\"x\",\"filters\":[{{\"column\":\"ghost\",\"op\":\"=\",\"value\":1}}]}}]}}'"
//             ),
//         )
//         .await;
//         assert!(
//             rejected.is_err(),
//             "preset over a missing column should be rejected"
//         );

//         // DROP removes it; the document becomes empty.
//         run_sql(&runtime, &format!("DROP EXTENSION 'preset' FOR {table}")).await;
//         assert!(
//             runtime
//                 .get_table_extensions(table.clone())
//                 .await
//                 .expect("extensions should load")
//                 .is_empty(),
//             "dropping the only extension leaves an empty document"
//         );
//     }

//     /// Like `run_sql`, but returns the result (draining the stream) so callers can
//     /// assert on failures from side-effecting statements.
//     async fn try_run_sql(runtime: &Runtime, sql: &str) -> anyhow::Result<()> {
//         runtime
//             .run_query(
//                 crate::query::Query::sql(sql.to_string()),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await?
//             .into_record_stream()?
//             .try_collect::<Vec<_>>()
//             .await?;
//         Ok(())
//     }

//     /// A query with an `output` format is written to a file and returned as a
//     /// file download.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn query_with_output_format_produces_file() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");
//         let suffix = uuid::Uuid::new_v4().simple();
//         let table = format!("out_{suffix}");

//         run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
//         run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1), (2)")).await;

//         let result = runtime
//             .run_query(
//                 query(serde_json::json!({
//                     "from": table,
//                     "select": ["a"],
//                     "output": { "format": "csv" },
//                 })),
//                 beacon_auth::AuthIdentity::empty(),
//             )
//             .await
//             .expect("query with output should run");

//         match result.query_output {
//             QueryOutput::File(file) => {
//                 assert!(
//                     file.size().expect("file size") > 0,
//                     "output file should contain data"
//                 );
//             }
//             QueryOutput::Stream(_) => panic!("expected a file output"),
//         }
//     }

//     /// NetCDF output goes through a custom `DataSink` that writes a real local
//     /// file (the netcdf-c writer cannot stream to an object store). This asserts
//     /// the file lands under the configured tmp store root — not the OS temp dir —
//     /// and is non-empty: the regression fixed by threading `StorageConfig` into
//     /// the NetCDF factory/sink.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn query_with_netcdf_output_writes_under_configured_tmp() {
//         let config = std::sync::Arc::new(beacon_config::Config::load().unwrap());
//         let tmp_dir = config.storage.tmp_dir.clone();
//         let runtime = Runtime::new_with_in_memory_auth(config)
//             .await
//             .expect("runtime should start");
//         let suffix = uuid::Uuid::new_v4().simple();
//         let table = format!("ncout_{suffix}");

//         run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
//         run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1), (2)")).await;

//         let result = runtime
//             .run_query(
//                 query(serde_json::json!({
//                     "from": table,
//                     "select": ["a"],
//                     "output": { "format": "netcdf" },
//                 })),
//                 beacon_auth::AuthIdentity::empty(),
//             )
//             .await
//             .expect("netcdf query with output should run");

//         match result.query_output {
//             QueryOutput::File(file) => {
//                 // tempfile resolves to an absolute path; canonicalize both
//                 // sides so the comparison is independent of cwd-relative form.
//                 let got = std::fs::canonicalize(file.path().parent().unwrap())
//                     .expect("canonicalize output parent");
//                 let want = std::fs::canonicalize(&tmp_dir).expect("canonicalize tmp dir");
//                 assert_eq!(
//                     got, want,
//                     "netcdf output should be written under the configured tmp dir"
//                 );
//                 assert!(
//                     file.size().expect("file size") > 0,
//                     "netcdf output file should contain data"
//                 );
//             }
//             QueryOutput::Stream(_) => panic!("expected a file output"),
//         }
//     }

//     /// `validate_query_plan` is the single permission gate: non-super-users may run
//     /// read-only SELECTs but not DDL/DML (standard nodes) nor any beacon extension
//     /// operation (super-user-only).
//     #[tokio::test(flavor = "multi_thread")]
//     async fn non_super_user_is_gated_by_validation() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");
//         let suffix = uuid::Uuid::new_v4().simple();
//         let table = format!("val_{suffix}");

//         // Super-user seeds a table.
//         run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;

//         // Non-super-user: read-only SELECT is allowed.
//         runtime
//             .run_query(
//                 crate::query::Query::sql(format!("SELECT * FROM {table}")),
//                 beacon_auth::AuthIdentity::empty(),
//             )
//             .await
//             .expect("non-super SELECT should be allowed")
//             .into_record_stream()
//             .expect("streamed result")
//             .try_collect::<Vec<_>>()
//             .await
//             .expect("SELECT should drain");

//         // Non-super-user: standard DDL is rejected (by verify_plan).
//         runtime
//             .run_query(
//                 crate::query::Query::sql(format!("CREATE TABLE {table}_2 (a BIGINT)")),
//                 beacon_auth::AuthIdentity::empty(),
//             )
//             .await
//             .err()
//             .expect("non-super CREATE TABLE should be rejected");

//         // Non-super-user: a beacon extension operation is rejected (super-user only).
//         let err = runtime
//             .run_query(
//                 crate::query::Query::sql(format!(
//                     "CREATE MATERIALIZED VIEW {table}_mv AS SELECT 1 AS a"
//                 )),
//                 beacon_auth::AuthIdentity::empty(),
//             )
//             .await
//             .err()
//             .expect("non-super CREATE MATERIALIZED VIEW should be rejected");
//         assert!(
//             err.to_string().contains("super-user"),
//             "unexpected error: {err}"
//         );
//     }

//     /// A non-super-user cannot escalate its own privileges: all auth-management statements are
//     /// super-user-only (they lower to extension nodes), so a role-limited caller cannot create a
//     /// role, grant itself privileges, or create users. A super-user can run the same statements.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn non_super_user_cannot_escalate_privileges() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");

//         // A caller holding a non-super role (no global ALL grant).
//         let limited = beacon_auth::AuthIdentity {
//             username: "alice".to_string(),
//             roles: vec!["reader".to_string()],
//             is_super_user: false,
//         };

//         for sql in [
//             "CREATE ROLE hacker",
//             "GRANT ALL TO ROLE reader",
//             "GRANT ALL TO ROLE hacker",
//             "CREATE USER bob WITH PASSWORD 'pw'",
//             "GRANT ROLE reader TO USER alice",
//         ] {
//             let err = runtime
//                 .run_query(crate::query::Query::sql(sql.to_string()), limited.clone())
//                 .await
//                 .err()
//                 .unwrap_or_else(|| panic!("non-super should be rejected: {sql}"));
//             assert!(
//                 err.to_string().contains("super-user"),
//                 "expected a super-user error for `{sql}`, got: {err}"
//             );
//         }

//         // The super-user can create read-only roles and grant SELECT.
//         run_sql(&runtime, "CREATE ROLE analyst").await;
//         run_sql(&runtime, "GRANT SELECT ON TABLE t TO ROLE analyst").await;

//         // But even the super-user cannot grant write/management privileges to a role — roles are
//         // read-only, so there is no way to mint another super-user through SQL.
//         let err = runtime
//             .run_query(
//                 crate::query::Query::sql("GRANT ALL TO ROLE analyst".to_string()),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await
//             .err()
//             .expect("granting a write/ALL privilege to a role should be rejected");
//         assert!(
//             err.to_string().contains("read-only"),
//             "expected a read-only-role error, got: {err}"
//         );
//     }

//     /// End-to-end index lifecycle on a Lance-backed managed table: CREATE INDEX,
//     /// SHOW INDEXES (one row), DROP INDEX (zero rows) — exercising the parser,
//     /// planner, execs, and Lance index ops through the full SQL path.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn create_show_drop_index_round_trip() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");
//         let suffix = uuid::Uuid::new_v4().simple();
//         let table = format!("idx_{suffix}");

//         run_sql(
//             &runtime,
//             &format!("CREATE TABLE {table} (id BIGINT, name VARCHAR)"),
//         )
//         .await;
//         run_sql(
//             &runtime,
//             &format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b')"),
//         )
//         .await;
//         run_sql(
//             &runtime,
//             &format!("CREATE INDEX {table}_id_idx ON {table} (id) USING btree"),
//         )
//         .await;

//         let count_indexes = |sql: String| {
//             let runtime = &runtime;
//             async move {
//                 runtime
//                     .run_query(
//                         crate::query::Query::sql(sql),
//                         beacon_auth::AuthIdentity::system(),
//                     )
//                     .await
//                     .expect("show indexes should run")
//                     .into_record_stream()
//                     .expect("streamed result")
//                     .try_collect::<Vec<_>>()
//                     .await
//                     .expect("stream should drain")
//                     .iter()
//                     .map(|b| b.num_rows())
//                     .sum::<usize>()
//             }
//         };

//         assert_eq!(
//             count_indexes(format!("SHOW INDEXES ON {table}")).await,
//             1,
//             "one index should be listed"
//         );

//         run_sql(&runtime, &format!("DROP INDEX {table}_id_idx ON {table}")).await;
//         assert_eq!(
//             count_indexes(format!("SHOW INDEXES ON {table}")).await,
//             0,
//             "no indexes after drop"
//         );

//         run_sql(&runtime, &format!("DROP TABLE {table}")).await;
//     }
// }

// #[cfg(test)]
// mod restart_tests {
//     use super::Runtime;
//     use futures::TryStreamExt;

//     async fn run_sql(runtime: &Runtime, sql: &str) {
//         runtime
//             .run_query(
//                 crate::query::Query::sql(sql.to_string()),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await
//             .expect("sql should run")
//             .into_record_stream()
//             .expect("streamed result")
//             .try_collect::<Vec<_>>()
//             .await
//             .expect("sql should drain");
//     }

//     async fn count_rows(runtime: &Runtime, sql: &str) -> usize {
//         runtime
//             .run_query(
//                 crate::query::Query::sql(sql.to_string()),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await
//             .expect("sql should run")
//             .into_record_stream()
//             .expect("streamed result")
//             .try_collect::<Vec<_>>()
//             .await
//             .expect("sql should drain")
//             .iter()
//             .map(|batch| batch.num_rows())
//             .sum()
//     }

//     /// Tables persisted by one runtime are rebuilt by a fresh runtime via
//     /// `init_tables`: the base table's data survives, and the dependent view is
//     /// rebuilt in dependency order so it resolves the base table registered ahead
//     /// of it. This exercises the persist-on-register + startup-recovery round trip
//     /// that replaces the old `TableManager` registry.
//     ///
//     /// Also a regression guard for the store-lock lifecycle: reopening the *same*
//     /// redb file in one process only works because dropping the first runtime
//     /// releases its exclusive lock — the `SessionContext` cycles are broken in
//     /// `Drop for Runtime` plus the schema/crawler `Weak` back-references.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn persisted_tables_survive_a_restart() {
//         let suffix = uuid::Uuid::new_v4().simple();
//         let base = format!("restart_base_{suffix}");
//         let view = format!("restart_view_{suffix}");

//         // Both runtimes share one config and one tables-store path so the restart
//         // reopens the same store and rebuilds from what the first runtime persisted.
//         let config = std::sync::Arc::new(beacon_config::Config::load().unwrap());
//         let db_path = super::test_db_path();

//         // First runtime: create a base table with data and a view over it.
//         let runtime = Runtime::new_with_in_memory_auth_at(config.clone(), db_path.clone())
//             .await
//             .expect("runtime should start");
//         run_sql(&runtime, &format!("CREATE TABLE {base} (a BIGINT)")).await;
//         run_sql(&runtime, &format!("INSERT INTO {base} VALUES (1), (2)")).await;
//         run_sql(
//             &runtime,
//             &format!("CREATE VIEW {view} AS SELECT a FROM {base}"),
//         )
//         .await;
//         drop(runtime);

//         // A fresh runtime rebuilds the catalog purely from the persisted
//         // `db://<name>/table.json` definitions.
//         let restarted = Runtime::new_with_in_memory_auth_at(config, db_path)
//             .await
//             .expect("runtime should restart");

//         assert_eq!(
//             count_rows(&restarted, &format!("SELECT * FROM {base}")).await,
//             2,
//             "base table data should persist across a restart"
//         );
//         assert_eq!(
//             count_rows(&restarted, &format!("SELECT * FROM {view}")).await,
//             2,
//             "the view should be rebuilt and resolve its dependency after a restart"
//         );

//         // Cleanup so the shared on-disk tables store does not leak into other
//         // tests (best-effort; `DROP TABLE` deregisters either provider type).
//         let _ = restarted
//             .run_query(
//                 crate::query::Query::sql(format!("DROP TABLE {view}")),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await;
//         let _ = restarted
//             .run_query(
//                 crate::query::Query::sql(format!("DROP TABLE {base}")),
//                 beacon_auth::AuthIdentity::system(),
//             )
//             .await;
//     }
// }

// #[cfg(test)]
// mod external_table_sql_tests {
//     use super::build_create_external_table_sql;
//     use crate::api::CreateExternalTableRequest;
//     use std::collections::HashMap;

//     fn req(name: &str, file_type: &str, location: &str) -> CreateExternalTableRequest {
//         CreateExternalTableRequest {
//             name: name.to_string(),
//             location: location.to_string(),
//             file_type: file_type.to_string(),
//             partition_cols: vec![],
//             options: HashMap::new(),
//             if_not_exists: false,
//         }
//     }

//     #[test]
//     fn builds_minimal_statement() {
//         let sql = build_create_external_table_sql(&req("obs_ext", "PARQUET", "obs/")).unwrap();
//         assert_eq!(
//             sql,
//             "CREATE EXTERNAL TABLE obs_ext STORED AS PARQUET LOCATION 'obs/'"
//         );
//     }

//     #[test]
//     fn builds_with_if_not_exists_partitions_and_sorted_options() {
//         let mut r = req("argo", "NETCDF", "argo/**/*.nc");
//         r.if_not_exists = true;
//         r.partition_cols = vec!["year".to_string(), "month".to_string()];
//         r.options
//             .insert("read_dimensions".to_string(), "lat,lon".to_string());
//         r.options.insert("a_flag".to_string(), "true".to_string());

//         let sql = build_create_external_table_sql(&r).unwrap();
//         assert_eq!(
//             sql,
//             "CREATE EXTERNAL TABLE IF NOT EXISTS argo STORED AS NETCDF LOCATION 'argo/**/*.nc' \
//              PARTITIONED BY (year, month) OPTIONS ('a_flag' 'true', 'read_dimensions' 'lat,lon')"
//         );
//     }

//     #[test]
//     fn escapes_quotes_in_location_and_option_values() {
//         let mut r = req("t", "CSV", "weird'/path");
//         r.options.insert("delimiter".to_string(), "'".to_string());

//         let sql = build_create_external_table_sql(&r).unwrap();
//         assert_eq!(
//             sql,
//             "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'weird''/path' OPTIONS ('delimiter' '''')"
//         );
//     }

//     #[test]
//     fn rejects_unsafe_identifiers() {
//         assert!(build_create_external_table_sql(&req("bad name", "PARQUET", "x/")).is_err());
//         assert!(build_create_external_table_sql(&req("t; DROP TABLE u", "PARQUET", "x/")).is_err());
//         assert!(build_create_external_table_sql(&req("t", "PARQUET'", "x/")).is_err());

//         let mut r = req("t", "PARQUET", "x/");
//         r.partition_cols = vec!["year)".to_string()];
//         assert!(build_create_external_table_sql(&r).is_err());
//     }
// }

// #[cfg(test)]
// mod crawler_admin_tests {
//     use super::Runtime;
//     use crate::api::{CreateCrawlerRequest, TableNamingView};
//     use std::collections::HashMap;

//     /// The admin crawler API round-trips through the manager: create -> list/get ->
//     /// run (zero discovery over an empty prefix) -> drop (idempotency error on the
//     /// second drop). Uses a unique name so the shared on-disk store does not leak.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn crawler_create_list_get_run_drop_round_trip() {
//         let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
//             beacon_config::Config::load().unwrap(),
//         ))
//         .await
//         .expect("runtime should start");

//         let name = format!("crawler_test_{}", uuid::Uuid::new_v4().simple());

//         runtime
//             .create_crawler(CreateCrawlerRequest {
//                 name: name.clone(),
//                 target_prefix: format!("{name}/"),
//                 format_filter: Some(vec!["parquet".to_string()]),
//                 table_naming: TableNamingView::CrawlerPrefixed,
//                 detect_partitions: true,
//                 schedule_secs: None,
//                 event_driven: false,
//                 options: HashMap::new(),
//             })
//             .await
//             .expect("create_crawler should succeed");

//         // Listed and individually retrievable, with fields preserved.
//         assert!(runtime.list_crawlers().iter().any(|c| c.name == name));
//         let got = runtime
//             .get_crawler(&name)
//             .expect("crawler should be retrievable");
//         assert_eq!(got.target_prefix, format!("{name}/"));
//         assert_eq!(got.table_naming, TableNamingView::CrawlerPrefixed);

//         // Runs on demand: an empty prefix yields a zero-discovery report.
//         let report = runtime
//             .run_crawler(&name)
//             .await
//             .expect("run_crawler should succeed");
//         assert_eq!(report.crawler, name);
//         assert_eq!(report.discovered, 0);

//         // Dropped: gone from the catalog, and a second drop errors.
//         runtime
//             .drop_crawler(&name)
//             .await
//             .expect("drop_crawler should succeed");
//         assert!(runtime.get_crawler(&name).is_none());
//         assert!(runtime.drop_crawler(&name).await.is_err());
//     }
// }
