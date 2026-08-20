//! High-level Beacon runtime shared by the API transports.

use std::{path::PathBuf, sync::Arc};

use crate::metrics::MetricsTracker;
use arrow::{array::AsArray, datatypes::UInt64Type};
use beacon_datafusion_ext::consts::TMP_STORE_URL_OBJECT_URL;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;

use crate::{
    parser::{beacon_parser::BeaconParser, statement::BeaconStatement},
    query_metrics_store::QueryMetricsStore,
    query_result::{ArrowOutputStream, QueryOutput, QueryResult},
};

/// Beacon's single execution layer.
///
/// The interface is deliberately narrow: authenticate a caller, then run a query.
/// Most of what a consumer wants to know — extensions, crawlers, the tables in a
/// schema — is reachable as SQL (`SHOW EXTENSIONS`, `SHOW CRAWLERS`, a scan), so
/// they ask for it in a query and map the resulting `RecordBatch`es into their
/// own types rather than calling a typed accessor per thing.
///
/// What is left here is what a caller *cannot* ask for themselves: reads of the
/// metadata schemas, which are the super-user's alone ([`Self::visible_tables`],
/// [`Self::show_functions`]), a table's schema straight from its provider
/// ([`Self::table_arrow_schema`]), and the runtime's own bookkeeping
/// ([`Self::get_query_metrics`]). Those answer in Arrow and in beacon's own
/// types; turning them into a wire format is the transport's job, not this
/// one's.
pub struct Runtime {
    pub(crate) session_ctx: Arc<SessionContext>,
    /// Where a completed query's metrics are written: the managed table that
    /// `beacon.system.query_metrics` exposes, which is how callers read them.
    pub(crate) query_metrics: Arc<QueryMetricsStore>,
    /// Authentication + authorization context (users, roles, grants). Shared, owned by the runtime.
    pub(crate) auth: Arc<beacon_auth::AuthContext>,

    /// Whether table-level grants are enforced for non-super-users.
    pub(crate) auth_enforce: bool,

    /// Whether this runtime was opened read-only: any statement that does not produce a result set
    /// (writes, DDL/DML, and beacon's side-effecting extension statements) is refused.
    pub(crate) read_only: bool,

    /// The background file-statistics service, when enabled. Held so the runtime
    /// owns its timer: dropping the runtime aborts it.
    pub(crate) file_stats: Option<Arc<crate::file_stats::FileStatsService>>,

    /// tmp directory for storing temporary files (e.g. for query output)
    pub(crate) tmp_dir: PathBuf,
}

impl Runtime {
    pub fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    /// The runtime's auth context (users, roles, grants).
    pub fn auth(&self) -> &Arc<beacon_auth::AuthContext> {
        &self.auth
    }

    /// Resolves a credential to an identity, erroring when it is not valid.
    pub async fn authenticate(
        &self,
        credential: &beacon_auth::Credential,
    ) -> anyhow::Result<beacon_auth::AuthIdentity> {
        self.auth.authenticate(credential).await
    }

    /// Resolves the anonymous principal's identity, erroring when anonymous access is disabled.
    pub async fn authenticate_anonymous(&self) -> anyhow::Result<beacon_auth::AuthIdentity> {
        self.auth.authenticate_anonymous().await
    }

    /// Whether anonymous access is enabled.
    pub fn anonymous_enabled(&self) -> bool {
        self.auth.anonymous_enabled()
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
        let crate::query::Query {
            inner,
            output,
            params,
        } = query;

        let mut plan = self.lower_query(inner).await?;
        // Bind positional parameters into the placeholders the planner produced. Done before
        // validation/authorization so those see the concrete plan, and only when parameters
        // were supplied — `with_param_values` errors if a plan has no placeholders but values
        // were given, which is exactly the mismatch a caller wants reported.
        if !params.is_empty() {
            plan = plan
                .with_param_values(params)
                .map_err(|e| anyhow::anyhow!("failed to bind query parameters: {e}"))?;
        }
        crate::statement_plan::validate_query_plan(&plan, identity.is_super_user)?;
        // A read-only database refuses anything that would mutate it. `plan_produces_result_set`
        // is the read/write discriminator: reads (SELECT, VALUES, `SHOW …`) produce rows; every
        // write (DDL/DML/COPY/SET and beacon's side-effecting extension statements, e.g. `ATTACH`,
        // `CREATE SECRET`) reports an empty schema.
        if self.read_only && !crate::statement_plan::plan_produces_result_set(&plan) {
            anyhow::bail!(
                "this database was opened read-only; writes are not permitted on this connection"
            );
        }
        crate::statement_plan::authorize_logical_plan(
            &plan,
            &self.session_ctx,
            &self.auth,
            &identity,
            self.auth_enforce,
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
                self.run_query_to_file(plan, output, query_id, query_json, &identity.username)
                    .await
            }
            None => {
                self.run_query_to_stream(plan, query_id, query_json, &identity.username)
                    .await
            }
        }
    }

    /// Stream the plan's results, wrapping the stream so output rows/bytes are
    /// recorded under `query_id` as the client drains it.
    async fn run_query_to_stream(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
        query_id: uuid::Uuid,
        query_json: serde_json::Value,
        username: &str,
    ) -> anyhow::Result<QueryResult> {
        let metrics = MetricsTracker::new(query_json, query_id, username);
        metrics.set_logical_plan(&plan);
        // Record the optimized logical plan alongside the parsed one. `optimize`
        // errors for `Statement` plans (`SET ...`), which have nothing to
        // optimize — leave the column null in that case rather than failing.
        if let Ok(optimized) = self.session_ctx.state().optimize(&plan) {
            metrics.set_optimized_logical_plan(&optimized);
        }
        let (stream, physical_plan) =
            crate::statement_plan::execute_statement_plan_tracked(&self.session_ctx, plan).await?;
        // The physical plan's per-node metrics fill in as the stream drains, and the
        // stream consolidates when it ends — so registering the plan here is what
        // makes `node_metrics` non-empty in `beacon.system.query_metrics`.
        if let Some(physical_plan) = physical_plan {
            metrics.set_physical_plan(physical_plan);
        }
        let output_stream = ArrowOutputStream::new(stream, metrics, self.query_metrics.clone());
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
        username: &str,
    ) -> anyhow::Result<QueryResult> {
        // `Output::parse` wraps the (already validated) plan in a `COPY TO` the
        // temp file; this COPY is beacon-generated, so it is not re-validated.
        //
        // `self.tmp_dir` is load-bearing coupling: it is both the tmp object store's
        // root (where object-store writers land `tmp://<name>`) and the NetCDF/ODV
        // factory's `output_dir` (registered from `storage.tmp_dir` in
        // `register_file_formats`, where the native sinks reconstruct
        // `output_dir.join(<name>)`). They MUST be the same directory or the written
        // bytes are invisible to the returned `QueryOutputFile`.
        let (copy_plan, output_file) = output
            .parse(
                &self.session_ctx,
                &self.tmp_dir,
                &TMP_STORE_URL_OBJECT_URL,
                plan,
            )
            .await?;

        let metrics = MetricsTracker::new(query_json, query_id, username);
        metrics.set_logical_plan(&copy_plan);
        if let Ok(optimized) = self.session_ctx.state().optimize(&copy_plan) {
            metrics.set_optimized_logical_plan(&optimized);
        }
        let (mut stream, physical_plan) =
            crate::statement_plan::execute_statement_plan_tracked(&self.session_ctx, copy_plan)
                .await?;
        if let Some(physical_plan) = physical_plan {
            metrics.set_physical_plan(physical_plan);
        }
        // The COPY emits a single `count` row; drain it to complete the write.
        while let Some(batch) = stream.try_next().await? {
            let counts = batch.column(0).as_primitive::<UInt64Type>();
            if !counts.is_empty() {
                metrics.add_output_rows(counts.value(0));
            }
        }
        metrics.add_output_bytes(output_file.size()?);
        // The file is written, so this query is complete — record it here rather
        // than at stream end, as the streamed path does.
        self.query_metrics
            .record(metrics.get_consolidated_metrics())
            .await;

        Ok(QueryResult {
            query_output: QueryOutput::File(output_file),
            query_id,
        })
    }

    /// A registered table's Arrow schema, taken from the table provider itself.
    ///
    /// Deliberately **not** `SELECT * FROM t LIMIT 0`: planning a scan forces the
    /// whole read path to resolve, and for an N-dimensional table whose variables
    /// cannot be broadcast onto a common shape that fails — so a zero-row query
    /// errors on a table whose schema is perfectly well defined. Asking the
    /// provider skips planning entirely, costs no I/O, and answers for every table
    /// a scan could not.
    ///
    /// Authorization matches the read path in
    /// [`authorize_logical_plan`](crate::statement_plan::authorize_logical_plan):
    /// the internal `__beacon_*` tables and the metadata schemas
    /// (`beacon.system`, `information_schema`) are super-user only regardless of
    /// enforcement, and otherwise a non-super-user needs `Select` on the table. A
    /// schema is metadata about data the caller may not read, so it is gated the
    /// same way the rows are.
    pub async fn table_arrow_schema(
        &self,
        table: impl Into<datafusion::sql::TableReference>,
        identity: &beacon_auth::AuthIdentity,
    ) -> anyhow::Result<arrow::datatypes::SchemaRef> {
        use beacon_auth::{ConcreteTarget, Privilege};
        use beacon_datafusion_ext::table_ext::INTERNAL_TABLE_PREFIX;

        let table: datafusion::sql::TableReference = table.into();

        if !identity.is_super_user {
            if table.table().starts_with(INTERNAL_TABLE_PREFIX) {
                anyhow::bail!(
                    "permission denied: the internal '{INTERNAL_TABLE_PREFIX}*' tables are \
                     restricted to the super-user"
                );
            }
            if let Some(schema) = table
                .schema()
                .filter(|s| crate::system_schema::is_metadata_schema(s))
            {
                anyhow::bail!(
                    "permission denied: the '{schema}' schema is restricted to the super-user"
                );
            }
        }

        if self.auth_enforce
            && !identity.is_super_user
            && !self.auth.is_allowed(
                &identity.roles,
                Privilege::Select,
                &ConcreteTarget::Table(table.table().to_string()),
            )
        {
            anyhow::bail!("permission denied: SELECT on table {}", table.table());
        }

        let provider = self
            .session_ctx
            .table_provider(table.clone())
            .await
            .map_err(|e| anyhow::anyhow!("table '{table}' could not be resolved: {e}"))?;
        Ok(provider.schema())
    }

    /// The catalog and schema an unqualified table name resolves against.
    pub fn default_catalog_and_schema(&self) -> (String, String) {
        let options = self.session_ctx.copied_config().options().catalog.clone();
        (options.default_catalog, options.default_schema)
    }

    /// The `information_schema.tables` rows `identity` is entitled to see, as
    /// Arrow batches — `table_catalog`, `table_schema`, `table_name`,
    /// `table_type`, ordered by the first three.
    ///
    /// This is how a transport lists the catalog. It cannot query
    /// `information_schema` as the caller any more — that schema is the
    /// super-user's — so the runtime reads it as the engine and drops every row
    /// [`Self::can_list_table`] rejects: no metadata schemas, no internal tables,
    /// and, with grant enforcement on, only the tables the caller's roles grant
    /// `Select` on. A listing therefore shows exactly what the caller could go on
    /// to read.
    pub async fn visible_tables(
        &self,
        identity: &beacon_auth::AuthIdentity,
    ) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        // Read directly through the session rather than `run_query`: this is the
        // engine reading its own catalog on a caller's behalf, so it is neither
        // subject to the caller's authorization (the filter below is) nor worth
        // recording as one of their queries.
        let batches = self
            .session_ctx
            .sql(
                "SELECT table_catalog, table_schema, table_name, table_type \
                 FROM information_schema.tables \
                 ORDER BY table_catalog, table_schema, table_name",
            )
            .await?
            .collect()
            .await?;

        // A super-user keeps every row, so there is nothing to filter or rebuild.
        if identity.is_super_user {
            return Ok(batches);
        }

        let mut visible = Vec::with_capacity(batches.len());
        for batch in &batches {
            let column = |name: &str| {
                batch
                    .column_by_name(name)
                    .map(|array| array.as_string::<i32>())
            };
            let (Some(schemas), Some(names)) = (column("table_schema"), column("table_name"))
            else {
                anyhow::bail!("information_schema.tables is missing an expected column");
            };
            let mask: arrow::array::BooleanArray = (0..batch.num_rows())
                .map(|row| {
                    Some(self.can_list_table(identity, Some(schemas.value(row)), names.value(row)))
                })
                .collect();
            visible.push(arrow::compute::filter_record_batch(batch, &mask)?);
        }
        Ok(visible)
    }

    /// Whether `identity` may see `schema`.`table` in a catalog listing.
    ///
    /// The same rule the read path enforces, asked ahead of time: a name a caller
    /// could not read is a name they are not shown. `schema` is `None` for an
    /// unqualified reference, which resolves in the default schema.
    pub fn can_list_table(
        &self,
        identity: &beacon_auth::AuthIdentity,
        schema: Option<&str>,
        table: &str,
    ) -> bool {
        use beacon_auth::{ConcreteTarget, Privilege};
        use beacon_datafusion_ext::table_ext::INTERNAL_TABLE_PREFIX;

        if identity.is_super_user {
            return true;
        }
        if table.starts_with(INTERNAL_TABLE_PREFIX) {
            return false;
        }
        if schema.is_some_and(crate::system_schema::is_metadata_schema) {
            return false;
        }
        // With enforcement off every table is readable, so listing them all is
        // what the listing promises: what you see is what you can read.
        !self.auth_enforce
            || self.auth.is_allowed(
                &identity.roles,
                Privilege::Select,
                &ConcreteTarget::Table(table.to_string()),
            )
    }

    /// DataFusion's function catalog — the `SHOW FUNCTIONS` result, as Arrow
    /// batches: `function_name`, `return_type`, `parameters`, `parameter_types`,
    /// `function_type`, `description`, `syntax_example`, one row per overload
    /// signature.
    ///
    /// The only catalog of the scalar, aggregate, and window functions: beacon
    /// keeps no copy. Table-valued functions are not in it — DataFusion does not
    /// catalog UDTFs.
    ///
    /// Engine documentation, not user data, so every caller may read it —
    /// including an anonymous one. That is why it is served from here rather than
    /// by running the statement as the caller: it reads `information_schema`,
    /// which is super-user-only.
    pub async fn show_functions(&self) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        Ok(self
            .session_ctx
            .sql("SHOW FUNCTIONS")
            .await?
            .collect()
            .await?)
    }

    /// The metrics recorded for one previously executed query, as Arrow — the
    /// row `beacon.system.query_metrics` holds for it, for transports that
    /// address a single query by id. Empty batches when the id is unknown.
    pub async fn get_query_metrics(
        &self,
        query_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        self.query_metrics.read(query_id).await
    }

    /// Explain a query without running it: returns its logical plan as
    /// PostgreSQL-style JSON (pgjson), the shape plan visualizers consume.
    ///
    /// `EXPLAIN` is read-only — nothing is executed — so the plan is validated
    /// against the *anonymous* permission set regardless of who is asking. A
    /// caller must still be authorized to read the tables the plan touches.
    pub async fn explain_query(
        &self,
        query: crate::query::Query,
        identity: beacon_auth::AuthIdentity,
    ) -> anyhow::Result<String> {
        // `output` (file format) is meaningless here: only the query body is planned.
        let plan = self.lower_query(query.inner).await?;
        crate::statement_plan::validate_query_plan(&plan, false)?;
        crate::statement_plan::authorize_logical_plan(
            &plan,
            &self.session_ctx,
            &self.auth,
            &identity,
            self.auth_enforce,
        )?;
        // Bound to a local: the `Display` wrapper borrows `plan`, so formatting
        // it inline would drop `plan` while the temporary still holds the borrow.
        let pg_json = plan.display_pg_json().to_string();
        Ok(pg_json)
    }

    /// Run a query and return its physical plan as pgjson annotated with
    /// per-node runtime metrics — the `EXPLAIN ANALYZE` analog of
    /// [`Self::explain_query`].
    ///
    /// Unlike `explain_query` this **executes** the plan to completion to
    /// populate the metrics, so it applies the same gate the normal query path
    /// uses: anonymous/role callers are held to `validate_query_plan`, while a
    /// super-user may also analyze DDL/DML — with the same side effects running
    /// it through `run_query` would have. A read-only runtime therefore refuses
    /// anything that does not produce a result set, exactly as `run_query` does.
    pub async fn explain_analyze_query(
        &self,
        query: crate::query::Query,
        identity: beacon_auth::AuthIdentity,
    ) -> anyhow::Result<String> {
        let plan = self.lower_query(query.inner).await?;
        crate::statement_plan::validate_query_plan(&plan, identity.is_super_user)?;
        if self.read_only && !crate::statement_plan::plan_produces_result_set(&plan) {
            anyhow::bail!(
                "this database was opened read-only; writes are not permitted on this connection"
            );
        }
        crate::statement_plan::authorize_logical_plan(
            &plan,
            &self.session_ctx,
            &self.auth,
            &identity,
            self.auth_enforce,
        )?;

        // Keep the `Arc` so per-node metrics can be read once the stream drains.
        // `execute_statement_plan` discards the plan, so create/execute are inlined.
        let physical_plan = self.session_ctx.state().create_physical_plan(&plan).await?;
        let mut stream = datafusion::physical_plan::execute_stream(
            physical_plan.clone(),
            self.session_ctx.task_ctx(),
        )?;
        // Drain to completion so every node's metrics are fully recorded.
        while stream.try_next().await?.is_some() {}

        Ok(crate::metrics::explain_analyze_pg_json(physical_plan.as_ref()).to_string())
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
            BeaconStatement::AnalyzeFiles(statement) => {
                Ok(crate::statement_plan::analyze_files_plan(statement))
            }
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
            BeaconStatement::Attach(statement) => {
                crate::statement_plan::attach_plan(statement)
            }
            BeaconStatement::Detach(statement) => {
                Ok(crate::statement_plan::detach_plan(statement))
            }
            BeaconStatement::CreateSecret(statement) => {
                crate::statement_plan::create_secret_plan(statement)
            }
            BeaconStatement::DropSecret(statement) => {
                Ok(crate::statement_plan::drop_secret_plan(statement))
            }
            BeaconStatement::ShowSecrets => Ok(crate::statement_plan::show_secrets_plan()),
            BeaconStatement::Summarize(statement) => {
                crate::statement_plan::summarize_plan(&self.session_ctx, statement).await
            }
            BeaconStatement::DFStatement(statement) => {
                crate::statement_plan::lower_df_statement(&self.session_ctx, *statement).await
            }
        }
    }

    fn parse_beacon_statement(sql: &str) -> anyhow::Result<BeaconStatement> {
        let mut parser = BeaconParser::new(sql)?;
        parser.parse_statement().map_err(Into::into)
    }
}

#[cfg(test)]
mod test_support {
    use std::path::{Path, PathBuf};

    use beacon_datafusion_ext::listing_factory::RootStore;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use tempfile::TempDir;

    use super::Runtime;
    use crate::runtime_builder::RuntimeBuilder;

    /// A unit-test runtime plus the temp root backing its datasets/tmp dirs. Dropping
    /// it removes the root.
    pub(crate) struct TestRt {
        pub runtime: Runtime,
        pub root: TempDir,
    }

    impl TestRt {
        /// The tmp store root — where query-output files are written and read back.
        pub fn tmp_dir(&self) -> PathBuf {
            self.root.path().join("tmp")
        }
    }

    /// Build a runtime over `root`: a local datasets store rooted at `root/datasets`
    /// (so a relative `LOCATION` resolves there), a tmp dir at `root/tmp`, and an
    /// in-memory tables store unless `db_path` is given (a persistent redb file, used
    /// by the restart test). No admin is bootstrapped — tests authenticate as
    /// `AuthIdentity::system()` (super-user) or `::empty()` (non-super).
    pub(crate) async fn build_runtime(root: &Path, db_path: Option<PathBuf>) -> Runtime {
        let datasets = root.join("datasets");
        let tmp = root.join("tmp");
        std::fs::create_dir_all(&datasets).unwrap();
        std::fs::create_dir_all(&tmp).unwrap();
        let mut builder = RuntimeBuilder::new()
            .with_default_store(
                ObjectStoreUrl::parse("datasets://").unwrap(),
                RootStore::FileSystem(datasets),
            )
            .with_tmp_dir_path(tmp);
        if let Some(db) = db_path {
            builder = builder.with_db_path(db);
        }
        builder.build().await.expect("test runtime should build")
    }

    /// The default unit-test runtime: isolated temp root, in-memory tables store.
    pub(crate) async fn test_runtime() -> TestRt {
        let root = TempDir::new().expect("temp root");
        let runtime = build_runtime(root.path(), None).await;
        TestRt { runtime, root }
    }
}

#[cfg(test)]
mod materialized_view_tests {
    use super::test_support::test_runtime;
    use super::Runtime;
    use futures::TryStreamExt;

    async fn collect_sql(
        runtime: &Runtime,
        sql: &str,
    ) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        let batches = runtime
            .run_query(
                crate::query::Query::sql(sql.to_string()),
                beacon_auth::AuthIdentity::system(),
            )
            .await?
            .into_record_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(batches)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn materialized_view_create_query_refresh_and_drop() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;

        // CREATE
        collect_sql(runtime, "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a, 2 AS b")
            .await
            .expect("create materialized view should succeed");

        // QUERY reads from the persisted Parquet result.
        let batches = collect_sql(runtime, "SELECT * FROM mv")
            .await
            .expect("query of materialized view should succeed");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
        assert_eq!(batches[0].num_columns(), 2);

        // REFRESH recomputes and replaces the stored data.
        collect_sql(runtime, "REFRESH mv")
            .await
            .expect("refresh should succeed");
        let rows: usize = collect_sql(runtime, "SELECT * FROM mv")
            .await
            .expect("query after refresh should succeed")
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 1);

        // REFRESH of an unknown view fails clearly.
        let err = collect_sql(runtime, "REFRESH mv_missing")
            .await
            .expect_err("refresh of unknown view should fail");
        assert!(
            err.to_string().contains("does not exist"),
            "unexpected error: {err}"
        );

        // REFRESH of a regular view fails clearly.
        collect_sql(runtime, "CREATE VIEW rv AS SELECT 1 AS a")
            .await
            .expect("create regular view should succeed");
        let err = collect_sql(runtime, "REFRESH rv")
            .await
            .expect_err("refresh of a regular view should fail");
        assert!(
            err.to_string().contains("is not a materialized view"),
            "unexpected error: {err}"
        );

        // DROP removes the materialized view; it is no longer queryable.
        collect_sql(runtime, "DROP TABLE mv")
            .await
            .expect("drop should succeed");
        assert!(
            collect_sql(runtime, "SELECT * FROM mv").await.is_err(),
            "materialized view should not be queryable after drop"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn materialized_view_handles_zero_row_result() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;

        // A query with no rows must still create a queryable, Parquet-backed view.
        collect_sql(
            runtime,
            "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a WHERE false",
        )
        .await
        .expect("create materialized view with empty result should succeed");

        let batches = collect_sql(runtime, "SELECT * FROM mv")
            .await
            .expect("query of empty materialized view should succeed");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0);

        // Refresh of an empty result must also succeed and stay empty.
        collect_sql(runtime, "REFRESH mv")
            .await
            .expect("refresh of empty materialized view should succeed");
        let rows: usize = collect_sql(runtime, "SELECT * FROM mv")
            .await
            .expect("query after refresh should succeed")
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 0);

        collect_sql(runtime, "DROP TABLE mv")
            .await
            .expect("drop should succeed");
    }
}

#[cfg(test)]
mod client_query_tests {
    use super::test_support::{test_runtime, TestRt};
    use super::Runtime;
    use crate::query_result::QueryOutput;
    use futures::TryStreamExt;

    async fn run_sql(runtime: &Runtime, sql: &str) {
        runtime
            .run_query(
                crate::query::Query::sql(sql.to_string()),
                beacon_auth::AuthIdentity::system(),
            )
            .await
            .expect("sql should run")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("sql should drain");
    }

    /// Like `run_sql`, but returns the result (draining the stream) so callers can
    /// assert on failures from side-effecting statements.
    async fn try_run_sql(runtime: &Runtime, sql: &str) -> anyhow::Result<()> {
        runtime
            .run_query(
                crate::query::Query::sql(sql.to_string()),
                beacon_auth::AuthIdentity::system(),
            )
            .await?
            .into_record_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    /// A client query, as the transport hands it over: the request body's keys
    /// are flattened onto `Query` itself.
    fn query(value: serde_json::Value) -> crate::query::Query {
        serde_json::from_value(value).expect("query body should deserialize")
    }

    /// A JSON client query is lowered to a `LogicalPlan` and executed through the
    /// same pipeline as `run_sql`, returning a streamed result.
    #[tokio::test(flavor = "multi_thread")]
    async fn json_query_runs_through_unified_path() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;

        run_sql(runtime, "CREATE TABLE t (a BIGINT, b BIGINT)").await;
        run_sql(runtime, "INSERT INTO t VALUES (1, 2), (3, 4)").await;

        let batches = runtime
            .run_query(
                query(serde_json::json!({ "from": "t", "select": ["a", "b"] })),
                beacon_auth::AuthIdentity::system(),
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

    /// `SET EXTENSION` / `SHOW EXTENSIONS` / `DROP EXTENSION` round-trip end to
    /// end, and an extension referencing a missing column is rejected.
    #[tokio::test(flavor = "multi_thread")]
    async fn table_extensions_sql_round_trip() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;

        run_sql(runtime, "CREATE TABLE ext (lat BIGINT, depth BIGINT)").await;

        // `SHOW EXTENSIONS` is the only way to read extensions back — there is no
        // typed accessor on the runtime.
        async fn show_extensions(runtime: &Runtime) -> serde_json::Value {
            let batches = runtime
                .run_query(
                    crate::query::Query::sql("SHOW EXTENSIONS FOR ext".to_string()),
                    beacon_auth::AuthIdentity::system(),
                )
                .await
                .expect("show extensions should run")
                .into_record_stream()
                .expect("streamed result")
                .try_collect::<Vec<_>>()
                .await
                .expect("stream should drain");
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 1, "SHOW EXTENSIONS returns one row");
            let json = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("extensions column is Utf8")
                .value(0);
            serde_json::from_str(json).expect("extensions column should be JSON")
        }

        run_sql(
            runtime,
            "SET EXTENSION 'preset' FOR ext TO '{\"presets\":[{\"name\":\"shallow\",\"filters\":[{\"column\":\"depth\",\"op\":\"<=\",\"value\":10}]}]}'",
        )
        .await;

        let extensions = show_extensions(runtime).await;
        assert_eq!(
            extensions
                .pointer("/preset/presets/0/name")
                .and_then(|name| name.as_str()),
            Some("shallow"),
            "SHOW output should include the preset: {extensions}"
        );

        // An extension over a non-existent column is rejected by validation.
        let rejected = try_run_sql(
            runtime,
            "SET EXTENSION 'preset' FOR ext TO '{\"presets\":[{\"name\":\"x\",\"filters\":[{\"column\":\"ghost\",\"op\":\"=\",\"value\":1}]}]}'",
        )
        .await;
        assert!(
            rejected.is_err(),
            "preset over a missing column should be rejected"
        );

        // DROP removes it; the document becomes empty.
        run_sql(runtime, "DROP EXTENSION 'preset' FOR ext").await;
        let extensions = show_extensions(runtime).await;
        assert!(
            extensions.get("preset").is_none(),
            "dropping the only extension leaves an empty document, got: {extensions}"
        );
    }

    /// A query with an `output` format is written to a file and returned as a
    /// file download.
    #[tokio::test(flavor = "multi_thread")]
    async fn query_with_output_format_produces_file() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;

        run_sql(runtime, "CREATE TABLE out_t (a BIGINT)").await;
        run_sql(runtime, "INSERT INTO out_t VALUES (1), (2)").await;

        let result = runtime
            .run_query(
                query(serde_json::json!({
                    "from": "out_t",
                    "select": ["a"],
                    "output": { "format": "csv" },
                })),
                beacon_auth::AuthIdentity::system(),
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

    /// NetCDF output goes through a custom `DataSink` that writes a real local
    /// file (the netcdf-c writer cannot stream to an object store). This asserts
    /// the file lands under the configured tmp store root — not the OS temp dir —
    /// and is non-empty.
    #[tokio::test(flavor = "multi_thread")]
    async fn query_with_netcdf_output_writes_under_configured_tmp() {
        let rt = test_runtime().await;
        let tmp_dir = rt.tmp_dir();
        let runtime = &rt.runtime;

        run_sql(runtime, "CREATE TABLE nc_t (a BIGINT)").await;
        run_sql(runtime, "INSERT INTO nc_t VALUES (1), (2)").await;

        let result = runtime
            .run_query(
                query(serde_json::json!({
                    "from": "nc_t",
                    "select": ["a"],
                    "output": { "format": "netcdf" },
                })),
                beacon_auth::AuthIdentity::system(),
            )
            .await
            .expect("netcdf query with output should run");

        match result.query_output {
            QueryOutput::File(file) => {
                // tempfile resolves to an absolute path; canonicalize both sides so
                // the comparison is independent of cwd-relative form.
                let got = std::fs::canonicalize(file.path().parent().unwrap())
                    .expect("canonicalize output parent");
                let want = std::fs::canonicalize(&tmp_dir).expect("canonicalize tmp dir");
                assert_eq!(
                    got, want,
                    "netcdf output should be written under the configured tmp dir"
                );
                assert!(
                    file.size().expect("file size") > 0,
                    "netcdf output file should contain data"
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
        let rt: TestRt = test_runtime().await;
        let runtime = &rt.runtime;

        // Super-user seeds a table.
        run_sql(runtime, "CREATE TABLE val (a BIGINT)").await;

        // Non-super-user: read-only SELECT is allowed.
        runtime
            .run_query(
                crate::query::Query::sql("SELECT * FROM val".to_string()),
                beacon_auth::AuthIdentity::empty(),
            )
            .await
            .expect("non-super SELECT should be allowed")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("SELECT should drain");

        // Non-super-user: standard DDL is rejected (by validate_query_plan).
        runtime
            .run_query(
                crate::query::Query::sql("CREATE TABLE val_2 (a BIGINT)".to_string()),
                beacon_auth::AuthIdentity::empty(),
            )
            .await
            .err()
            .expect("non-super CREATE TABLE should be rejected");

        // Non-super-user: a beacon extension operation is rejected (super-user only).
        let err = runtime
            .run_query(
                crate::query::Query::sql(
                    "CREATE MATERIALIZED VIEW val_mv AS SELECT 1 AS a".to_string(),
                ),
                beacon_auth::AuthIdentity::empty(),
            )
            .await
            .err()
            .expect("non-super CREATE MATERIALIZED VIEW should be rejected");
        assert!(
            err.to_string().contains("super-user"),
            "unexpected error: {err}"
        );
    }

    /// A non-super-user cannot escalate its own privileges: all auth-management
    /// statements are super-user-only (they lower to extension nodes), so a
    /// role-limited caller cannot create a role, grant itself privileges, or create
    /// users. A super-user can run the read-only variants.
    #[tokio::test(flavor = "multi_thread")]
    async fn non_super_user_cannot_escalate_privileges() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;

        // A caller holding a non-super role (no global ALL grant).
        let limited = beacon_auth::AuthIdentity {
            username: "alice".to_string(),
            roles: vec!["reader".to_string()],
            is_super_user: false,
        };

        for sql in [
            "CREATE ROLE hacker",
            "GRANT ALL TO ROLE reader",
            "GRANT ALL TO ROLE hacker",
            "CREATE USER bob WITH PASSWORD 'pw'",
            "GRANT ROLE reader TO USER alice",
        ] {
            let err = runtime
                .run_query(crate::query::Query::sql(sql.to_string()), limited.clone())
                .await
                .err()
                .unwrap_or_else(|| panic!("non-super should be rejected: {sql}"));
            assert!(
                err.to_string().contains("super-user"),
                "expected a super-user error for `{sql}`, got: {err}"
            );
        }

        // The super-user can create read-only roles and grant SELECT.
        run_sql(runtime, "CREATE ROLE analyst").await;
        run_sql(runtime, "GRANT SELECT ON TABLE t TO ROLE analyst").await;

        // But even the super-user cannot grant write/management privileges to a role —
        // roles are read-only, so there is no way to mint another super-user through SQL.
        let err = runtime
            .run_query(
                crate::query::Query::sql("GRANT ALL TO ROLE analyst".to_string()),
                beacon_auth::AuthIdentity::system(),
            )
            .await
            .err()
            .expect("granting a write/ALL privilege to a role should be rejected");
        assert!(
            err.to_string().contains("read-only"),
            "expected a read-only-role error, got: {err}"
        );
    }

    /// End-to-end index lifecycle on a Lance-backed managed table: CREATE INDEX,
    /// SHOW INDEXES (one row), DROP INDEX (zero rows) — exercising the parser,
    /// planner, execs, and Lance index ops through the full SQL path.
    #[tokio::test(flavor = "multi_thread")]
    async fn create_show_drop_index_round_trip() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;

        run_sql(runtime, "CREATE TABLE idx (id BIGINT, name VARCHAR)").await;
        run_sql(runtime, "INSERT INTO idx VALUES (1, 'a'), (2, 'b')").await;
        run_sql(runtime, "CREATE INDEX idx_id_idx ON idx (id) USING btree").await;

        let count_indexes = |sql: String| {
            let runtime = &runtime;
            async move {
                runtime
                    .run_query(
                        crate::query::Query::sql(sql),
                        beacon_auth::AuthIdentity::system(),
                    )
                    .await
                    .expect("show indexes should run")
                    .into_record_stream()
                    .expect("streamed result")
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("stream should drain")
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
            }
        };

        assert_eq!(
            count_indexes("SHOW INDEXES ON idx".to_string()).await,
            1,
            "one index should be listed"
        );

        run_sql(runtime, "DROP INDEX idx_id_idx ON idx").await;
        assert_eq!(
            count_indexes("SHOW INDEXES ON idx".to_string()).await,
            0,
            "no indexes after drop"
        );

        run_sql(runtime, "DROP TABLE idx").await;
    }
}

#[cfg(test)]
mod restart_tests {
    use super::test_support::build_runtime;
    use super::Runtime;
    use futures::TryStreamExt;

    async fn run_sql(runtime: &Runtime, sql: &str) {
        runtime
            .run_query(
                crate::query::Query::sql(sql.to_string()),
                beacon_auth::AuthIdentity::system(),
            )
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
            .run_query(
                crate::query::Query::sql(sql.to_string()),
                beacon_auth::AuthIdentity::system(),
            )
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
    /// of it. This exercises the persist-on-register + startup-recovery round trip.
    ///
    /// Also a regression guard for the store-lock lifecycle: reopening the *same*
    /// redb file in one process only works because dropping the first runtime
    /// releases its exclusive lock.
    #[tokio::test(flavor = "multi_thread")]
    async fn persisted_tables_survive_a_restart() {
        // Both runtimes share one temp root (so the datasets/tmp dirs persist) and one
        // tables-store path, so the restart reopens the same store and rebuilds from
        // what the first runtime persisted.
        let root = tempfile::TempDir::new().expect("temp root");
        let db_path = root.path().join("beacon.db");

        // First runtime: create a base table with data and a view over it.
        let runtime = build_runtime(root.path(), Some(db_path.clone())).await;
        run_sql(&runtime, "CREATE TABLE restart_base (a BIGINT)").await;
        run_sql(&runtime, "INSERT INTO restart_base VALUES (1), (2)").await;
        run_sql(
            &runtime,
            "CREATE VIEW restart_view AS SELECT a FROM restart_base",
        )
        .await;
        drop(runtime);

        // A fresh runtime rebuilds the catalog purely from the persisted
        // `db://<name>/table.json` definitions.
        let restarted = build_runtime(root.path(), Some(db_path)).await;

        assert_eq!(
            count_rows(&restarted, "SELECT * FROM restart_base").await,
            2,
            "base table data should persist across a restart"
        );
        assert_eq!(
            count_rows(&restarted, "SELECT * FROM restart_view").await,
            2,
            "the view should be rebuilt and resolve its dependency after a restart"
        );
    }
}

#[cfg(test)]
mod crawler_sql_tests {
    use super::test_support::test_runtime;
    use super::Runtime;
    use futures::TryStreamExt;

    async fn collect(runtime: &Runtime, sql: &str) -> Vec<arrow::record_batch::RecordBatch> {
        runtime
            .run_query(
                crate::query::Query::sql(sql.to_string()),
                beacon_auth::AuthIdentity::system(),
            )
            .await
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should drain")
    }

    fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    /// The crawler admin surface is driven entirely through SQL now (there are no
    /// `Runtime::*_crawler` methods): `CREATE CRAWLER` → `SHOW CRAWLERS` → `RUN
    /// CRAWLER` (zero discovery over an empty prefix) → `DROP CRAWLER`.
    #[tokio::test(flavor = "multi_thread")]
    async fn crawler_create_show_run_drop_via_sql() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;
        // The (empty) target prefix must exist so the scan lists cleanly.
        std::fs::create_dir_all(rt.root.path().join("datasets/c_src")).unwrap();

        collect(runtime, "CREATE CRAWLER c ON 'c_src/' WITH ('format' 'parquet')").await;
        assert_eq!(
            total_rows(&collect(runtime, "SHOW CRAWLERS").await),
            1,
            "SHOW CRAWLERS should list the created crawler"
        );

        // Runs on demand; an empty prefix yields a zero-discovery report (no error).
        collect(runtime, "RUN CRAWLER c").await;

        // Dropped: gone from the catalog.
        collect(runtime, "DROP CRAWLER c").await;
        assert_eq!(
            total_rows(&collect(runtime, "SHOW CRAWLERS").await),
            0,
            "SHOW CRAWLERS should be empty after DROP CRAWLER"
        );
    }
}

// The former `external_table_sql_tests` module tested `build_create_external_table_sql`,
// a helper that no longer exists: `CREATE EXTERNAL TABLE` is now expressed directly in
// SQL (see the `tests/` integration suite, e.g. `tables_store_lock_release`), so there is
// no SQL-builder function left to unit test.

impl Runtime {
    /// The background file-statistics service, when the subsystem is enabled and
    /// this runtime has a database file to keep the registry in.
    pub fn file_stats(&self) -> Option<&Arc<crate::file_stats::FileStatsService>> {
        self.file_stats.as_ref()
    }
}

