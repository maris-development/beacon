//! High-level Beacon runtime shared by the API transports.

use std::{path::PathBuf, sync::Arc};

use crate::metrics::MetricsTracker;
use arrow::{array::AsArray, datatypes::UInt64Type};
use beacon_datafusion_ext::consts::TMP_STORE_URL_OBJECT_URL;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;

use beacon_sql_parser::{beacon_parser::BeaconParser, statement::BeaconStatement};
use crate::{
    plan_validator::{PlanFacts, PlanValidator, QueryLimits, TableFacts},
    query_cpu::QueryCpuMeter,
    query_executor::QueryExecutor,
    query_metrics_store::QueryMetricsStore,
    query_result::{ArrowOutputStream, QueryOutput, QueryResult},
};

/// Beacon's single execution layer.
///
/// The interface is deliberately narrow: run a query, with a [`PlanValidator`] that
/// permits or refuses the plan. The runtime knows no users and no roles. A host
/// application gives the validator of its caller, and the embedded database gives
/// [`AllowAll`](crate::plan_validator::AllowAll).
/// Most of what a consumer wants to know — comments, crawlers, the tables in a
/// schema — is reachable as SQL (`beacon.system.comments`, `SHOW CRAWLERS`, a scan), so
/// they ask for it in a query and map the resulting `RecordBatch`es into their
/// own types rather than calling a typed accessor per thing.
///
/// What is left here is what a caller *cannot* always ask for themselves: reads of the
/// metadata schemas, which a validator can refuse ([`Self::visible_tables`],
/// [`Self::show_functions`]), a table's schema straight from its provider
/// ([`Self::table_arrow_schema`]), and the runtime's own bookkeeping
/// ([`Self::get_query_metrics`]). Those answer in Arrow and in beacon's own
/// types; turning them into a wire format is the transport's job, not this
/// one's.
///
/// Every field is shared, so a clone is cheap. [`Self::run_query`] clones the
/// runtime into the task it spawns on the query executor.
#[derive(Clone)]
pub struct Runtime {
    /// The runtime queries run on. The server gives it a runtime of its own, so
    /// a long scan never holds an API worker. The embedded database gives it the
    /// one runtime it has.
    pub(crate) executor: QueryExecutor,
    pub(crate) session_ctx: Arc<SessionContext>,
    /// Where a completed query's metrics are written: the managed table that
    /// `beacon.system.query_metrics` exposes, which is how callers read them.
    pub(crate) query_metrics: Arc<QueryMetricsStore>,
    /// The `beacon.system` schema. The catalog holds the same provider.
    pub(crate) system_schema: Arc<crate::system_schema::SystemSchemaProvider>,

    /// Whether this runtime was opened read-only: any statement that does not produce a result set
    /// (writes, DDL/DML, and beacon's side-effecting extension statements) is refused.
    pub(crate) read_only: bool,

    /// The file-statistics engine, when enabled. The session config holds the
    /// same engine; dropping the runtime stops its refresh tasks.
    pub(crate) file_stats: Option<beacon_file_statistics::FileStatistics>,

    /// The installed function plugins. The session config holds the same registry.
    pub(crate) plugins: Arc<crate::plugins::PluginRegistry>,
    /// The plugins directory, the only store of the plugins. `None` disables plugins.
    pub(crate) plugins_dir: Option<PathBuf>,

    /// tmp directory for storing temporary files (e.g. for query output)
    pub(crate) tmp_dir: PathBuf,
}

impl Runtime {
    pub fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    /// Adds the read-only table `beacon.system.<name>` of the host application.
    ///
    /// The server adds its users and roles this way. Build the table with
    /// [`SystemTable`](crate::SystemTable).
    ///
    /// # Arguments
    ///
    /// * `name` - The table name in the `system` schema
    /// * `table` - The table provider
    ///
    /// # Errors
    ///
    /// Returns an error when the schema already holds a table with this name.
    pub fn register_system_table(
        &self,
        name: &str,
        table: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> anyhow::Result<()> {
        self.system_schema.add_table(name, table)
    }

    /// Whether the catalog holds `table`. The reserved `__beacon_*` tables count too, although
    /// no catalog list shows them.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog or the schema of `table` does not exist.
    pub fn table_exists(
        &self,
        table: impl Into<datafusion::sql::TableReference>,
    ) -> anyhow::Result<bool> {
        Ok(self.session_ctx.table_exist(table.into())?)
    }

    /// The facts of `table` that a [`PlanValidator`] checks, with each spelling of its name.
    pub fn table_facts(&self, table: impl Into<datafusion::sql::TableReference>) -> TableFacts {
        TableFacts::new(table.into(), &self.session_ctx)
    }

    /// Execute a client query (JSON or SQL) and return a metrics-tracked result.
    ///
    /// The single entry point for query execution: the JSON form is compiled by
    /// beacon's query compiler, the SQL form goes through beacon's SQL parser, and
    /// both are lowered to a `LogicalPlan`. `validator` then permits or refuses
    /// the plan. The plan then runs through the unified
    /// `create_physical_plan -> execute_stream` pipeline. Without an `output`
    /// format the result is streamed (metrics recorded as the client drains);
    /// with one, the result is written to a temporary file in that format and
    /// returned as a file download.
    ///
    /// The limits of the query come from [`PlanValidator::limits`]. The CPU
    /// budget covers planning and execution. A query that goes over it fails
    /// with a "CPU budget" error.
    ///
    /// # Arguments
    ///
    /// * `query` - The JSON or SQL query, with its output format and parameters
    /// * `validator` - The access check of the caller
    ///
    /// # Errors
    ///
    /// Returns an error when the query does not plan, when `validator` refuses
    /// the plan, when a read-only database gets a write, or when execution fails.
    #[tracing::instrument(skip(self, query, validator))]
    pub async fn run_query(
        &self,
        query: crate::query::Query,
        validator: Arc<dyn PlanValidator>,
    ) -> anyhow::Result<QueryResult> {
        let limits = validator.limits();
        let meter = QueryCpuMeter::new(limits.cpu_budget);
        // Planning reads metadata too, so the whole query moves to the executor.
        let runtime = self.clone();
        let metered = meter.meter_future({
            let meter = meter.clone();
            async move {
                runtime
                    .run_query_on_executor(query, validator.as_ref(), limits, meter)
                    .await
            }
        });
        self.executor.run(metered).await
    }

    /// [`Self::run_query`], on the query executor.
    async fn run_query_on_executor(
        &self,
        query: crate::query::Query,
        validator: &dyn PlanValidator,
        limits: QueryLimits,
        cpu_meter: Arc<QueryCpuMeter>,
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
        validator
            .validate(&PlanFacts::new(&plan, &self.session_ctx))
            .await?;
        // A read-only database refuses anything that would mutate it. `plan_produces_result_set`
        // is the read/write discriminator: reads (SELECT, VALUES, `SHOW …`) produce rows; every
        // write (DDL/DML/COPY/SET and beacon's side-effecting extension statements, e.g. `ATTACH`,
        // `CREATE SECRET`) reports an empty schema.
        if self.read_only && !crate::statement_plan::plan_produces_result_set(&plan) {
            anyhow::bail!(
                "this database was opened read-only; writes are not permitted on this connection"
            );
        }

        let metrics = MetricsTracker::new(query_json, query_id, validator.caller());
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
                self.run_query_to_file(plan, output, metrics, limits.output_row_limit, cpu_meter)
                    .await
            }
            None => {
                self.run_query_to_stream(plan, metrics, limits.output_row_limit, cpu_meter)
                    .await
            }
        }
    }

    /// Stream the plan's results, wrapping the stream so output rows/bytes are
    /// recorded in `metrics` as the client drains it.
    async fn run_query_to_stream(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
        metrics: Arc<MetricsTracker>,
        output_row_limit: Option<u64>,
        cpu_meter: Arc<QueryCpuMeter>,
    ) -> anyhow::Result<QueryResult> {
        let query_id = metrics.query_id;
        metrics.set_cpu_meter(cpu_meter.clone());
        metrics.set_logical_plan(&plan);
        // Record the optimized logical plan alongside the parsed one. `optimize`
        // errors for `Statement` plans (`SET ...`), which have nothing to
        // optimize — leave the column null in that case rather than failing.
        if let Ok(optimized) = self.session_ctx.state().optimize(&plan) {
            metrics.set_optimized_logical_plan(&optimized);
        }
        let (stream, physical_plan) = crate::statement_plan::execute_statement_plan_tracked(
            &self.session_ctx,
            plan,
            Some(&cpu_meter),
            output_row_limit,
        )
        .await?;
        // The physical plan's per-node metrics fill in as the stream drains, and the
        // stream consolidates when it ends — so registering the plan here is what
        // makes `node_metrics` non-empty in `beacon.system.query_metrics`.
        if let Some(physical_plan) = physical_plan {
            metrics.set_physical_plan(physical_plan);
        }
        // The caller drains the result on its own runtime. The bridge keeps the
        // scan, and the tasks DataFusion spawns on first poll, on the executor.
        let stream = self.executor.bridge_stream(stream);
        let output_stream = ArrowOutputStream::new(stream, metrics, self.query_metrics.clone());
        Ok(QueryResult {
            query_output: QueryOutput::Stream(output_stream),
            query_id,
        })
    }

    /// Write the plan's results to a temporary file in the requested `output`
    /// format (via a `COPY TO`) and return a file download. The file write is
    /// driven to completion here and its metrics recorded in `metrics`.
    async fn run_query_to_file(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
        output: crate::query::output::Output,
        metrics: Arc<MetricsTracker>,
        output_row_limit: Option<u64>,
        cpu_meter: Arc<QueryCpuMeter>,
    ) -> anyhow::Result<QueryResult> {
        let query_id = metrics.query_id;
        // `Output::parse` wraps the (already validated) plan in a `COPY TO` the
        // temp file; this COPY is beacon-generated, so it is not re-validated.
        //
        // `self.tmp_dir` MUST be the tmp store root, or `QueryOutputFile` cannot see the bytes.
        let (copy_plan, output_file) = output
            .parse(
                &self.session_ctx,
                &self.tmp_dir,
                &TMP_STORE_URL_OBJECT_URL,
                plan,
            )
            .await?;

        metrics.set_cpu_meter(cpu_meter.clone());
        metrics.set_logical_plan(&copy_plan);
        if let Ok(optimized) = self.session_ctx.state().optimize(&copy_plan) {
            metrics.set_optimized_logical_plan(&optimized);
        }
        let (mut stream, physical_plan) = crate::statement_plan::execute_statement_plan_tracked(
            &self.session_ctx,
            copy_plan,
            Some(&cpu_meter),
            output_row_limit,
        )
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

    /// A registered table's Arrow schema, taken from the table provider itself, when
    /// `validator` permits the read.
    ///
    /// Deliberately **not** `SELECT * FROM t LIMIT 0`: planning a scan forces the
    /// whole read path to resolve, and for an N-dimensional table whose variables
    /// cannot be broadcast onto a common shape that fails — so a zero-row query
    /// errors on a table whose schema is perfectly well defined. Asking the
    /// provider skips planning entirely, reads no data, and answers for every table
    /// a scan could not.
    ///
    /// The schema carries the table's comments (`COMMENT ON`) as `comment`
    /// metadata; see [`TableComments::apply`](crate::comments::TableComments::apply).
    /// To get them, Beacon reads the small `comments.json` sidecar of the table.
    ///
    /// A schema is metadata about data the caller may not read, so the same check
    /// applies as for the rows.
    ///
    /// # Arguments
    ///
    /// * `table` - The table reference
    /// * `validator` - The access check of the caller
    ///
    /// # Errors
    ///
    /// Returns an error when [`PlanValidator::check_table`] refuses the table or
    /// when the table does not resolve.
    pub async fn table_arrow_schema(
        &self,
        table: impl Into<datafusion::sql::TableReference>,
        validator: &dyn PlanValidator,
    ) -> anyhow::Result<arrow::datatypes::SchemaRef> {
        let table: datafusion::sql::TableReference = table.into();
        validator.check_table(&TableFacts::new(table.clone(), &self.session_ctx))?;

        let provider = self
            .session_ctx
            .table_provider(table.clone())
            .await
            .map_err(|e| anyhow::anyhow!("table '{table}' could not be resolved: {e}"))?;
        // Unreadable comments must not hide the schema; `COMMENT ON` reports the error.
        let comments = crate::comments::load_comments(&self.session_ctx, &table)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%table, %error, "ignored unreadable table comments");
                crate::comments::TableComments::default()
            });
        Ok(Arc::new(comments.apply(&provider.schema())))
    }

    /// The catalog and schema an unqualified table name resolves against.
    pub fn default_catalog_and_schema(&self) -> (String, String) {
        let options = self.session_ctx.copied_config().options().catalog.clone();
        (options.default_catalog, options.default_schema)
    }

    /// The `information_schema.tables` rows that `validator` permits, as Arrow
    /// batches — `table_catalog`, `table_schema`, `table_name`, `table_type`,
    /// ordered by the first three.
    ///
    /// This is how a transport lists the catalog for a caller that cannot read
    /// `information_schema`. The runtime reads it as the engine and drops every
    /// row that [`PlanValidator::check_table`] refuses. A listing therefore shows
    /// exactly what the caller could go on to read.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog read fails.
    pub async fn visible_tables(
        &self,
        validator: &dyn PlanValidator,
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
                    let reference = datafusion::sql::TableReference::partial(
                        schemas.value(row),
                        names.value(row),
                    );
                    let table = TableFacts::new(reference, &self.session_ctx);
                    Some(validator.check_table(&table).is_ok())
                })
                .collect();
            visible.push(arrow::compute::filter_record_batch(batch, &mask)?);
        }
        Ok(visible)
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
    /// which a validator can refuse.
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
    /// `EXPLAIN` is read-only — nothing is executed — so only a plain read is
    /// explained, for each caller. `validator` must also permit the tables that the
    /// plan reads.
    ///
    /// # Errors
    ///
    /// Returns an error when the query does not plan, when the statement is not
    /// a plain read, or when `validator` refuses the plan.
    pub async fn explain_query(
        &self,
        query: crate::query::Query,
        validator: Arc<dyn PlanValidator>,
    ) -> anyhow::Result<String> {
        let runtime = self.clone();
        self.executor
            .run(async move {
                runtime
                    .explain_query_on_executor(query, validator.as_ref())
                    .await
            })
            .await
    }

    /// [`Self::explain_query`], on the query executor.
    async fn explain_query_on_executor(
        &self,
        query: crate::query::Query,
        validator: &dyn PlanValidator,
    ) -> anyhow::Result<String> {
        // `output` (file format) is meaningless here: only the query body is planned.
        let plan = self.lower_query(query.inner).await?;
        let facts = PlanFacts::new(&plan, &self.session_ctx);
        facts.verify_unprivileged()?;
        validator.validate(&facts).await?;
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
    /// populate the metrics, so `validator` decides as it does for `run_query`.
    /// A permitted DDL or DML statement runs with its side effects. A read-only
    /// runtime therefore refuses anything that does not produce a result set,
    /// exactly as `run_query` does.
    ///
    /// # Errors
    ///
    /// Returns an error when the query does not plan, when `validator` refuses
    /// the plan, when a read-only database gets a write, or when execution fails.
    pub async fn explain_analyze_query(
        &self,
        query: crate::query::Query,
        validator: Arc<dyn PlanValidator>,
    ) -> anyhow::Result<String> {
        // The plan runs in full, so the CPU budget of `run_query` applies.
        let meter = QueryCpuMeter::new(validator.limits().cpu_budget);
        let runtime = self.clone();
        let metered = meter.meter_future({
            let meter = meter.clone();
            async move {
                runtime
                    .explain_analyze_query_on_executor(query, validator.as_ref(), meter)
                    .await
            }
        });
        self.executor.run(metered).await
    }

    /// [`Self::explain_analyze_query`], on the query executor.
    async fn explain_analyze_query_on_executor(
        &self,
        query: crate::query::Query,
        validator: &dyn PlanValidator,
        cpu_meter: Arc<QueryCpuMeter>,
    ) -> anyhow::Result<String> {
        let plan = self.lower_query(query.inner).await?;
        validator
            .validate(&PlanFacts::new(&plan, &self.session_ctx))
            .await?;
        if self.read_only && !crate::statement_plan::plan_produces_result_set(&plan) {
            anyhow::bail!(
                "this database was opened read-only; writes are not permitted on this connection"
            );
        }

        // Keep the `Arc` so per-node metrics can be read once the stream drains.
        // `execute_statement_plan` discards the plan, so create/execute are inlined.
        let physical_plan = self.session_ctx.state().create_physical_plan(&plan).await?;
        let physical_plan = crate::query_cpu::meter_plan(physical_plan, &cpu_meter);
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
                let settings = crate::settings::SqlSettings::from_session(&self.session_ctx);
                let options = crate::query::JsonQueryOptions {
                    default_table: settings.default_table,
                    enable_pushdown_projection: settings.enable_pushdown_projection,
                };
                crate::query::compile_json_query(body, self.session_ctx.as_ref(), &options).await
            }
        }
    }

    /// Lower a SQL statement (SELECT, DDL/DML, or a beacon custom statement) to a
    /// `LogicalPlan`. The result is validated in `run_query` before execution.
    async fn lower_sql(&self, sql: &str) -> anyhow::Result<datafusion::logical_expr::LogicalPlan> {
        match Self::parse_beacon_statement(sql)? {
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
            BeaconStatement::CreateIndex(statement) => {
                Ok(crate::statement_plan::create_index_plan(statement))
            }
            BeaconStatement::DropIndex(statement) => {
                Ok(crate::statement_plan::drop_index_plan(statement))
            }
            BeaconStatement::ShowIndexes(statement) => {
                Ok(crate::statement_plan::show_indexes_plan(statement))
            }
            BeaconStatement::CompactTable(statement) => {
                Ok(crate::statement_plan::compact_table_plan(statement))
            }
            BeaconStatement::Attach(statement) => {
                crate::statement_plan::attach_plan(statement)
            }
            BeaconStatement::Detach(statement) => {
                Ok(crate::statement_plan::detach_plan(statement))
            }
            BeaconStatement::Install(statement) => Ok(crate::statement_plan::install_plan(
                statement,
                self.plugins_dir.clone(),
            )),
            BeaconStatement::Uninstall(statement) => Ok(crate::statement_plan::uninstall_plan(
                statement,
                self.plugins_dir.clone(),
            )),
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
            BeaconStatement::InsertByName(statement) => {
                crate::statement_plan::insert_by_name_plan(statement, &self.session_ctx.state())
                    .await
            }
            BeaconStatement::DFStatement(statement) => {
                beacon_sql_parser::expression_depth::check_expression_depth(&statement)?;
                crate::statement_plan::lower_df_statement(&self.session_ctx, *statement).await
            }
        }
    }

    fn parse_beacon_statement(sql: &str) -> anyhow::Result<BeaconStatement> {
        let mut parser = BeaconParser::new(sql)?;
        parser.parse_single_statement().map_err(Into::into)
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
    /// by the restart test). Tests run as `allow_all()` or as `unprivileged()`.
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
                crate::plan_validator::allow_all(),
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
                crate::plan_validator::allow_all(),
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
                crate::plan_validator::allow_all(),
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
                crate::plan_validator::allow_all(),
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
                crate::plan_validator::allow_all(),
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

    /// The NetCDF sink uploads its file through the tmp object store. This asserts
    /// the file lands under the configured tmp store root, not the OS temp dir,
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
                crate::plan_validator::allow_all(),
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
                crate::plan_validator::unprivileged(),
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
                crate::plan_validator::unprivileged(),
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
                crate::plan_validator::unprivileged(),
            )
            .await
            .err()
            .expect("non-super CREATE MATERIALIZED VIEW should be rejected");
        assert!(
            err.to_string().contains("super-user"),
            "unexpected error: {err}"
        );
    }

    /// A validator that refuses each statement and permits one output row.
    struct RefuseAll;

    #[async_trait::async_trait]
    impl crate::plan_validator::PlanValidator for RefuseAll {
        fn caller(&self) -> &str {
            "refused"
        }

        fn limits(&self) -> crate::plan_validator::QueryLimits {
            crate::plan_validator::QueryLimits::default()
        }

        async fn validate(
            &self,
            _facts: &crate::plan_validator::PlanFacts<'_>,
        ) -> anyhow::Result<()> {
            anyhow::bail!("refused by the test validator")
        }

        fn check_table(&self, _table: &crate::plan_validator::TableFacts) -> anyhow::Result<()> {
            anyhow::bail!("refused by the test validator")
        }

        fn can_read_path(&self, _path: &str) -> bool {
            false
        }
    }

    /// The engine runs a plan only after the validator permits it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_validator_refusal_stops_the_query() {
        let rt: TestRt = test_runtime().await;
        let runtime = &rt.runtime;

        let error = runtime
            .run_query(
                crate::query::Query::sql("CREATE TABLE refused (a BIGINT)".to_string()),
                std::sync::Arc::new(RefuseAll),
            )
            .await
            .err()
            .expect("the validator refuses the statement");

        assert!(error.to_string().contains("refused by the test validator"));
        let tables = runtime
            .visible_tables(&crate::plan_validator::AllowAll)
            .await
            .expect("the catalog should list");
        let rows: usize = tables.iter().map(|batch| batch.num_rows()).sum();
        let refused = runtime.visible_tables(&RefuseAll).await.unwrap();
        assert!(rows > 0);
        assert_eq!(refused.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);
        assert!(runtime
            .table_arrow_schema("refused", &crate::plan_validator::AllowAll)
            .await
            .is_err());
    }

    /// `EXPLAIN` and `EXPLAIN ANALYZE` plan a statement, so they ask the validator too.
    #[tokio::test(flavor = "multi_thread")]
    async fn explain_asks_the_validator() {
        let rt: TestRt = test_runtime().await;
        let runtime = &rt.runtime;
        run_sql(runtime, "CREATE TABLE explained (a BIGINT)").await;
        let query = || crate::query::Query::sql("SELECT * FROM explained".to_string());
        let refuse = || std::sync::Arc::new(RefuseAll);

        let explained = runtime
            .explain_query(query(), crate::plan_validator::allow_all())
            .await;
        let analyzed = runtime
            .explain_analyze_query(query(), crate::plan_validator::allow_all())
            .await;
        let explain_refused = runtime.explain_query(query(), refuse()).await;
        let analyze_refused = runtime.explain_analyze_query(query(), refuse()).await;

        assert!(explained.is_ok(), "{explained:?}");
        assert!(analyzed.is_ok(), "{analyzed:?}");
        for refused in [explain_refused, analyze_refused] {
            let error = refused.expect_err("the validator refuses the plan");
            assert!(error.to_string().contains("refused by the test validator"));
        }
    }

    /// `EXPLAIN` runs nothing, so it explains only a plain read, also with full access.
    #[tokio::test(flavor = "multi_thread")]
    async fn explain_refuses_a_write_for_each_validator() {
        let rt: TestRt = test_runtime().await;

        let explained = rt
            .runtime
            .explain_query(
                crate::query::Query::sql("CREATE TABLE never_made (a BIGINT)".to_string()),
                crate::plan_validator::allow_all(),
            )
            .await;

        assert!(explained.is_err());
    }

    /// A validator sees the plan after the engine binds the parameters.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_validator_gets_the_tables_of_the_plan() {
        /// Records the table names of each plan that it permits.
        struct Recorder(std::sync::Mutex<Vec<String>>);

        #[async_trait::async_trait]
        impl crate::plan_validator::PlanValidator for Recorder {
            fn caller(&self) -> &str {
                "recorder"
            }

            fn limits(&self) -> crate::plan_validator::QueryLimits {
                crate::plan_validator::QueryLimits::default()
            }

            async fn validate(
                &self,
                facts: &crate::plan_validator::PlanFacts<'_>,
            ) -> anyhow::Result<()> {
                let mut seen = self.0.lock().expect("the lock is not poisoned");
                for read in facts.reads() {
                    if let crate::plan_validator::PlanRead::Table(table) = read {
                        seen.push(table.name().to_string());
                    }
                }
                Ok(())
            }

            fn check_table(
                &self,
                _table: &crate::plan_validator::TableFacts,
            ) -> anyhow::Result<()> {
                Ok(())
            }

            fn can_read_path(&self, _path: &str) -> bool {
                true
            }
        }

        let rt: TestRt = test_runtime().await;
        let runtime = &rt.runtime;
        run_sql(runtime, "CREATE TABLE left_t (a BIGINT)").await;
        run_sql(runtime, "CREATE TABLE right_t (a BIGINT)").await;
        run_sql(runtime, "CREATE VIEW both_v AS SELECT * FROM left_t JOIN right_t USING (a)").await;
        let recorder = std::sync::Arc::new(Recorder(std::sync::Mutex::new(Vec::new())));

        runtime
            .run_query(
                crate::query::Query::sql("SELECT * FROM both_v".to_string()),
                recorder.clone(),
            )
            .await
            .expect("the recorder permits the query");

        let mut seen = recorder.0.lock().expect("the lock is not poisoned").clone();
        seen.sort();
        seen.dedup();
        assert!(
            seen.iter().any(|name| name.contains("both_v"))
                || (seen.iter().any(|name| name.contains("left_t"))
                    && seen.iter().any(|name| name.contains("right_t"))),
            "the validator must see the view or its tables: {seen:?}"
        );
    }

    /// A host application adds a table to `beacon.system`, and a name is used once.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_host_table_joins_the_system_schema() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let rt: TestRt = test_runtime().await;
        let runtime = &rt.runtime;
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let table = || {
            // Each scan gets its own handle to the schema.
            let row_schema = schema.clone();
            let snapshot: crate::Snapshot = std::sync::Arc::new(move || {
                let row_schema = row_schema.clone();
                Box::pin(async move {
                    let values = std::sync::Arc::new(Int64Array::from(vec![1, 2, 3]));
                    Ok(arrow::record_batch::RecordBatch::try_new(row_schema, vec![values])?)
                })
            });
            std::sync::Arc::new(crate::SystemTable::new(schema.clone(), snapshot))
        };

        let added = runtime.register_system_table("host_rows", table());
        let again = runtime.register_system_table("host_rows", table());
        let reserved = runtime.register_system_table("query_metrics", table());
        let rows: usize = runtime
            .run_query(
                crate::query::Query::sql("SELECT n FROM beacon.system.host_rows".to_string()),
                crate::plan_validator::allow_all(),
            )
            .await
            .expect("the host table reads")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("the stream drains")
            .iter()
            .map(|batch| batch.num_rows())
            .sum();

        assert!(added.is_ok());
        assert!(again.is_err(), "a name is used once");
        assert!(reserved.is_err(), "`query_metrics` is a name of the engine");
        assert_eq!(rows, 3);
    }

    /// The empty result of a host statement has no columns and no rows.
    #[tokio::test]
    async fn an_empty_result_has_no_columns_and_no_rows() {
        let stream = crate::query_result::QueryResult::empty()
            .into_record_stream()
            .expect("an empty result is a stream");

        let columns = stream.schema().fields().len();
        let batches = stream.try_collect::<Vec<_>>().await.expect("the stream ends");

        assert_eq!(columns, 0);
        assert!(batches.is_empty());
    }

    /// `AllowAll` needs no identity: it runs DDL, DML and a read.
    #[tokio::test(flavor = "multi_thread")]
    async fn allow_all_runs_privileged_statements() {
        let rt: TestRt = test_runtime().await;
        let runtime = &rt.runtime;
        let run = |sql: &str| {
            runtime.run_query(
                crate::query::Query::sql(sql.to_string()),
                std::sync::Arc::new(crate::plan_validator::AllowAll),
            )
        };

        for sql in [
            "CREATE TABLE open_t (a BIGINT)",
            "INSERT INTO open_t VALUES (1), (2)",
        ] {
            run(sql)
                .await
                .expect("AllowAll permits the statement")
                .into_record_stream()
                .expect("streamed result")
                .try_collect::<Vec<_>>()
                .await
                .expect("the statement should drain");
        }
        let rows: usize = run("SELECT * FROM open_t")
            .await
            .expect("AllowAll permits the read")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("the read should drain")
            .iter()
            .map(|batch| batch.num_rows())
            .sum();

        assert_eq!(rows, 2);
        assert!(runtime
            .table_arrow_schema("open_t", &crate::plan_validator::AllowAll)
            .await
            .is_ok());
    }

    /// A validator that permits each statement with a limit of one output row.
    struct OneRow;

    #[async_trait::async_trait]
    impl crate::plan_validator::PlanValidator for OneRow {
        fn caller(&self) -> &str {
            "one-row"
        }

        fn limits(&self) -> crate::plan_validator::QueryLimits {
            crate::plan_validator::QueryLimits {
                output_row_limit: Some(1),
                ..Default::default()
            }
        }

        async fn validate(
            &self,
            _facts: &crate::plan_validator::PlanFacts<'_>,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn check_table(&self, _table: &crate::plan_validator::TableFacts) -> anyhow::Result<()> {
            Ok(())
        }

        fn can_read_path(&self, _path: &str) -> bool {
            true
        }
    }

    /// The limits of the validator apply to the query.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_row_limit_of_the_validator_applies() {
        let rt: TestRt = test_runtime().await;
        let runtime = &rt.runtime;

        let result = runtime
            .run_query(
                crate::query::Query::sql("SELECT * FROM (VALUES (1), (2), (3))".to_string()),
                std::sync::Arc::new(OneRow),
            )
            .await
            .expect("the validator permits the read")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await;

        assert!(result.is_err(), "three rows pass a limit of one row");
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
                        crate::plan_validator::allow_all(),
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
                crate::plan_validator::allow_all(),
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
                crate::plan_validator::allow_all(),
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
mod show_create_tests {
    use super::test_support::{build_runtime, test_runtime};
    use super::Runtime;
    use arrow::array::{Array, AsArray};
    use futures::TryStreamExt;

    async fn try_collect(
        runtime: &Runtime,
        sql: &str,
        validator: std::sync::Arc<dyn crate::plan_validator::PlanValidator>,
    ) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        Ok(runtime
            .run_query(crate::query::Query::sql(sql.to_string()), validator)
            .await?
            .into_record_stream()?
            .try_collect::<Vec<_>>()
            .await?)
    }

    async fn run_sql(runtime: &Runtime, sql: &str) {
        try_collect(runtime, sql, crate::plan_validator::allow_all())
            .await
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"));
    }

    /// The `definition` column of `SHOW CREATE TABLE <table>`.
    async fn definition(runtime: &Runtime, table: &str) -> Option<String> {
        let sql = format!("SHOW CREATE TABLE {table}");
        let batches = try_collect(runtime, &sql, crate::plan_validator::allow_all())
            .await
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"));
        assert_eq!(batches.len(), 1, "one batch for {table}");
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1, "one row for {table}");
        assert_eq!(batch.column(2).as_string::<i32>().value(0), table);
        let column = batch.column(3).as_string::<i32>();
        (!column.is_null(0)).then(|| column.value(0).to_string())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn returns_the_statement_of_each_table_kind() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;
        std::fs::write(rt.root.path().join("datasets/e.csv"), "a,b\n1,x\n").unwrap();

        run_sql(runtime, "CREATE TABLE managed (a BIGINT)").await;
        run_sql(runtime, "INSERT INTO managed VALUES (1), (2)").await;
        run_sql(runtime, "CREATE TABLE copied AS SELECT a FROM managed WHERE a > 1").await;
        run_sql(runtime, "CREATE VIEW v AS SELECT a FROM managed").await;
        run_sql(runtime, "CREATE MATERIALIZED VIEW mv AS SELECT a FROM managed").await;
        run_sql(runtime, "CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION 'e.csv'").await;

        assert_eq!(
            definition(runtime, "managed").await.as_deref(),
            Some("CREATE TABLE managed (a BIGINT)")
        );
        assert_eq!(
            definition(runtime, "copied").await.as_deref(),
            Some("CREATE TABLE copied AS SELECT a FROM managed WHERE a > 1")
        );
        assert_eq!(
            definition(runtime, "v").await.as_deref(),
            Some("CREATE VIEW v AS SELECT a FROM managed")
        );
        assert_eq!(
            definition(runtime, "mv").await.as_deref(),
            Some("CREATE MATERIALIZED VIEW \"mv\" AS SELECT a FROM managed")
        );
        assert_eq!(
            definition(runtime, "ext").await.as_deref(),
            Some("CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION 'e.csv'")
        );

        // The CTAS input still ran: it copied the one matching row.
        let rows: usize = try_collect(runtime, "SELECT * FROM copied", crate::plan_validator::allow_all())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        assert_eq!(rows, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn names_a_missing_table_and_refuses_other_users() {
        let rt = test_runtime().await;
        let runtime = &rt.runtime;
        run_sql(runtime, "CREATE TABLE secret_shape (a BIGINT)").await;

        let err = try_collect(runtime, "SHOW CREATE TABLE missing", crate::plan_validator::allow_all())
            .await
            .expect_err("a missing table is an error");
        assert!(err.to_string().contains("missing"), "unexpected error: {err}");

        let err = try_collect(
            runtime,
            "SHOW CREATE TABLE secret_shape",
            crate::plan_validator::unprivileged(),
        )
        .await
        .expect_err("only the super-user sees definitions");
        assert!(err.to_string().contains("super-user"), "unexpected error: {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn statements_survive_a_restart() {
        let root = tempfile::TempDir::new().expect("temp root");
        let db_path = root.path().join("beacon.db");

        let runtime = build_runtime(root.path(), Some(db_path.clone())).await;
        run_sql(&runtime, "CREATE TABLE kept (a BIGINT)").await;
        run_sql(&runtime, "CREATE VIEW kept_view AS SELECT a FROM kept").await;
        drop(runtime);

        let restarted = build_runtime(root.path(), Some(db_path)).await;
        assert_eq!(
            definition(&restarted, "kept").await.as_deref(),
            Some("CREATE TABLE kept (a BIGINT)")
        );
        assert_eq!(
            definition(&restarted, "kept_view").await.as_deref(),
            Some("CREATE VIEW kept_view AS SELECT a FROM kept")
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
                crate::plan_validator::allow_all(),
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
    /// The file-statistics engine, when the subsystem is enabled.
    pub fn file_stats(&self) -> Option<&beacon_file_statistics::FileStatistics> {
        self.file_stats.as_ref()
    }

    /// Checks a plugin archive, unpacks it into `<plugins_dir>/<name>/` and registers
    /// its functions. `source` is recorded as given.
    ///
    /// It skips the `SHA256SUMS` and super-user checks. It exists for tests and
    /// embedders.
    #[doc(hidden)]
    pub async fn install_plugin_archive(
        &self,
        name: &str,
        archive: bytes::Bytes,
        source: &str,
    ) -> anyhow::Result<()> {
        let plugins_dir = self.plugins_dir.as_deref().ok_or_else(|| {
            anyhow::anyhow!("plugins are disabled: this runtime has no plugins directory")
        })?;
        crate::plugins::loader::install_archive(
            &self.session_ctx,
            &self.plugins,
            plugins_dir,
            name,
            archive,
            source,
        )
        .await
    }
}

