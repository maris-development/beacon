//! High-level Beacon runtime shared by the API transports.

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use crate::metrics::{ConsolidatedMetrics, MetricsTracker};
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
    api::{
        CrawlReportView, CrawlerView, CreateCrawlerRequest, CreateExternalTableRequest,
        DatasetInfo, FunctionInfo, QueryMetricsView, QueryRequest, SchemaView, TableConfigView,
    },
    parser::{beacon_parser::BeaconParser, statement::BeaconStatement},
    query_job::{
        BufferBudget, CancelOutcome, FileResultMeta, JobKind, PollOutcome, QueryJob,
        QueryJobSnapshot, QueryJobState, SpillableBatchBuffer,
    },
    query_result::{ArrowOutputStream, QueryOutput, QueryOutputFile, QueryResult},
    sys::{self, SystemInfo},
};

/// Maximum number of batches a single stream poll drains in one response, to
/// bound the response size regardless of how far the producer has run ahead.
const MAX_BATCHES_PER_POLL: usize = 32;

/// Beacon's single execution layer: startup, catalog access, queries, SQL, and files.
pub struct Runtime {
    session_ctx: Arc<SessionContext>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    listing_table_factory: Arc<ListingTableFactoryExt>,
    crawler_manager: Arc<beacon_data_lake::crawler::CrawlerManager>,
    query_metrics: Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>,
    /// Registry of async query jobs (submit/poll), keyed by query id.
    query_jobs: Arc<Mutex<HashMap<uuid::Uuid, QueryJob>>>,
    /// Process-wide in-memory budget shared by all streamable job buffers.
    query_job_budget: Arc<BufferBudget>,
    /// Caps how many query jobs execute in the background concurrently.
    query_job_semaphore: Arc<tokio::sync::Semaphore>,
    /// The configuration this runtime was built with. Owned, not process-global.
    config: Arc<beacon_config::Config>,
}

impl Runtime {
    /// Boots the Beacon execution environment with the given configuration.
    ///
    /// The config is owned by the runtime (not read from a process-global) and is
    /// threaded into every component below — including, via a `SessionConfig`
    /// extension, the deep session-scoped code (query planning, table refresh,
    /// metadata UDFs) that cannot otherwise reach it.
    pub async fn new(config: Arc<beacon_config::Config>) -> anyhow::Result<Self> {
        let memory_pool = Arc::new(FairSpillPool::new(
            config.runtime.vm_memory_size * 1024 * 1024,
        ));

        let object_stores = beacon_object_storage::ObjectStores::new(&config.storage).await?;

        // The crawler manager is built after the file formats are registered, so
        // register an empty handle now and fill it below — the same late-fill
        // pattern as the query planner's session cell.
        let crawler_handle = beacon_data_lake::crawler::new_crawler_manager_handle();
        let session_ctx = Self::init_ctx(
            memory_pool,
            object_stores.datasets.clone(),
            config.clone(),
            crawler_handle.clone(),
        )?;
        beacon_data_lake::register_object_stores(&session_ctx, &object_stores)?;

        let file_formats = beacon_data_lake::file_formats(
            session_ctx.clone(),
            object_stores.datasets.clone(),
            &config,
        )?;

        let mut table_functions = vec![];
        table_functions.extend(beacon_functions::file_formats::register_table_functions(
            tokio::runtime::Handle::current(),
            session_ctx.clone(),
            DATASETS_OBJECT_STORE_URL.clone(),
            object_stores.datasets.clone(),
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
        beacon_iceberg::catalog::init_datasets_warehouse(
            object_stores.datasets.clone(),
            &config.storage,
        )
        .await?;

        geodatafusion::register(&session_ctx);

        for udf in beacon_functions::geo::geo_udfs(config.runtime.st_within_point_cache_size) {
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

        // Build the crawler manager once the data lake and tables exist, then publish
        // it through the handle so `CREATE/RUN/DROP CRAWLER` actions can reach it.
        let events_available = config.storage.enable_fs_events || config.storage.enable_s3_events;
        let crawler_manager = beacon_data_lake::crawler::CrawlerManager::new(
            session_ctx.clone(),
            file_formats.clone(),
            object_stores.datasets.clone(),
            config.crawler.clone(),
            events_available,
        );
        crawler_manager.init().await?;
        let _ = crawler_handle.set(crawler_manager.clone());

        let job_cfg = &config.runtime.query_jobs;
        let query_jobs = Arc::new(Mutex::new(HashMap::new()));
        let query_job_budget = BufferBudget::new(job_cfg.buffer_memory_bytes);
        let query_job_semaphore = Arc::new(tokio::sync::Semaphore::new(job_cfg.max_concurrent.max(1)));
        spawn_query_job_sweeper(
            Arc::downgrade(&query_jobs),
            Duration::from_secs(job_cfg.ttl_secs),
            Duration::from_secs(job_cfg.idle_secs),
        );

        Ok(Self {
            session_ctx,
            file_formats,
            listing_table_factory,
            crawler_manager,
            query_metrics: Arc::new(Mutex::new(HashMap::new())),
            query_jobs,
            query_job_budget,
            query_job_semaphore,
            config,
        })
    }

    fn init_ctx(
        memory_pool: Arc<FairSpillPool>,
        datasets_store: Arc<beacon_object_storage::DatasetsStore>,
        app_config: Arc<beacon_config::Config>,
        crawler_handle: beacon_data_lake::crawler::CrawlerManagerHandle,
    ) -> anyhow::Result<Arc<SessionContext>> {
        let mut config = SessionConfig::new()
            .with_batch_size(app_config.runtime.batch_size)
            .with_coalesce_batches(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema("beacon", "public")
            .with_collect_statistics(true)
            // Make the runtime's datasets store and config reachable from deep,
            // session-scoped code (query planning, table refresh, metadata UDFs)
            // without a process-global accessor.
            .with_extension(datasets_store)
            .with_extension(app_config)
            // The (late-filled) crawler manager handle, so DDL crawler actions can
            // reach it from session-scoped execution.
            .with_extension(crawler_handle);

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
            // Enable `datafusion-federation`: this is the default DataFusion
            // optimizer rule set with the `FederationOptimizerRule` inserted, so
            // sub-plans rooted at remote tables get pushed down. The matching
            // `FederatedPlanner` lives in `BeaconQueryPlanner`'s extension planners.
            .with_optimizer_rules(datafusion_federation::default_optimizer_rules())
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
        let (plan, query_id, query_json, output) = self.prepare_plan(query, is_super_user).await?;

        match output {
            Some(output) => {
                self.run_query_to_file(plan, output, query_id, query_json)
                    .await
            }
            None => self.run_query_to_stream(plan, query_id, query_json).await,
        }
    }

    /// Lower, validate, and assign an id to a query without executing it. Shared
    /// by the synchronous `run_query` and the async `submit_query_job` paths, so
    /// parse/validation errors surface to the caller (HTTP 400) before any work
    /// is scheduled.
    async fn prepare_plan(
        &self,
        query: crate::query::Query,
        is_super_user: bool,
    ) -> anyhow::Result<(
        datafusion::logical_expr::LogicalPlan,
        uuid::Uuid,
        serde_json::Value,
        Option<crate::query::output::Output>,
    )> {
        let query_id = uuid::Uuid::new_v4();
        let query_json = serde_json::to_value(&query)?;
        let crate::query::Query { inner, output } = query;

        let plan = self.lower_query(inner).await?;
        crate::statement_plan::validate_query_plan(&plan, is_super_user)?;

        Ok((plan, query_id, query_json, output))
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
            .parse(
                self.session_ctx.as_ref(),
                &self.config.storage.tmp_dir,
                plan,
            )
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

    /// Submit a query for asynchronous execution and return its id plus delivery
    /// kind. Planning and validation happen synchronously (so bad queries error
    /// here, surfaced as HTTP 400); execution runs on a background task whose
    /// results are retrieved via [`Self::poll_query_job_stream`] (streamable) or
    /// [`Self::query_job_snapshot`] + the file result download (file).
    ///
    /// Classification: no `output` or `output.format = Ipc` → [`JobKind::Streamable`];
    /// any other output format → [`JobKind::File`].
    pub async fn submit_query_job(
        self: &Arc<Self>,
        query: crate::query::Query,
        is_super_user: bool,
    ) -> anyhow::Result<(uuid::Uuid, JobKind)> {
        use crate::query::output::OutputFormat;

        let (plan, query_id, query_json, output) =
            self.prepare_plan(query, is_super_user).await?;

        let is_file = matches!(&output, Some(o) if !matches!(o.format, OutputFormat::Ipc));

        if is_file {
            let output = output.expect("file job classified from Some(output)");
            self.query_jobs
                .lock()
                .insert(query_id, QueryJob::new_running(JobKind::File, None));

            let this = self.clone();
            let handle = tokio::spawn(async move {
                let _permit = this.query_job_semaphore.clone().acquire_owned().await;
                let result = this
                    .run_query_to_file(plan, output, query_id, query_json)
                    .await;
                let mut jobs = this.query_jobs.lock();
                if let Some(job) = jobs.get_mut(&query_id) {
                    if matches!(job.state, QueryJobState::Running) {
                        match result {
                            Ok(QueryResult {
                                query_output: QueryOutput::File(file),
                                ..
                            }) => {
                                job.output = Some(file);
                                job.state = QueryJobState::Succeeded;
                            }
                            Ok(_) => {
                                job.state = QueryJobState::Failed {
                                    error: "file job did not produce a file output".to_string(),
                                };
                            }
                            Err(error) => {
                                job.state = QueryJobState::Failed {
                                    error: error.to_string(),
                                };
                            }
                        }
                        job.finished_at = Some(Instant::now());
                        job.handle = None;
                    }
                }
            });
            self.attach_job_handle(query_id, handle);
            return Ok((query_id, JobKind::File));
        }

        // Streamable: build the physical stream now (so plan errors surface at
        // submit), then spawn a producer that drains it into the spill buffer.
        let stream =
            crate::statement_plan::execute_statement_plan(&self.session_ctx, plan).await?;
        let schema = stream.schema();
        let buffer = SpillableBatchBuffer::new(
            schema,
            self.query_job_budget.clone(),
            self.config.storage.tmp_dir.clone(),
            self.config.runtime.query_jobs.max_spill_bytes,
        );
        self.query_jobs.lock().insert(
            query_id,
            QueryJob::new_running(JobKind::Streamable, Some(buffer.clone())),
        );

        let this = self.clone();
        let metrics = MetricsTracker::new(query_json, query_id);
        let handle = tokio::spawn(async move {
            let _permit = this.query_job_semaphore.clone().acquire_owned().await;
            let outcome = produce_stream_into_buffer(stream, &buffer, &metrics).await;
            buffer.finish(outcome.clone());
            if outcome.is_ok() {
                this.query_metrics
                    .lock()
                    .insert(query_id, metrics.get_consolidated_metrics());
            }
            let mut jobs = this.query_jobs.lock();
            if let Some(job) = jobs.get_mut(&query_id) {
                if matches!(job.state, QueryJobState::Running) {
                    job.state = match outcome {
                        Ok(()) => QueryJobState::Succeeded,
                        Err(error) => QueryJobState::Failed { error },
                    };
                    job.finished_at = Some(Instant::now());
                    job.handle = None;
                }
            }
        });
        self.attach_job_handle(query_id, handle);
        Ok((query_id, JobKind::Streamable))
    }

    /// Store a job's background task handle, unless the task already finished (or
    /// the job was cancelled) before we got here — in which case the handle is
    /// simply dropped.
    fn attach_job_handle(&self, query_id: uuid::Uuid, handle: tokio::task::JoinHandle<()>) {
        let mut jobs = self.query_jobs.lock();
        match jobs.get_mut(&query_id) {
            Some(job) if matches!(job.state, QueryJobState::Running) => {
                job.handle = Some(handle);
            }
            _ => {}
        }
    }

    /// Read-only snapshot of a job for the status/result handlers.
    pub fn query_job_snapshot(&self, query_id: uuid::Uuid) -> Option<QueryJobSnapshot> {
        let jobs = self.query_jobs.lock();
        let job = jobs.get(&query_id)?;
        let file = job.output.as_ref().map(|output| FileResultMeta {
            path: output.path().to_path_buf(),
            content_type: output.content_type(),
            file_ext: output.extension(),
        });
        Some(QueryJobSnapshot {
            kind: job.kind,
            state: job.state.clone(),
            file,
        })
    }

    /// Long-poll a streamable job for its next batches, waiting up to `wait`.
    pub async fn poll_query_job_stream(
        &self,
        query_id: uuid::Uuid,
        wait: Duration,
    ) -> PollOutcome {
        let buffer = {
            let mut jobs = self.query_jobs.lock();
            let Some(job) = jobs.get_mut(&query_id) else {
                return PollOutcome::NotFound;
            };
            if matches!(job.kind, JobKind::File) {
                return PollOutcome::NotStreamable;
            }
            if matches!(job.state, QueryJobState::Cancelled) {
                return PollOutcome::Cancelled;
            }
            job.last_poll_at = Instant::now();
            job.buffer.clone()
        };
        let Some(buffer) = buffer else {
            return PollOutcome::NotFound;
        };

        use crate::query_job::DrainOutcome;
        match buffer.drain(MAX_BATCHES_PER_POLL, wait).await {
            DrainOutcome::Batches(batches) => PollOutcome::Batches {
                schema: buffer.schema(),
                batches,
            },
            DrainOutcome::Completed => PollOutcome::Completed,
            DrainOutcome::Failed(error) => PollOutcome::Failed(error),
            DrainOutcome::Pending => PollOutcome::Pending,
        }
    }

    /// Cancel a running job, aborting its background task.
    pub fn cancel_query_job(&self, query_id: uuid::Uuid) -> CancelOutcome {
        let mut jobs = self.query_jobs.lock();
        let Some(job) = jobs.get_mut(&query_id) else {
            return CancelOutcome::NotFound;
        };
        if job.is_terminal() {
            return CancelOutcome::AlreadyFinished;
        }
        if let Some(handle) = job.handle.take() {
            handle.abort();
        }
        job.state = QueryJobState::Cancelled;
        job.finished_at = Some(Instant::now());
        CancelOutcome::Cancelled
    }

    /// The default stream-poll wait, from config.
    pub fn query_job_poll_wait(&self) -> Duration {
        Duration::from_millis(self.config.runtime.query_jobs.poll_wait_ms)
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
            BeaconStatement::DFStatement(statement) => {
                crate::statement_plan::lower_df_statement(&self.session_ctx, *statement).await
            }
        }
    }

    pub fn system_info(&self) -> SystemInfo {
        sys::SystemInfo::new(self.config.runtime.enable_sys_info)
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
        is_super_user: bool,
    ) -> anyhow::Result<String> {
        // `output` (file format) is meaningless for EXPLAIN ANALYZE — only the
        // query body is planned and executed for its metrics.
        let plan = self.lower_query(query.inner).await?;
        crate::statement_plan::validate_query_plan(&plan, is_super_user)?;

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

    /// The configuration this runtime was built with.
    pub fn config(&self) -> &Arc<beacon_config::Config> {
        &self.config
    }

    pub fn default_table(&self) -> String {
        self.config.sql.default_table.clone()
    }

    pub async fn list_table_config(&self, table_name: String) -> Option<TableConfigView> {
        let provider = self
            .session_ctx
            .table_provider(table_name.as_str())
            .await
            .ok()?;
        let config =
            beacon_data_lake::definition_from_provider(&table_name, provider.as_ref()).ok()?;
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
            .table(self.config.sql.default_table.as_str())
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
        Ok(beacon_data_lake::list_datasets(
            &self.session_ctx,
            &self.file_formats,
            offset,
            limit,
            pattern,
        )
        .await?)
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

    /// Define (or replace) a crawler. Mirrors `CREATE CRAWLER` but takes a
    /// structured request; delegates to the crawler manager, which persists the
    /// definition and (re)starts its scheduled/event triggers.
    pub async fn create_crawler(&self, req: CreateCrawlerRequest) -> anyhow::Result<()> {
        self.crawler_manager.create(req.into()).await
    }

    /// List all defined crawlers. Mirrors `SHOW CRAWLERS`.
    pub fn list_crawlers(&self) -> Vec<CrawlerView> {
        self.crawler_manager
            .list()
            .into_iter()
            .map(CrawlerView::from)
            .collect()
    }

    /// Return a single crawler definition by name, or `None` if it is not defined.
    pub fn get_crawler(&self, name: &str) -> Option<CrawlerView> {
        self.crawler_manager
            .list()
            .into_iter()
            .find(|crawler| crawler.name == name)
            .map(CrawlerView::from)
    }

    /// Run a crawler once on demand and return its report. Mirrors `RUN CRAWLER`.
    pub async fn run_crawler(&self, name: &str) -> anyhow::Result<CrawlReportView> {
        Ok(self.crawler_manager.run(name).await?.into())
    }

    /// Remove a crawler definition and stop its triggers (crawled tables are left
    /// in place). Mirrors `DROP CRAWLER`.
    pub async fn drop_crawler(&self, name: &str) -> anyhow::Result<()> {
        self.crawler_manager.drop_crawler(name).await
    }

    /// Create an external table from structured fields. Assembles the equivalent
    /// `CREATE EXTERNAL TABLE` statement and runs it through the same super-user DDL
    /// path as SQL, so all `STORED AS` variants (listing/Delta/Remote) and catalog
    /// persistence are reused. The statement produces an empty result stream that is
    /// drained here to drive the registration to completion.
    pub async fn create_external_table(
        &self,
        req: CreateExternalTableRequest,
    ) -> anyhow::Result<()> {
        let sql = build_create_external_table_sql(&req)?;
        self.run_query(crate::query::Query::sql(sql), true)
            .await?
            .into_record_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    fn parse_beacon_statement(sql: &str) -> anyhow::Result<BeaconStatement> {
        let mut parser = BeaconParser::new(sql)?;
        parser.parse_statement().map_err(Into::into)
    }
}

/// Drive a DataFusion result stream into a streamable job's spill buffer,
/// recording output metrics per batch. Returns `Ok` on clean completion or an
/// error string (execution failure, or a spill-cap breach reported by `push`).
async fn produce_stream_into_buffer(
    mut stream: datafusion::execution::SendableRecordBatchStream,
    buffer: &SpillableBatchBuffer,
    metrics: &MetricsTracker,
) -> Result<(), String> {
    loop {
        match stream.try_next().await {
            Ok(Some(batch)) => {
                metrics.add_output_rows(batch.num_rows() as u64);
                metrics.add_output_bytes(batch.get_array_memory_size() as u64);
                buffer.push(batch)?;
            }
            Ok(None) => return Ok(()),
            Err(error) => return Err(error.to_string()),
        }
    }
}

/// Background sweeper that evicts terminal query jobs past their TTL and aborts
/// streamable jobs that have not been polled within the idle window. Holds only a
/// `Weak` reference to the job registry, so it stops once the runtime is dropped.
fn spawn_query_job_sweeper(
    jobs: Weak<Mutex<HashMap<uuid::Uuid, QueryJob>>>,
    ttl: Duration,
    idle: Duration,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        loop {
            ticker.tick().await;
            let Some(jobs) = jobs.upgrade() else {
                break; // runtime dropped
            };
            let mut map = jobs.lock();
            sweep_jobs(&mut map, Instant::now(), ttl, idle);
        }
    });
}

/// Abort idle streamable jobs and evict terminal jobs past their TTL. Extracted
/// from the sweeper loop so the lifecycle policy can be unit-tested deterministically.
fn sweep_jobs(
    map: &mut HashMap<uuid::Uuid, QueryJob>,
    now: Instant,
    ttl: Duration,
    idle: Duration,
) {
    // Abort streamable jobs that no client is polling anymore.
    let idle_ids: Vec<uuid::Uuid> = map
        .iter()
        .filter(|(_, job)| {
            matches!(job.kind, JobKind::Streamable)
                && matches!(job.state, QueryJobState::Running)
                && now.duration_since(job.last_poll_at) > idle
        })
        .map(|(id, _)| *id)
        .collect();
    for id in idle_ids {
        if let Some(job) = map.get_mut(&id) {
            if let Some(handle) = job.handle.take() {
                handle.abort();
            }
            job.state = QueryJobState::Cancelled;
            job.finished_at = Some(now);
        }
    }

    // Evict terminal jobs whose retention window has elapsed (dropping their
    // buffers/output files, which deletes the temp files).
    map.retain(|_, job| match job.finished_at {
        Some(finished) => now.duration_since(finished) <= ttl,
        None => true,
    });
}

/// Assemble an injection-safe `CREATE EXTERNAL TABLE` statement from a structured
/// request. Identifiers (table name, `STORED AS` type, partition columns) are
/// validated as bare words; `LOCATION` and `OPTIONS` values are emitted as escaped
/// string literals. Options are rendered in sorted key order so the output is stable.
fn build_create_external_table_sql(req: &CreateExternalTableRequest) -> anyhow::Result<String> {
    ensure_bare_ident("table name", &req.name)?;
    ensure_bare_ident("file_type", &req.file_type)?;
    for col in &req.partition_cols {
        ensure_bare_ident("partition column", col)?;
    }

    let mut sql = String::from("CREATE EXTERNAL TABLE ");
    if req.if_not_exists {
        sql.push_str("IF NOT EXISTS ");
    }
    sql.push_str(&req.name);
    sql.push_str(" STORED AS ");
    sql.push_str(&req.file_type);
    sql.push_str(" LOCATION ");
    sql.push_str(&sql_string_literal(&req.location));

    if !req.partition_cols.is_empty() {
        sql.push_str(" PARTITIONED BY (");
        sql.push_str(&req.partition_cols.join(", "));
        sql.push(')');
    }

    if !req.options.is_empty() {
        let mut opts: Vec<(&String, &String)> = req.options.iter().collect();
        opts.sort_by(|a, b| a.0.cmp(b.0));
        let rendered = opts
            .iter()
            .map(|(key, value)| {
                format!("{} {}", sql_string_literal(key), sql_string_literal(value))
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(" OPTIONS (");
        sql.push_str(&rendered);
        sql.push(')');
    }

    Ok(sql)
}

/// Validate that `value` is a safe bare SQL identifier (`[A-Za-z_][A-Za-z0-9_]*`),
/// so it can be interpolated into generated DDL without quoting or injection risk.
fn ensure_bare_ident(kind: &str, value: &str) -> anyhow::Result<()> {
    let mut chars = value.chars();
    let valid = match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if !valid {
        anyhow::bail!("invalid {kind} '{value}': must match [A-Za-z_][A-Za-z0-9_]*");
    }
    Ok(())
}

/// Render a SQL single-quoted string literal, escaping embedded quotes by doubling.
fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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
        let runtime = Runtime::new(std::sync::Arc::new(beacon_config::Config::load().unwrap()))
            .await
            .expect("runtime should start");

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
        let runtime = Runtime::new(std::sync::Arc::new(beacon_config::Config::load().unwrap()))
            .await
            .expect("runtime should start");
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
        let runtime = Runtime::new(std::sync::Arc::new(beacon_config::Config::load().unwrap()))
            .await
            .expect("runtime should start");
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("json_q_{suffix}");

        run_sql(
            &runtime,
            &format!("CREATE TABLE {table} (a BIGINT, b BIGINT)"),
        )
        .await;
        run_sql(
            &runtime,
            &format!("INSERT INTO {table} VALUES (1, 2), (3, 4)"),
        )
        .await;

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
        let runtime = Runtime::new(std::sync::Arc::new(beacon_config::Config::load().unwrap()))
            .await
            .expect("runtime should start");
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

    /// NetCDF output goes through a custom `DataSink` that writes a real local
    /// file (the netcdf-c writer cannot stream to an object store). This asserts
    /// the file lands under the configured tmp store root — not the OS temp dir —
    /// and is non-empty: the regression fixed by threading `StorageConfig` into
    /// the NetCDF factory/sink.
    #[tokio::test(flavor = "multi_thread")]
    async fn query_with_netcdf_output_writes_under_configured_tmp() {
        let config = std::sync::Arc::new(beacon_config::Config::load().unwrap());
        let tmp_dir = config.storage.tmp_dir.clone();
        let runtime = Runtime::new(config).await.expect("runtime should start");
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("ncout_{suffix}");

        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1), (2)")).await;

        let result = runtime
            .run_query(
                query(serde_json::json!({
                    "from": table,
                    "select": ["a"],
                    "output": { "format": "netcdf" },
                })),
                false,
            )
            .await
            .expect("netcdf query with output should run");

        match result.query_output {
            QueryOutput::File(file) => {
                // tempfile resolves to an absolute path; canonicalize both
                // sides so the comparison is independent of cwd-relative form.
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
        let runtime = Runtime::new(std::sync::Arc::new(beacon_config::Config::load().unwrap()))
            .await
            .expect("runtime should start");
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("val_{suffix}");

        // Super-user seeds a table.
        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;

        // Non-super-user: read-only SELECT is allowed.
        runtime
            .run_query(
                crate::query::Query::sql(format!("SELECT * FROM {table}")),
                false,
            )
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

        // Both runtimes share one config so they resolve the same on-disk tables
        // store; the restart must rebuild from what the first runtime persisted.
        let config = std::sync::Arc::new(beacon_config::Config::load().unwrap());

        // First runtime: create a base table with data and a view over it.
        let runtime = Runtime::new(config.clone())
            .await
            .expect("runtime should start");
        run_sql(&runtime, &format!("CREATE TABLE {base} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {base} VALUES (1), (2)")).await;
        run_sql(
            &runtime,
            &format!("CREATE VIEW {view} AS SELECT a FROM {base}"),
        )
        .await;
        drop(runtime);

        // A fresh runtime rebuilds the catalog purely from the persisted
        // `tables://<name>/table.json` definitions.
        let restarted = Runtime::new(config).await.expect("runtime should restart");

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

#[cfg(test)]
mod external_table_sql_tests {
    use super::build_create_external_table_sql;
    use crate::api::CreateExternalTableRequest;
    use std::collections::HashMap;

    fn req(name: &str, file_type: &str, location: &str) -> CreateExternalTableRequest {
        CreateExternalTableRequest {
            name: name.to_string(),
            location: location.to_string(),
            file_type: file_type.to_string(),
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        }
    }

    #[test]
    fn builds_minimal_statement() {
        let sql = build_create_external_table_sql(&req("obs_ext", "PARQUET", "obs/")).unwrap();
        assert_eq!(
            sql,
            "CREATE EXTERNAL TABLE obs_ext STORED AS PARQUET LOCATION 'obs/'"
        );
    }

    #[test]
    fn builds_with_if_not_exists_partitions_and_sorted_options() {
        let mut r = req("argo", "NETCDF", "argo/**/*.nc");
        r.if_not_exists = true;
        r.partition_cols = vec!["year".to_string(), "month".to_string()];
        r.options
            .insert("read_dimensions".to_string(), "lat,lon".to_string());
        r.options.insert("a_flag".to_string(), "true".to_string());

        let sql = build_create_external_table_sql(&r).unwrap();
        assert_eq!(
            sql,
            "CREATE EXTERNAL TABLE IF NOT EXISTS argo STORED AS NETCDF LOCATION 'argo/**/*.nc' \
             PARTITIONED BY (year, month) OPTIONS ('a_flag' 'true', 'read_dimensions' 'lat,lon')"
        );
    }

    #[test]
    fn escapes_quotes_in_location_and_option_values() {
        let mut r = req("t", "CSV", "weird'/path");
        r.options.insert("delimiter".to_string(), "'".to_string());

        let sql = build_create_external_table_sql(&r).unwrap();
        assert_eq!(
            sql,
            "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'weird''/path' OPTIONS ('delimiter' '''')"
        );
    }

    #[test]
    fn rejects_unsafe_identifiers() {
        assert!(build_create_external_table_sql(&req("bad name", "PARQUET", "x/")).is_err());
        assert!(build_create_external_table_sql(&req("t; DROP TABLE u", "PARQUET", "x/")).is_err());
        assert!(build_create_external_table_sql(&req("t", "PARQUET'", "x/")).is_err());

        let mut r = req("t", "PARQUET", "x/");
        r.partition_cols = vec!["year)".to_string()];
        assert!(build_create_external_table_sql(&r).is_err());
    }
}

#[cfg(test)]
mod crawler_admin_tests {
    use super::Runtime;
    use crate::api::{CreateCrawlerRequest, TableNamingView};
    use std::collections::HashMap;

    /// The admin crawler API round-trips through the manager: create -> list/get ->
    /// run (zero discovery over an empty prefix) -> drop (idempotency error on the
    /// second drop). Uses a unique name so the shared on-disk store does not leak.
    #[tokio::test(flavor = "multi_thread")]
    async fn crawler_create_list_get_run_drop_round_trip() {
        let runtime = Runtime::new(std::sync::Arc::new(beacon_config::Config::load().unwrap()))
            .await
            .expect("runtime should start");

        let name = format!("crawler_test_{}", uuid::Uuid::new_v4().simple());

        runtime
            .create_crawler(CreateCrawlerRequest {
                name: name.clone(),
                target_prefix: format!("{name}/"),
                format_filter: Some(vec!["parquet".to_string()]),
                table_naming: TableNamingView::CrawlerPrefixed,
                detect_partitions: true,
                schedule_secs: None,
                event_driven: false,
                options: HashMap::new(),
            })
            .await
            .expect("create_crawler should succeed");

        // Listed and individually retrievable, with fields preserved.
        assert!(runtime.list_crawlers().iter().any(|c| c.name == name));
        let got = runtime
            .get_crawler(&name)
            .expect("crawler should be retrievable");
        assert_eq!(got.target_prefix, format!("{name}/"));
        assert_eq!(got.table_naming, TableNamingView::CrawlerPrefixed);

        // Runs on demand: an empty prefix yields a zero-discovery report.
        let report = runtime
            .run_crawler(&name)
            .await
            .expect("run_crawler should succeed");
        assert_eq!(report.crawler, name);
        assert_eq!(report.discovered, 0);

        // Dropped: gone from the catalog, and a second drop errors.
        runtime
            .drop_crawler(&name)
            .await
            .expect("drop_crawler should succeed");
        assert!(runtime.get_crawler(&name).is_none());
        assert!(runtime.drop_crawler(&name).await.is_err());
    }
}

#[cfg(test)]
mod query_job_tests {
    use super::{sweep_jobs, Runtime};
    use crate::api::QueryRequest;
    use crate::query::Query;
    use crate::query_job::{CancelOutcome, JobKind, PollOutcome, QueryJob, QueryJobState};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Boot a runtime (wrapped in `Arc`, as the job API requires), optionally
    /// tweaking the config first.
    async fn runtime_with(tweak: impl FnOnce(&mut beacon_config::Config)) -> Arc<Runtime> {
        let mut config = beacon_config::Config::load().unwrap();
        tweak(&mut config);
        Arc::new(
            Runtime::new(Arc::new(config))
                .await
                .expect("runtime should start"),
        )
    }

    /// Seed tables via the synchronous super-user path.
    async fn run_sql(runtime: &Runtime, sql: &str) {
        runtime
            .run_query(Query::sql(sql.to_string()), true)
            .await
            .expect("sql should run")
            .into_record_stream()
            .expect("streamed result")
            .try_collect::<Vec<_>>()
            .await
            .expect("sql should drain");
    }

    fn json_query(value: serde_json::Value) -> Query {
        serde_json::from_value::<QueryRequest>(value)
            .expect("query request should deserialize")
            .into_query()
            .expect("query request should convert")
    }

    /// Poll a streamable job to a terminal state, collecting every batch.
    async fn drain_job(
        runtime: &Arc<Runtime>,
        id: uuid::Uuid,
    ) -> Result<Vec<RecordBatch>, String> {
        let mut all = Vec::new();
        loop {
            match runtime
                .poll_query_job_stream(id, Duration::from_millis(500))
                .await
            {
                PollOutcome::Batches { batches, .. } => all.extend(batches),
                PollOutcome::Pending => continue,
                PollOutcome::Completed => return Ok(all),
                PollOutcome::Failed(error) => return Err(error),
                PollOutcome::NotStreamable
                | PollOutcome::Cancelled
                | PollOutcome::NotFound => panic!("unexpected poll outcome for {id}"),
            }
        }
    }

    /// A streamable job delivers all produced batches and ends `Completed`.
    #[tokio::test(flavor = "multi_thread")]
    async fn streamable_job_delivers_all_batches() {
        let runtime = runtime_with(|_| {}).await;
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("job_ok_{suffix}");

        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1), (2), (3)")).await;

        let (id, kind) = runtime
            .submit_query_job(Query::sql(format!("SELECT a FROM {table}")), false)
            .await
            .expect("submit should succeed");
        assert_eq!(kind, JobKind::Streamable);

        let batches = drain_job(&runtime, id).await.expect("job should succeed");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "all inserted rows should be delivered");
    }

    /// A query that fails mid-execution surfaces as a `Failed` terminal poll —
    /// the case the synchronous streaming endpoint cannot report.
    #[tokio::test(flavor = "multi_thread")]
    async fn streamable_job_reports_midstream_failure() {
        let runtime = runtime_with(|_| {}).await;
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("job_err_{suffix}");

        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1), (2)")).await;

        // `a - 1` is zero for a = 1, so the integer division errors at execution
        // (and cannot be constant-folded away since `a` is a column).
        let (id, _) = runtime
            .submit_query_job(
                Query::sql(format!("SELECT 1 / (a - 1) AS x FROM {table}")),
                false,
            )
            .await
            .expect("submit should succeed (planning is valid)");

        let error = drain_job(&runtime, id)
            .await
            .expect_err("job should fail at execution");
        assert!(!error.is_empty(), "failure should carry an error message");

        // Status reflects the failure too.
        let snapshot = runtime.query_job_snapshot(id).expect("job present");
        assert!(matches!(snapshot.state, QueryJobState::Failed { .. }));
    }

    /// With a tiny memory budget every batch spills to disk; results must still
    /// come back correctly and the global budget returns to zero.
    #[tokio::test(flavor = "multi_thread")]
    async fn streamable_job_spills_under_memory_pressure() {
        let runtime = runtime_with(|c| c.runtime.query_jobs.buffer_memory_bytes = 1).await;
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("job_spill_{suffix}");

        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {table} VALUES (10), (20), (30)")).await;

        let (id, _) = runtime
            .submit_query_job(Query::sql(format!("SELECT a FROM {table}")), false)
            .await
            .expect("submit should succeed");

        let batches = drain_job(&runtime, id).await.expect("job should succeed");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "rows must survive the spill round-trip");
        assert_eq!(
            runtime.query_job_budget.used(),
            0,
            "all in-memory budget should be released after draining"
        );
    }

    /// A file job materializes a result that the snapshot exposes for download.
    #[tokio::test(flavor = "multi_thread")]
    async fn file_job_materializes_result() {
        let runtime = runtime_with(|_| {}).await;
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("job_file_{suffix}");

        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1), (2)")).await;

        let (id, kind) = runtime
            .submit_query_job(
                json_query(serde_json::json!({
                    "from": table,
                    "select": ["a"],
                    "output": { "format": "parquet" },
                })),
                false,
            )
            .await
            .expect("submit should succeed");
        assert_eq!(kind, JobKind::File);

        // Poll status until terminal.
        let snapshot = loop {
            let snapshot = runtime.query_job_snapshot(id).expect("job present");
            match snapshot.state {
                QueryJobState::Running => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                _ => break snapshot,
            }
        };
        assert!(matches!(snapshot.state, QueryJobState::Succeeded));
        let file = snapshot.file.expect("succeeded file job has a result file");
        let len = std::fs::metadata(&file.path).expect("result file exists").len();
        assert!(len > 0, "result file should be non-empty");
    }

    /// Cancelling a job drives it terminal; a second cancel is a no-op.
    #[tokio::test(flavor = "multi_thread")]
    async fn cancel_drives_job_terminal() {
        let runtime = runtime_with(|_| {}).await;
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("job_cancel_{suffix}");

        run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
        run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1)")).await;

        let (id, _) = runtime
            .submit_query_job(Query::sql(format!("SELECT a FROM {table}")), false)
            .await
            .expect("submit should succeed");

        // The job may finish before we cancel (tiny query): both are valid first
        // outcomes. Either way the job is terminal afterward.
        let first = runtime.cancel_query_job(id);
        assert!(matches!(
            first,
            CancelOutcome::Cancelled | CancelOutcome::AlreadyFinished
        ));
        assert!(
            !matches!(
                runtime.query_job_snapshot(id).expect("job present").state,
                QueryJobState::Running
            ),
            "job should be terminal after cancel"
        );
        assert_eq!(runtime.cancel_query_job(id), CancelOutcome::AlreadyFinished);
    }

    /// The sweeper aborts idle running streamable jobs and evicts terminal jobs
    /// past their TTL.
    #[test]
    fn sweep_aborts_idle_and_evicts_expired() {
        let ttl = Duration::from_secs(600);
        let idle = Duration::from_secs(120);
        let now = Instant::now();
        let old = now
            .checked_sub(Duration::from_secs(10_000))
            .unwrap_or(now);

        let mut map: HashMap<uuid::Uuid, QueryJob> = HashMap::new();

        // Idle running streamable job → should be cancelled (now terminal).
        let idle_id = uuid::Uuid::new_v4();
        let mut idle_job = QueryJob::new_running(JobKind::Streamable, None);
        idle_job.last_poll_at = old;
        map.insert(idle_id, idle_job);

        // Terminal job finished long ago → should be evicted.
        let expired_id = uuid::Uuid::new_v4();
        let mut expired = QueryJob::new_running(JobKind::File, None);
        expired.state = QueryJobState::Succeeded;
        expired.finished_at = Some(old);
        map.insert(expired_id, expired);

        // Fresh running job → untouched.
        let fresh_id = uuid::Uuid::new_v4();
        map.insert(fresh_id, QueryJob::new_running(JobKind::Streamable, None));

        sweep_jobs(&mut map, now, ttl, idle);

        assert!(
            matches!(
                map.get(&idle_id).map(|j| &j.state),
                Some(QueryJobState::Cancelled)
            ),
            "idle streamable job should be cancelled"
        );
        assert!(!map.contains_key(&expired_id), "expired job should be evicted");
        assert!(
            matches!(
                map.get(&fresh_id).map(|j| &j.state),
                Some(QueryJobState::Running)
            ),
            "fresh job should be left running"
        );
    }
}
