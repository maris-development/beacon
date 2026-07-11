//! High-level Beacon runtime shared by the API transports.

use std::{collections::HashMap, sync::Arc};

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
    query_result::{ArrowOutputStream, QueryOutput, QueryOutputFile, QueryResult},
    sys::{self, SystemInfo},
};

/// Beacon's single execution layer: startup, catalog access, queries, SQL, and files.
pub struct Runtime {
    session_ctx: Arc<SessionContext>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    listing_table_factory: Arc<ListingTableFactoryExt>,
    crawler_manager: Arc<beacon_data_lake::crawler::CrawlerManager>,
    query_metrics: Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>,
    /// Documentation for the registered table-valued functions, captured at
    /// startup (DataFusion's UDTF registry is not enumerable with metadata).
    table_function_docs: Vec<FunctionDoc>,
    /// The configuration this runtime was built with. Owned, not process-global.
    config: Arc<beacon_config::Config>,
    /// Authentication + authorization context (users, roles, grants). Shared, owned by the runtime.
    auth: Arc<beacon_auth::AuthContext>,
    /// The datasets object store, retained so the admin file-management endpoints
    /// (upload/download/delete) can operate on it directly.
    datasets_store: Arc<beacon_object_storage::DatasetsStore>,
    /// In-progress chunked (resumable) upload sessions for large files.
    upload_manager: Arc<crate::dataset_uploads::UploadManager>,
}

impl Runtime {
    /// Boots the Beacon execution environment with the given configuration.
    ///
    /// The config is owned by the runtime (not read from a process-global) and is
    /// threaded into every component below — including, via a `SessionConfig`
    /// extension, the deep session-scoped code (query planning, table refresh,
    /// metadata UDFs) that cannot otherwise reach it.
    ///
    /// Auth state is persisted in the SQLite directory under [`beacon_config::USERS_DIR`].
    pub async fn new(config: Arc<beacon_config::Config>) -> anyhow::Result<Self> {
        let auth = Self::init_auth(&config)?;
        Self::new_with_auth(config, auth).await
    }

    /// Boots a runtime with an ephemeral in-memory auth context (no on-disk SQLite directory).
    /// Used by tests so they don't contend on the shared persistent auth database.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn new_with_in_memory_auth(
        config: Arc<beacon_config::Config>,
    ) -> anyhow::Result<Self> {
        let auth = Self::init_in_memory_auth(&config)?;
        Self::new_with_auth(config, auth).await
    }

    /// Boots the Beacon execution environment around an already-built authorization context.
    async fn new_with_auth(
        config: Arc<beacon_config::Config>,
        auth: Arc<beacon_auth::AuthContext>,
    ) -> anyhow::Result<Self> {
        let memory_pool = Arc::new(FairSpillPool::new(
            config.runtime.vm_memory_size * 1024 * 1024,
        ));

        let object_stores = beacon_object_storage::ObjectStores::new(&config.storage).await?;
        let datasets_store = object_stores.datasets.clone();
        let upload_manager = crate::dataset_uploads::UploadManager::new(
            config.storage.upload_part_size,
            config.storage.upload_session_ttl_secs,
        );

        // The crawler manager is built after the file formats are registered, so
        // register an empty handle now and fill it below — the same late-fill
        // pattern as the query planner's session cell.
        let crawler_handle = beacon_data_lake::crawler::new_crawler_manager_handle();
        let session_ctx = Self::init_ctx(
            memory_pool,
            object_stores.datasets.clone(),
            object_stores.tables.clone(),
            config.clone(),
            crawler_handle.clone(),
            auth.clone(),
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

        // Capture table-function docs now: the UDTF registry above cannot be
        // enumerated with metadata, so `/api/table-functions` reads this snapshot.
        let table_function_docs = table_functions
            .iter()
            .map(|f| FunctionDoc::from_beacon_table_function(f.as_ref()))
            .collect::<Vec<_>>();

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

        Ok(Self {
            session_ctx,
            file_formats,
            listing_table_factory,
            crawler_manager,
            query_metrics: Arc::new(Mutex::new(HashMap::new())),
            table_function_docs,
            config,
            auth,
            datasets_store,
            upload_manager,
        })
    }

    /// Builds the authorization context backed by the SQLite auth directory (users, roles, and
    /// grants persisted under [`beacon_config::USERS_DIR`]), seeding the configured admin as a
    /// super-user (built-in `admin` role with a global `ALL` grant) so the auth-management SQL is
    /// reachable on a fresh instance.
    ///
    /// State persists across restarts, so the bootstrap is idempotent: existing roles/users are
    /// left in place rather than re-created.
    fn init_auth(config: &beacon_config::Config) -> anyhow::Result<Arc<beacon_auth::AuthContext>> {
        let store = beacon_auth::SqliteStore::open(beacon_config::USERS_DIR.join("directory.db"))?;
        let role_provider = beacon_auth::RoleProvider::with_persistence(store.clone())?;
        let local: Arc<dyn beacon_auth::AuthProvider> =
            Arc::new(beacon_auth::SqliteAuthProvider::new(store));
        let provider = Self::build_auth_provider(local, config);
        let auth = beacon_auth::AuthContext::with_role_provider(provider, role_provider);
        Self::bootstrap_auth(auth, config)
    }

    /// Wraps the local username/password provider with an OIDC provider (routing bearer tokens to
    /// it) when OIDC is enabled, otherwise returns the local provider unchanged. Beacon still owns
    /// authorization: OIDC supplies the identity and role names, the role model owns the grants.
    fn build_auth_provider(
        local: Arc<dyn beacon_auth::AuthProvider>,
        config: &beacon_config::Config,
    ) -> Arc<dyn beacon_auth::AuthProvider> {
        if !config.oidc.enabled {
            return local;
        }
        let oidc = beacon_auth::OidcAuthProvider::new(beacon_auth::OidcConfig {
            issuer: config.oidc.issuer.clone(),
            jwks_url: config.oidc.jwks_url.clone(),
            audience: (!config.oidc.audience.is_empty()).then(|| config.oidc.audience.clone()),
            roles_claim: config.oidc.roles_claim.clone(),
            username_claim: config.oidc.username_claim.clone(),
            jwks_cache_ttl: std::time::Duration::from_secs(config.oidc.jwks_cache_ttl_secs),
        });
        Arc::new(beacon_auth::CompositeAuthProvider::new(
            local,
            Arc::new(oidc),
        ))
    }

    /// Builds an ephemeral, non-persistent auth context backed by the in-memory basic-auth provider.
    /// Used by tests to avoid contending on the shared on-disk SQLite directory.
    #[cfg(any(test, feature = "test-util"))]
    fn init_in_memory_auth(
        config: &beacon_config::Config,
    ) -> anyhow::Result<Arc<beacon_auth::AuthContext>> {
        let auth = beacon_auth::AuthContext::new(Arc::new(beacon_auth::BasicAuthProvider::new()));
        Self::bootstrap_auth(auth, config)
    }

    /// Configures the single super-user from the `BEACON_ADMIN_*` environment variables and seeds
    /// the optional anonymous user. Idempotent and safe to run against an already-populated
    /// (persistent) auth context.
    ///
    /// The super-user is config-defined, not a stored role or user: it cannot be created, changed,
    /// or duplicated through SQL. Every role and user created via SQL is read-only and never a
    /// super-user.
    fn bootstrap_auth(
        mut auth: beacon_auth::AuthContext,
        config: &beacon_config::Config,
    ) -> anyhow::Result<Arc<beacon_auth::AuthContext>> {
        auth.set_super_user(&config.admin.username, &config.admin.password);

        // Seed the built-in anonymous user (empty password, no roles) unless disabled. Admins can
        // assign read-only roles to it via `GRANT ROLE <role> TO USER anonymous`.
        if config.auth.anonymous_enabled {
            if !auth.user_exists(beacon_auth::ANONYMOUS_USERNAME) {
                auth.create_user(beacon_auth::ANONYMOUS_USERNAME, "")?;
            }
            auth.set_anonymous_user(beacon_auth::ANONYMOUS_USERNAME);
        }

        Ok(Arc::new(auth))
    }

    /// The runtime's shared authorization context (users, roles, grants).
    pub fn auth_context(&self) -> &Arc<beacon_auth::AuthContext> {
        &self.auth
    }

    /// Authenticates a credential against the configured auth provider and resolves the principal's
    /// roles into an identity.
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

    /// List all principals for the admin Users page: the config-defined super-user first (flagged,
    /// not editable), then every stored local user with its roles. OIDC-only deployments have no
    /// stored users, so only the super-user is returned.
    pub fn list_auth_users(&self) -> anyhow::Result<Vec<crate::api::AuthUserView>> {
        let super_username = self.config.admin.username.clone();
        let anonymous_username = self.auth.anonymous_username().map(str::to_owned);
        let mut views = vec![crate::api::AuthUserView {
            username: super_username.clone(),
            roles: Vec::new(),
            is_super_user: true,
            is_anonymous: false,
        }];
        for record in self.auth.list_users()? {
            // The super-user is config-only, never stored; guard against a name clash anyway.
            if record.username == super_username {
                continue;
            }
            let is_anonymous = anonymous_username.as_deref() == Some(record.username.as_str());
            views.push(crate::api::AuthUserView {
                is_anonymous,
                ..crate::api::AuthUserView::from(record)
            });
        }
        Ok(views)
    }

    /// Snapshot of all roles with their grant/deny rules, for the admin Roles page.
    pub fn list_auth_roles(&self) -> Vec<crate::api::AuthRoleView> {
        self.auth
            .list_roles()
            .into_iter()
            .map(crate::api::AuthRoleView::from)
            .collect()
    }

    fn init_ctx(
        memory_pool: Arc<FairSpillPool>,
        datasets_store: Arc<beacon_object_storage::DatasetsStore>,
        tables_store: Arc<dyn object_store::ObjectStore>,
        app_config: Arc<beacon_config::Config>,
        crawler_handle: beacon_data_lake::crawler::CrawlerManagerHandle,
        auth: Arc<beacon_auth::AuthContext>,
    ) -> anyhow::Result<Arc<SessionContext>> {
        // Runtime-scoped Lance warehouse over beacon's tables object store,
        // threaded through the session so managed-table CRUD/index ops stay
        // isolated per runtime — no process globals. The tables store is the same
        // (always-local) one used for each table's `table.json`.
        let lance_warehouse = Arc::new(beacon_lance::LanceWarehouse::new(tables_store));

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
            .with_extension(lance_warehouse)
            // The (late-filled) crawler manager handle, so DDL crawler actions can
            // reach it from session-scoped execution.
            .with_extension(crawler_handle)
            // The auth context, so the auth-management statement node can mutate users/roles/grants
            // from session-scoped execution.
            .with_extension(auth);

        config.options_mut().sql_parser.enable_ident_normalization = false;
        // Register beacon's session-scoped SQL options so `SET beacon.table_engine`
        // can override the managed-table engine per session.
        config
            .options_mut()
            .extensions
            .insert(crate::statement_plan::table_engine::BeaconSqlOptions::default());
        config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = false;
        config
            .options_mut()
            .execution
            .parquet
            .allow_single_file_parallelism = true;
        config.options_mut().optimizer.expand_views_at_output = true; // Expain view types (utf8view to utf8)
        config.options_mut().sql_parser.map_string_types_to_utf8view = false; // Map varchar to utf8

        let runtime_env = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(DiskManagerBuilder::default())
            .with_memory_pool(memory_pool)
            .with_cache_manager(
                datafusion::execution::cache::cache_manager::CacheManagerConfig {
                    table_files_statistics_cache: Some(beacon_file_statistics_cache()),
                    // Disable DataFusion's directory-listing cache. Its default
                    // config installs one (non-zero `list_files_cache_limit`) with a
                    // TTL, which serves stale listings — missing freshly written
                    // files and retaining deleted ones — and broke read-after-write
                    // for the datasets view (uploads invisible, deletes lingering).
                    list_files_cache_limit: 0,
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
            // Sink element-wise projections below the nd broadcast so they run
            // on un-broadcast coordinate axes instead of the full grid. Appended
            // after the default physical rules, so it sees the planned
            // `ProjectionExec` above `NdBroadcastExec`.
            .with_physical_optimizer_rule(Arc::new(
                beacon_datafusion_ext::nd::NdProjectionPushdown::new(),
            ))
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
            self.config.auth.enforce,
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
            self.config.auth.enforce,
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
            self.config.auth.enforce,
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

    /// Load a table's downstream extensions (MCP descriptor, query presets).
    /// Returns an empty set if the table has none.
    pub async fn get_table_extensions(
        &self,
        table_name: String,
    ) -> anyhow::Result<crate::extensions::TableExtensions> {
        crate::extensions::get_table_extensions(&self.session_ctx, &table_name).await
    }

    /// Replace a table's extensions document, validating it against the table
    /// schema. An empty document removes the stored extensions.
    pub async fn set_table_extensions(
        &self,
        table_name: String,
        extensions: crate::extensions::TableExtensions,
    ) -> anyhow::Result<()> {
        crate::extensions::set_table_extensions(&self.session_ctx, &table_name, extensions).await
    }

    /// Remove all of a table's extensions.
    pub async fn delete_table_extensions(&self, table_name: String) -> anyhow::Result<()> {
        crate::extensions::delete_table_extensions(&self.session_ctx, &table_name).await
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
        // List against the datasets store's *backing* store (disk/S3 truth) rather
        // than the cache-backed facade. The cache is kept warm asynchronously by
        // the filesystem watcher, which on some platforms lags or replays stale
        // events — so a freshly uploaded or just-deleted file would otherwise be
        // missing from / lingering in the listing until the watcher caught up.
        Ok(beacon_data_lake::list_datasets(
            &self.session_ctx,
            self.datasets_store.backing_store(),
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
        Ok(
            beacon_data_lake::list_dataset_schema(&self.session_ctx, &self.file_formats, &file)
                .await?,
        )
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
        self.run_query(
            crate::query::Query::sql(sql),
            beacon_auth::AuthIdentity::system(),
        )
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

    /// The set of filename extensions an uploaded dataset may carry, derived from
    /// the formats this runtime can actually read. Keeping the allowlist in sync
    /// with the registry means an upload only succeeds if it could be queried.
    pub fn dataset_upload_extensions(&self) -> Vec<String> {
        let mut exts: Vec<String> = self
            .file_formats
            .iter()
            .flat_map(|f| f.file_extensions())
            .map(|e| e.trim_start_matches('.').to_ascii_lowercase())
            .filter(|e| !e.is_empty())
            .collect();
        exts.sort();
        exts.dedup();
        exts
    }

    /// Stream an uploaded file into the datasets store. The path is validated
    /// (anti-traversal, internal-prefix guard) and its extension checked against
    /// [`Self::dataset_upload_extensions`] before any bytes are written. The size
    /// cap comes from `BEACON_MAX_UPLOAD_BYTES`.
    pub async fn upload_dataset<S>(
        &self,
        raw_path: &str,
        overwrite: bool,
        body: S,
    ) -> Result<crate::dataset_files::UploadResult, crate::dataset_files::FileError>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
    {
        let path = crate::dataset_files::validate_dataset_path(raw_path)?;
        crate::dataset_files::validate_extension(&path, &self.dataset_upload_extensions())?;
        let max_bytes = self.config.storage.max_upload_bytes;
        crate::dataset_files::upload_dataset(
            &self.datasets_store,
            &path,
            overwrite,
            max_bytes,
            body,
        )
        .await
    }

    /// Begin a chunked (resumable) upload for a large file. Validates the path and
    /// extension and the overwrite policy, opens a multipart upload, and returns
    /// the new session id plus the part size clients should slice the file into.
    pub async fn initiate_dataset_upload(
        &self,
        raw_path: &str,
        overwrite: bool,
    ) -> Result<(uuid::Uuid, usize), crate::dataset_files::FileError> {
        let path = crate::dataset_files::validate_dataset_path(raw_path)?;
        crate::dataset_files::validate_extension(&path, &self.dataset_upload_extensions())?;

        if !overwrite {
            use object_store::ObjectStoreExt;
            match self.datasets_store.head(&path).await {
                Ok(_) => {
                    return Err(crate::dataset_files::FileError::AlreadyExists(
                        path.to_string(),
                    ))
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }

        let max_bytes = self.config.storage.max_upload_bytes;
        let id = self
            .upload_manager
            .initiate(&self.datasets_store, path, max_bytes)
            .await?;
        Ok((id, self.upload_manager.part_size()))
    }

    /// Submit one part of a chunked upload (1-based `part_number`, in order).
    pub async fn upload_dataset_part(
        &self,
        upload_id: uuid::Uuid,
        part_number: u32,
        data: bytes::Bytes,
    ) -> Result<(), crate::dataset_files::FileError> {
        self.upload_manager
            .put_part(upload_id, part_number, data)
            .await
    }

    /// Finalize a chunked upload, returning the resulting object's key and size.
    pub async fn complete_dataset_upload(
        &self,
        upload_id: uuid::Uuid,
    ) -> Result<crate::dataset_files::UploadResult, crate::dataset_files::FileError> {
        self.upload_manager.complete(upload_id).await
    }

    /// Abort and discard an in-progress chunked upload.
    pub async fn abort_dataset_upload(
        &self,
        upload_id: uuid::Uuid,
    ) -> Result<(), crate::dataset_files::FileError> {
        self.upload_manager.abort(upload_id).await
    }

    /// Open a streaming read of a dataset file for download. The path is validated
    /// before the store is touched.
    pub async fn download_dataset(
        &self,
        raw_path: &str,
    ) -> Result<object_store::GetResult, crate::dataset_files::FileError> {
        let path = crate::dataset_files::validate_dataset_path(raw_path)?;
        crate::dataset_files::download_dataset(&self.datasets_store, &path).await
    }

    /// Delete a dataset file. The path is validated, then a best-effort dependency
    /// check refuses the delete (with [`crate::dataset_files::FileError::InUse`]) if
    /// any registered table references the file, so a query is not silently broken.
    pub async fn delete_dataset(
        &self,
        raw_path: &str,
    ) -> Result<(), crate::dataset_files::FileError> {
        let path = crate::dataset_files::validate_dataset_path(raw_path)?;

        let dependents = self.dataset_dependents(path.as_ref()).await;
        if !dependents.is_empty() {
            return Err(crate::dataset_files::FileError::InUse {
                path: path.to_string(),
                dependents,
            });
        }

        crate::dataset_files::delete_dataset(&self.datasets_store, &path).await
    }

    /// Best-effort discovery of registered tables that reference a dataset path.
    ///
    /// Each table's flattened config is serialized and scanned for the path
    /// substring (the same shape exposed by the admin table-config endpoint). This
    /// catches external tables pointed directly at a file or its parent directory;
    /// it is intentionally conservative — a hit blocks the delete so the caller can
    /// drop the table first.
    async fn dataset_dependents(&self, path: &str) -> Vec<String> {
        let mut dependents = Vec::new();
        for table in self.list_tables() {
            if let Some(view) = self.list_table_config(table.clone()).await {
                if let Ok(json) = serde_json::to_string(&view.config) {
                    if json.contains(path) {
                        dependents.push(table);
                    }
                }
            }
        }
        dependents
    }
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
        let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
            beacon_config::Config::load().unwrap(),
        ))
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
        let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
            beacon_config::Config::load().unwrap(),
        ))
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
        let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
            beacon_config::Config::load().unwrap(),
        ))
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
                beacon_auth::AuthIdentity::empty(),
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
        let runtime = Runtime::new(std::sync::Arc::new(beacon_config::Config::load().unwrap()))
            .await
            .expect("runtime should start");
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("ext_{suffix}");

        run_sql(
            &runtime,
            &format!("CREATE TABLE {table} (lat BIGINT, depth BIGINT)"),
        )
        .await;

        // SET a preset via SQL, then read it back through the typed API.
        run_sql(
            &runtime,
            &format!(
                "SET EXTENSION 'preset' FOR {table} TO '{{\"presets\":[{{\"name\":\"shallow\",\"filters\":[{{\"column\":\"depth\",\"op\":\"<=\",\"value\":10}}]}}]}}'"
            ),
        )
        .await;

        let ext = runtime
            .get_table_extensions(table.clone())
            .await
            .expect("extensions should load");
        let preset = ext.preset.expect("preset extension should be set");
        assert_eq!(preset.presets[0].name, "shallow");

        // SHOW EXTENSIONS returns one JSON row mentioning the preset.
        let batches = runtime
            .run_query(
                crate::query::Query::sql(format!("SHOW EXTENSIONS FOR {table}")),
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
        assert!(
            json.contains("shallow"),
            "SHOW output should include the preset: {json}"
        );

        // An extension over a non-existent column is rejected by validation.
        let rejected = try_run_sql(
            &runtime,
            &format!(
                "SET EXTENSION 'preset' FOR {table} TO '{{\"presets\":[{{\"name\":\"x\",\"filters\":[{{\"column\":\"ghost\",\"op\":\"=\",\"value\":1}}]}}]}}'"
            ),
        )
        .await;
        assert!(
            rejected.is_err(),
            "preset over a missing column should be rejected"
        );

        // DROP removes it; the document becomes empty.
        run_sql(&runtime, &format!("DROP EXTENSION 'preset' FOR {table}")).await;
        assert!(
            runtime
                .get_table_extensions(table.clone())
                .await
                .expect("extensions should load")
                .is_empty(),
            "dropping the only extension leaves an empty document"
        );
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

    /// A query with an `output` format is written to a file and returned as a
    /// file download.
    #[tokio::test(flavor = "multi_thread")]
    async fn query_with_output_format_produces_file() {
        let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
            beacon_config::Config::load().unwrap(),
        ))
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
                beacon_auth::AuthIdentity::empty(),
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
        let runtime = Runtime::new_with_in_memory_auth(config)
            .await
            .expect("runtime should start");
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
                beacon_auth::AuthIdentity::empty(),
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
        let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
            beacon_config::Config::load().unwrap(),
        ))
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
                beacon_auth::AuthIdentity::empty(),
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
                beacon_auth::AuthIdentity::empty(),
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

    /// A non-super-user cannot escalate its own privileges: all auth-management statements are
    /// super-user-only (they lower to extension nodes), so a role-limited caller cannot create a
    /// role, grant itself privileges, or create users. A super-user can run the same statements.
    #[tokio::test(flavor = "multi_thread")]
    async fn non_super_user_cannot_escalate_privileges() {
        let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
            beacon_config::Config::load().unwrap(),
        ))
        .await
        .expect("runtime should start");

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
        run_sql(&runtime, "CREATE ROLE analyst").await;
        run_sql(&runtime, "GRANT SELECT ON TABLE t TO ROLE analyst").await;

        // But even the super-user cannot grant write/management privileges to a role — roles are
        // read-only, so there is no way to mint another super-user through SQL.
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
        let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
            beacon_config::Config::load().unwrap(),
        ))
        .await
        .expect("runtime should start");
        let suffix = uuid::Uuid::new_v4().simple();
        let table = format!("idx_{suffix}");

        run_sql(
            &runtime,
            &format!("CREATE TABLE {table} (id BIGINT, name VARCHAR)"),
        )
        .await;
        run_sql(
            &runtime,
            &format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b')"),
        )
        .await;
        run_sql(
            &runtime,
            &format!("CREATE INDEX {table}_id_idx ON {table} (id) USING btree"),
        )
        .await;

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
            count_indexes(format!("SHOW INDEXES ON {table}")).await,
            1,
            "one index should be listed"
        );

        run_sql(&runtime, &format!("DROP INDEX {table}_id_idx ON {table}")).await;
        assert_eq!(
            count_indexes(format!("SHOW INDEXES ON {table}")).await,
            0,
            "no indexes after drop"
        );

        run_sql(&runtime, &format!("DROP TABLE {table}")).await;
    }
}

#[cfg(test)]
mod restart_tests {
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
        let runtime = Runtime::new_with_in_memory_auth(config.clone())
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
        let restarted = Runtime::new_with_in_memory_auth(config)
            .await
            .expect("runtime should restart");

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
            .run_query(
                crate::query::Query::sql(format!("DROP TABLE {view}")),
                beacon_auth::AuthIdentity::system(),
            )
            .await;
        let _ = restarted
            .run_query(
                crate::query::Query::sql(format!("DROP TABLE {base}")),
                beacon_auth::AuthIdentity::system(),
            )
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
        let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(
            beacon_config::Config::load().unwrap(),
        ))
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
