use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, OnceLock, Weak},
};

use crate::crawler::{new_crawler_manager_handle, CrawlerConfig, CrawlerManager};
use crate::schema_persistence::{init_tables, PersistentSchemaProvider};
use beacon_arrow_bbf::datafusion::BBFFormatFactory;
use beacon_arrow_csv::datafusion::CsvFormatFactory;
use beacon_arrow_geoparquet::datafusion::GeoParquetFormatFactory;
use beacon_arrow_ipc::datafusion::ArrowFormatFactory;
use beacon_arrow_netcdf::datafusion::NetcdfConfig;
use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
use beacon_arrow_tiff::datafusion::TiffFormatFactory;
use beacon_arrow_zarr::datafusion::ZarrFormatFactory;
use beacon_auth::{
    AuthContext, BasicAuthProvider, InMemoryUserStore, RoleProvider, RoleStore, UserDirectory,
};
use beacon_datafusion_ext::{
    consts::{DEFAULT_DB_STORE_URL_OBJECT_URL, TMP_STORE_URL_OBJECT_URL},
    format_ext::FileFormatFactoryExt,
    listing_table_factory_ext::ListingTableFactoryExt,
    nd::NdProjectionPushdown,
    object_store_registry::LazyObjectStoreRegistry,
    secrets::SecretStore,
    stats_cache::BeaconFileStatisticsCache,
    type_widening::{ArrowTypeWidening, ArrowTypeWideningStrategy, DefaultArrowTypeWidening},
};
use beacon_functions::register_functions;
use beacon_redb_store::RedbStore;
use datafusion::{
    execution::{
        cache::cache_manager::CacheManagerConfig,
        disk_manager::DiskManagerBuilder,
        memory_pool::FairSpillPool,
        object_store::ObjectStoreUrl,
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
        SessionStateBuilder,
    },
    optimizer::OptimizerRule,
    prelude::{SessionConfig, SessionContext},
};
use object_store::ObjectStore;
use parking_lot::Mutex;
use tempfile::env::temp_dir;
use tokio::runtime::Handle;

use crate::{
    auth_store::TablesAuthStore,
    runtime::Runtime,
    settings::{SqlSettings, SqlStreamCoalesceSettings},
    statement_plan::{new_session_cell, BeaconQueryPlanner, CoalesceSqlStream, SessionCell},
};

#[derive(Default)]
pub struct RuntimeBuilder {
    pub runtime_handle: Option<Handle>,

    pub db_path: Option<PathBuf>,
    pub tmp_dir_path: Option<PathBuf>,
    /// The URL relative dataset paths resolve against, and the URL the datasets
    /// store is registered under. `None` => the `datasets://` default (see
    /// [`ObjectStoreUrls::default`]).
    pub default_store_url: Option<ObjectStoreUrl>,
    /// Backs [`Self::default_store_url`]. `None` => the store described by
    /// [`Self::storage`] (by default a local filesystem rooted at the cwd).
    pub default_store: Option<Arc<dyn ObjectStore>>,

    pub admin_username: Option<String>,
    pub admin_password: Option<String>,

    pub vm_memory_limit: Option<usize>,
    pub vm_cpu_limit: Option<usize>,

    pub batch_size: Option<usize>,
    pub nd_pipeline: bool,

    /// How client queries are compiled and how their results are streamed back.
    /// Defaults to [`SqlSettings::default`].
    pub sql: SqlSettings,

    pub crawler: CrawlerConfig,

    pub netcdf: NetcdfConfig,

    pub auth_provider: Option<Arc<dyn beacon_auth::AuthProvider>>,
    pub secrets_encryption_key: Option<[u8; 32]>,

    /// Username resolving unauthenticated access. `None` disables anonymous access.
    pub anonymous_username: Option<String>,
    /// Whether table-level grants are enforced for non-super-users.
    pub auth_enforce: bool,

    pub type_widening: Option<Arc<dyn ArrowTypeWideningStrategy>>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_path(db_path: PathBuf) -> Self {
        Self::new().with_db_path(db_path)
    }

    pub fn with_runtime_handle(mut self, handle: Handle) -> Self {
        self.runtime_handle = Some(handle);
        self
    }

    /// Persists the tables store (catalog + managed data) to a single redb file at
    /// `db_path`. Without this the tables store is in-memory and nothing persists.
    pub fn with_db_path(mut self, db_path: PathBuf) -> Self {
        self.db_path = Some(db_path);
        self
    }

    /// Resolves dataset paths against `url`, backed by `store`, in place of the
    /// datasets store [`Self::storage`] would otherwise describe.
    ///
    /// `store` is a plain [`ObjectStore`], registered as the datasets store as-is.
    /// Two consequences, since a raw store cannot describe itself: it emits no change
    /// notifications, so external tables over it become current on an explicit
    /// `REFRESH` only; and the netCDF/Atlas readers open files natively under
    /// `storage.datasets_dir` (a local root), so an embedder injecting a *local*
    /// store that it wants to read natively must point `storage.datasets_dir` at that
    /// store's root (an S3-backed store cannot be read natively at all).
    pub fn with_default_store(mut self, url: ObjectStoreUrl, store: Arc<dyn ObjectStore>) -> Self {
        self.default_store_url = Some(url);
        self.default_store = Some(store);
        self
    }

    pub fn with_admin_credentials(mut self, username: String, password: String) -> Self {
        self.admin_username = Some(username);
        self.admin_password = Some(password);
        self
    }

    pub fn with_vm_memory_limit(mut self, limit: usize) -> Self {
        self.vm_memory_limit = Some(limit);
        self
    }

    pub fn with_vm_cpu_limit(mut self, limit: usize) -> Self {
        self.vm_cpu_limit = Some(limit);
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    pub fn with_nd_pipeline(mut self) -> Self {
        self.nd_pipeline = true;
        self
    }

    /// Replaces the SQL settings wholesale. Without this, the defaults apply.
    pub fn with_sql_settings(mut self, settings: SqlSettings) -> Self {
        self.sql = settings;
        self
    }

    /// Replaces just the result-stream coalescing settings; set `enabled: false`
    /// to stream batches through untouched.
    pub fn with_sql_stream_coalesce(mut self, settings: SqlStreamCoalesceSettings) -> Self {
        self.sql.stream_coalesce = settings;
        self
    }

    /// Configures the crawler subsystem. `CrawlerConfig::enable = false` builds a
    /// runtime with no crawler at all (crawler DDL then errors).
    pub fn with_crawler(mut self, crawler: CrawlerConfig) -> Self {
        self.crawler = crawler;
        self
    }

    pub fn with_netcdf_reader_cache(mut self, cache_size: usize) -> Self {
        self.netcdf.use_reader_cache = true;
        self.netcdf.reader_cache_size = cache_size;
        self
    }

    pub fn with_netcdf_statistics(mut self) -> Self {
        self.netcdf.enable_statistics = true;
        self
    }

    /// Replaces the whole NetCDF reader configuration.
    pub fn with_netcdf_config(mut self, netcdf: NetcdfConfig) -> Self {
        self.netcdf = netcdf;
        self
    }

    pub fn with_auth_provider(mut self, provider: Arc<dyn beacon_auth::AuthProvider>) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    pub fn with_secrets_encryption(mut self, key: [u8; 32]) -> Self {
        self.secrets_encryption_key = Some(key);
        self
    }

    pub fn with_type_widening(mut self, strategy: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        self.type_widening = Some(strategy);
        self
    }

    pub fn with_tmp_dir_path(mut self, path: PathBuf) -> Self {
        self.tmp_dir_path = Some(path);
        self
    }

    /// Enables anonymous access, resolving unauthenticated callers to `username`.
    /// The user is created (password-less, roleless) at build time if absent.
    pub fn with_anonymous_user(mut self, username: impl Into<String>) -> Self {
        self.anonymous_username = Some(username.into());
        self
    }

    /// Enables table-level grant enforcement for non-super-users. Off by default:
    /// without it, any authenticated identity may read any table.
    pub fn with_auth_enforcement(mut self, enforce: bool) -> Self {
        self.auth_enforce = enforce;
        self
    }

    pub async fn build(mut self) -> anyhow::Result<Runtime> {
        let runtime_handle = match &self.runtime_handle {
            Some(handle) => handle.clone(),
            None => Handle::try_current().map_err(|_| {
                anyhow::anyhow!("No current tokio runtime handle; please provide one")
            })?,
        };

        // The planner's late-filled weak handle to the session. Created here so both the auth store
        // and the query planner share the same cell; it is filled once the session exists.
        let session_cell = new_session_cell();

        // Built before the session so it can be published as a session extension: the `AuthExec`
        // node recovers it from there to apply auth DDL. Unhydrated — the db-backed store
        // reaches its tables through `session_cell`, which does not yet point anywhere.
        let AuthSetup {
            context: auth_context,
            store: auth_store,
        } = init_auth_context(&self, session_cell.clone()).await?;
        let auth_context = Arc::new(auth_context);

        let session_ctx = init_session_ctx(&self, auth_context.clone(), session_cell)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize session context: {:?}", e))?;

        // The session and its tables now exist and the session cell is filled, so the auth store can
        // reach its tables. Ensure they exist, hydrate the in-memory user/role copies from whatever
        // is already there (spawning from an already-populated store across a restart), then
        // bootstrap. All idempotent, so this is safe on every start.
        auth_store.ensure_tables().await?;
        auth_context.hydrate().await?;
        bootstrap_auth(&self, &auth_context).await?;

        // Register File Formats
        let file_formats = register_file_formats(&self, &session_ctx)?;
        // Register UDFs and Table Functions, returning their docs (only for udtfs) for cataloging. The functions are registered on the session context.
        let table_function_docs = register_functions(
            session_ctx.clone(),
            runtime_handle.clone(),
            file_formats.clone(),
        );

        let crawler_manager = init_crawler_manager(&self, &session_ctx, file_formats).await?;

        // Event-driven external-table refresh was removed; external tables become
        // current on an explicit `REFRESH` only.

        let tmp_dir = self.tmp_dir_path.unwrap_or_else(temp_dir);
        // Best-effort cleanup of query-output files a previous process crashed
        // before their `TempObject` could delete them. The happy path is RAII.
        crate::query::temp_object::sweep_stale_outputs(&tmp_dir);

        Ok(Runtime {
            session_ctx,
            table_function_docs,
            query_metrics: Arc::new(Mutex::new(HashMap::new())),
            crawler_manager,
            auth: auth_context,
            auth_enforce: self.auth_enforce,

            tmp_dir,
        })
    }
}

/// Builds the crawler manager, loads persisted crawlers and starts their triggers,
/// then publishes it through the session-extension handle so `CREATE/RUN/DROP
/// CRAWLER` can reach it. Returns `None` when the crawler subsystem is disabled.
async fn init_crawler_manager(
    builder: &RuntimeBuilder,
    session_ctx: &Arc<SessionContext>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
) -> anyhow::Result<Option<Arc<CrawlerManager>>> {
    if !builder.crawler.enable {
        return Ok(None);
    }

    let crawler_manager = CrawlerManager::new(
        session_ctx.clone(),
        file_formats,
        DEFAULT_DB_STORE_URL_OBJECT_URL.clone(),
        builder.crawler.clone(),
    );
    crawler_manager.init().await?;

    // The handle registered on the session holds a Weak, so the runtime keeps the
    // only strong reference and dropping it releases the session context. The
    // extension is keyed on the inner type: `CrawlerManagerHandle` is itself an Arc.
    if let Some(handle) = session_ctx
        .state()
        .config()
        .get_extension::<OnceLock<Weak<CrawlerManager>>>()
    {
        let _ = handle.set(Arc::downgrade(&crawler_manager));
    }

    Ok(Some(crawler_manager))
}

/// The auth context plus the tables-backed store that has to be brought online once the session
/// (and its tables) exist. Kept separate so `build` can drive the ordering: build unhydrated →
/// create session → ensure tables → hydrate → bootstrap.
struct AuthSetup {
    context: AuthContext,
    store: Arc<TablesAuthStore>,
}

/// Builds the auth context around the tables-backed store, **unhydrated**.
///
/// The store persists users, roles and grants in the internal managed tables and reaches them
/// through `session_cell` — which is filled only after the session exists — so the in-memory copies
/// stay empty until `build` hydrates them. Roles are always tables-backed; the user store is
/// tables-backed only for the default Basic provider (a custom provider, e.g. OIDC, owns its own
/// user directory, which beacon does not persist).
///
/// The config-defined super-user is set here (it is never stored in the tables — the only way to be
/// a super-user is the `BEACON_ADMIN_*` credential), and anonymous access is recorded so
/// `anonymous_enabled` reflects config immediately; the anonymous *user* is seeded in
/// [`bootstrap_auth`], after the tables are reachable.
async fn init_auth_context(
    builder: &RuntimeBuilder,
    session_cell: SessionCell,
) -> anyhow::Result<AuthSetup> {
    let store = Arc::new(TablesAuthStore::new(session_cell));
    let role_provider = RoleProvider::with_store(store.clone() as Arc<dyn RoleStore>);

    let provider: Arc<dyn beacon_auth::AuthProvider> = match &builder.auth_provider {
        Some(auth_provider) => auth_provider.clone(),
        None => {
            let user_store = Arc::new(InMemoryUserStore::with_store(
                store.clone() as Arc<dyn UserDirectory>
            ));
            Arc::new(BasicAuthProvider::with_user_store(user_store))
        }
    };

    let mut context = AuthContext::with_role_provider(provider, role_provider);

    if let (Some(username), Some(password)) = (&builder.admin_username, &builder.admin_password) {
        context.set_super_user(username, password);
    }

    if let Some(anonymous) = &builder.anonymous_username {
        context.set_anonymous_user(anonymous);
    }

    Ok(AuthSetup { context, store })
}

/// Idempotent bootstrap, run on every start once the tables are hydrated: seed the anonymous
/// principal (password-less, roleless) when anonymous access is enabled. Admins can later grant it
/// read-only roles via `GRANT ROLE <role> TO USER <anonymous>`. Re-running against a populated store
/// is a no-op (the user already exists in the hydrated copy).
async fn bootstrap_auth(builder: &RuntimeBuilder, auth: &Arc<AuthContext>) -> anyhow::Result<()> {
    if let Some(anonymous) = &builder.anonymous_username {
        if !auth.user_exists(anonymous).await {
            auth.create_user(anonymous, "").await?;
        }
    }
    Ok(())
}

/// Builds the session state, including the runtime environment, and returns a new session context. The
/// session cell is filled with a weak reference to the session context so that the auth store and
/// query planner can reach it once it exists.
///
/// Functions and File Formats ARE NOT REGISTERED HERE. This is done in `register_functions` after the session context is built.
async fn init_session_ctx(
    builder: &RuntimeBuilder,
    auth_context: Arc<AuthContext>,
    session_cell: SessionCell,
) -> anyhow::Result<Arc<SessionContext>> {
    let secrets_store = Arc::new(SecretStore::new_with_master_key(
        builder.secrets_encryption_key,
    ));

    let db_store: Arc<dyn ObjectStore> = match &builder.db_path {
        Some(db_path) => Arc::new(RedbStore::open(db_path)?),
        None => Arc::new(object_store::memory::InMemory::new()),
    };

    let config = build_session_config(
        builder,
        secrets_store.clone(),
        auth_context,
        db_store.clone(),
    )?;
    let runtime_env = runtime_env_builder(builder, secrets_store.clone())?;

    let session_state = build_session_state(builder, config, runtime_env, session_cell.clone())?;

    let session_ctx = Arc::new(SessionContext::new_with_state(session_state));

    // The query planner holds this cell (a Weak, to avoid a cycle through the
    // session state it is registered on) and upgrades it when executing a
    // statement plan. Fill it as soon as the context exists.
    let _ = session_cell.set(Arc::downgrade(&session_ctx));

    // Register db the object store for storing tables and managed datasets. This is the store the `db://` scheme resolves against.
    session_ctx.register_object_store(DEFAULT_DB_STORE_URL_OBJECT_URL.as_ref(), db_store.clone());
    // Register optionally a default store for resolving dataset paths. This is the store the `datasets://` scheme resolves against.
    match (&builder.default_store_url, &builder.default_store) {
        (Some(url), Some(store)) => {
            session_ctx.register_object_store(url.as_ref(), store.clone());
        }
        (None, None) => {
            // No default store is provided so the lazy object store registry will be used to resolve schemes dynamically.
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Both default_store_url and default_store must be provided together. Got default_store_url: {:?}, default_store: {:?}",
                builder.default_store_url,
                builder.default_store,
            ));
        }
    }
    // Register the tmp store for storing temporary query outputs. This is the store the `tmp://` scheme resolves against.
    let tmp_store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
        builder.tmp_dir_path.clone().unwrap_or_else(temp_dir),
    )?);
    session_ctx.register_object_store(TMP_STORE_URL_OBJECT_URL.as_ref(), tmp_store);

    Ok(session_ctx)
}

async fn register_schema_provider(
    _runtime_builder: &RuntimeBuilder,
    session_ctx: &Arc<SessionContext>,
) -> anyhow::Result<()> {
    let schema_provider = Arc::new(PersistentSchemaProvider::new(
        tokio::runtime::Handle::current(),
        session_ctx.clone(),
        DEFAULT_DB_STORE_URL_OBJECT_URL.clone(),
    ));

    session_ctx
        .catalog("beacon")
        .ok_or(anyhow::anyhow!("Failed to get catalog 'beacon'"))?
        .register_schema("public", schema_provider.clone())?;

    init_tables(
        &session_ctx,
        &schema_provider,
        &DEFAULT_DB_STORE_URL_OBJECT_URL,
    )
    .await?;

    Ok(())
}

fn register_file_formats(
    builder: &RuntimeBuilder,
    session_ctx: &Arc<SessionContext>,
) -> anyhow::Result<Vec<Arc<dyn FileFormatFactoryExt>>> {
    let state_ref = session_ctx.state_ref();
    let mut state = state_ref.write();

    let formats: Vec<Arc<dyn FileFormatFactoryExt>> = vec![
        Arc::new(ParquetFormatFactory),
        Arc::new(CsvFormatFactory),
        Arc::new(ArrowFormatFactory),
        Arc::new(TiffFormatFactory::new(Default::default())),
        Arc::new(ZarrFormatFactory),
        Arc::new(BBFFormatFactory::new(Default::default())),
        Arc::new(GeoParquetFormatFactory::default()),
    ];
    for format in &formats {
        state.register_file_format(format.clone(), true)?;
    }

    Ok(formats)
}

fn build_session_state(
    builder: &RuntimeBuilder,
    session_config: SessionConfig,
    runtime_env: Arc<RuntimeEnv>,
    session_cell: SessionCell,
) -> anyhow::Result<datafusion::execution::context::SessionState> {
    let mut optimizer_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![];
    // This is DataFusion's default logical rule set with `FederationOptimizerRule`
    // inserted, so replacing the defaults with it is intentional: sub-plans rooted
    // at remote tables get pushed down. The matching `FederatedPlanner` lives in
    // `BeaconQueryPlanner`'s extension planners.
    optimizer_rules.extend(datafusion_federation::default_optimizer_rules());

    let mut state_builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .with_optimizer_rules(optimizer_rules)
        .with_query_planner(Arc::new(BeaconQueryPlanner::new(session_cell.clone())));

    // Opt-in nd-pipeline optimizer. Appended to (never replacing) the default
    // physical rules: it must see the planned `ProjectionExec` above
    // `NdBroadcastExec`, and dropping the defaults would remove
    // `EnforceDistribution` — without which a Final aggregate never merges its
    // partitions and `count(*)` returns one row per file group.
    if builder.nd_pipeline {
        state_builder =
            state_builder.with_physical_optimizer_rule(Arc::new(NdProjectionPushdown::new()));
    }

    Ok(state_builder.build())
}

fn build_session_config(
    builder: &RuntimeBuilder,
    secrets_store: Arc<SecretStore>,
    auth_context: Arc<AuthContext>,
    db_store: Arc<dyn ObjectStore>,
) -> anyhow::Result<SessionConfig> {
    // Plan-time code reaches these settings through the SessionConfig extensions,
    // where no `Runtime` handle is available.
    let mut config = SessionConfig::new()
        // Beacon's tables live in `beacon.public`; the schema is later replaced by
        // the PersistentSchemaProvider.
        .with_default_catalog_and_schema("beacon", "public")
        .with_batch_size(builder.batch_size.unwrap_or(64 * 1024))
        .with_coalesce_batches(true)
        .with_target_partitions(builder.vm_cpu_limit.unwrap_or(num_cpus::get()))
        .with_information_schema(true)
        .with_collect_statistics(true)
        .with_spill_compression(datafusion::config::SpillCompression::Lz4Frame)
        // Recovered by the `AuthExec` node to apply auth DDL (CREATE USER/ROLE, GRANT, ...).
        .with_extension(auth_context)
        // Routes managed Lance tables through beacon's tables store. Per-runtime (no
        // process globals), so managed-table CRUD stays isolated.
        .with_extension(Arc::new(beacon_lance::LanceWarehouse::new(
            db_store.clone(),
        )))
        // Late-filled by `init_crawler_manager` once the data lake and tables exist.
        .with_extension(new_crawler_manager_handle())
        .with_extension(secrets_store.clone())
        .with_extension(Arc::new(ArrowTypeWidening::new(
            builder
                .type_widening
                .clone()
                .unwrap_or_else(|| Arc::new(DefaultArrowTypeWidening)),
        )))
        // The URLs plan-time code resolves against (datasets / tables / tmp), each
        // the same URL its store above is registered under.
        // .with_extension(Arc::new(urls.clone()))
        // The store a `CREATE EXTERNAL TABLE` LOCATION is resolved against. Built
        // from the datasets URL, not the local filesystem: a bare `LOCATION 'obs/'`
        // means "in the datasets store", and resolving it against `file://` silently
        // yields an empty table instead of an error.
        .with_extension(Arc::new(ListingTableFactoryExt))
        // Recovered by the JSON query compiler (default table, projection pushdown).
        .with_extension(Arc::new(builder.sql.clone()))
        // Recovered when a statement's result stream is built, to merge the small
        // batches a plan emits into client-sized ones.
        .with_extension(Arc::new(CoalesceSqlStream::new(
            builder.sql.stream_coalesce,
        )));

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
    config.options_mut().optimizer.expand_views_at_output = true;
    config.options_mut().sql_parser.map_string_types_to_utf8view = false;

    Ok(config)
}

/// Default query memory pool size: 80% of usable RAM.
///
/// Under a cgroup (any containerized deployment) `/proc/meminfo` still reports
/// the host's RAM, so the cgroup limit takes precedence where one is set.
fn default_vm_memory_limit() -> usize {
    const DEFAULT_MEMORY_LIMIT_FRACTION: f64 = 0.8;
    const FALLBACK_MEMORY_LIMIT: usize = 8 * 1024 * 1024 * 1024;

    let system = sysinfo::System::new_with_specifics(
        sysinfo::RefreshKind::nothing()
            .with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()),
    );

    let total_memory = system
        .cgroup_limits()
        .map(|limits| limits.total_memory.min(system.total_memory()))
        .unwrap_or_else(|| system.total_memory());

    if total_memory == 0 {
        tracing::warn!(
            "could not determine usable memory, defaulting query memory pool to {} bytes",
            FALLBACK_MEMORY_LIMIT
        );
        return FALLBACK_MEMORY_LIMIT;
    }

    let limit = (total_memory as f64 * DEFAULT_MEMORY_LIMIT_FRACTION) as u64;
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);

    tracing::info!(
        "query memory pool defaulting to {} bytes ({:.0}% of {} bytes usable memory)",
        limit,
        DEFAULT_MEMORY_LIMIT_FRACTION * 100.0,
        total_memory
    );

    limit
}

fn runtime_env_builder(
    builder: &RuntimeBuilder,
    secrets_store: Arc<SecretStore>,
) -> anyhow::Result<Arc<RuntimeEnv>> {
    // An injected default store is *not* registered here: it is registered by
    // `register_object_stores` under the configured datasets URL, so queries, the
    // crawler and `init_tables` all address the same object.
    let object_store_registry = LazyObjectStoreRegistry::new(secrets_store);

    let runtime_env_builder = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(DiskManagerBuilder::default())
        .with_temp_file_path(builder.tmp_dir_path.clone().unwrap_or_else(temp_dir))
        .with_memory_pool(Arc::new(FairSpillPool::new(
            builder
                .vm_memory_limit
                .unwrap_or_else(default_vm_memory_limit),
        )))
        .with_cache_manager(CacheManagerConfig {
            table_files_statistics_cache: Some(Arc::new(BeaconFileStatisticsCache::default())),
            list_files_cache_limit: 0,
            ..Default::default()
        })
        .with_object_store_registry(Arc::new(object_store_registry));

    Ok(Arc::new(runtime_env_builder.build()?))
}
