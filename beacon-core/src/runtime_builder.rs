use std::{collections::HashMap, path::PathBuf, sync::Arc};

use beacon_arrow_bbf::datafusion::BBFFormatFactory;
use beacon_arrow_csv::datafusion::CsvFormatFactory;
use beacon_arrow_geoparquet::datafusion::GeoParquetFormatFactory;
use beacon_arrow_ipc::datafusion::ArrowFormatFactory;
use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
use beacon_arrow_tiff::datafusion::TiffFormatFactory;
use beacon_arrow_zarr::datafusion::ZarrFormatFactory;
use beacon_auth::{AuthContext, BasicAuthProvider};
use beacon_data_lake::{init_tables, PersistentSchemaProvider, DB_OBJECT_STORE_URL};
use beacon_datafusion_ext::{
    format_ext::FileFormatFactoryExt,
    listing_table_factory_ext::ListingTableFactoryExt,
    nd::NdProjectionPushdown,
    object_store_registry::LazyObjectStoreRegistry,
    secrets::SecretStore,
    stats_cache::BeaconFileStatisticsCache,
    type_widening::{ArrowTypeWidening, ArrowTypeWideningStrategy, DefaultArrowTypeWidening},
};
use beacon_functions::register_functions;
use datafusion::{
    execution::{
        cache::cache_manager::CacheManagerConfig,
        disk_manager::DiskManagerBuilder,
        memory_pool::FairSpillPool,
        object_store::{ObjectStoreRegistry, ObjectStoreUrl},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
        SessionStateBuilder,
    },
    optimizer::OptimizerRule,
    physical_optimizer,
    prelude::{SessionConfig, SessionContext},
};
use object_store::ObjectStore;
use parking_lot::Mutex;
use tempfile::env::temp_dir;
use tokio::runtime::Handle;

use crate::{
    runtime::Runtime,
    statement_plan::{new_session_cell, BeaconQueryPlanner, SessionCell},
};

pub struct RuntimeBuilder {
    pub runtime_handle: Option<Handle>,
    pub db_path: Option<PathBuf>,
    pub default_store_url: Option<ObjectStoreUrl>,
    pub default_store: Option<Arc<dyn ObjectStore>>,

    pub admin_username: Option<String>,
    pub admin_password: Option<String>,

    pub tmp_dir_path: Option<PathBuf>,
    pub vm_memory_limit: Option<usize>,
    pub vm_cpu_limit: Option<usize>,

    pub batch_size: Option<usize>,
    pub nd_pipeline: bool,

    pub crawler_default_interval_secs: Option<u64>,

    pub netcdf_reader_cache_size: Option<usize>,
    pub netcdf_statistics: bool,

    pub auth_provider: Option<Arc<dyn beacon_auth::AuthProvider>>,
    pub secrets_encryption_key: Option<[u8; 32]>,

    pub type_widening: Option<Arc<dyn ArrowTypeWideningStrategy>>,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn new_with_path(db_path: PathBuf) -> Self {
        Self {
            db_path: Some(db_path),
            ..Default::default()
        }
    }

    pub fn with_runtime_handle(mut self, handle: Handle) -> Self {
        self.runtime_handle = Some(handle);
        self
    }

    pub fn with_db_path(mut self, db_path: PathBuf) -> Self {
        self.db_path = Some(db_path);
        self
    }

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

    pub fn with_crawler(mut self, default_interval_secs: Option<u64>) -> Self {
        self.crawler_default_interval_secs = default_interval_secs;
        self
    }

    pub fn with_netcdf_reader_cache(mut self, cache_size: usize) -> Self {
        self.netcdf_reader_cache_size = Some(cache_size);
        self
    }

    pub fn with_netcdf_statistics(mut self) -> Self {
        self.netcdf_statistics = true;
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

    pub async fn build(self) -> anyhow::Result<Runtime> {
        let runtime_handle = match &self.runtime_handle {
            Some(handle) => handle.clone(),
            None => Handle::try_current().map_err(|_| {
                anyhow::anyhow!("No current tokio runtime handle; please provide one")
            })?,
        };

        let session_ctx = init_session_ctx(&self, runtime_handle)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize session context: {:?}", e))?;

        let listing_table_factory = Arc::new(ListingTableFactoryExt::new(
            self.default_store_url
                .clone()
                .unwrap_or(ObjectStoreUrl::local_filesystem()),
            Arc::downgrade(&session_ctx),
        ));

        let auth_context = Arc::new(init_auth_context(&self).await?);

        Ok(Runtime {
            session_ctx,
            listing_table_factory,
            table_function_docs: vec![],
            query_metrics: Arc::new(Mutex::new(HashMap::new())),
            crawler_manager: None,
            auth: auth_context,

            tmp_dir: self.tmp_dir_path.unwrap_or_else(temp_dir),
        })
    }
}

async fn init_auth_context(builder: &RuntimeBuilder) -> anyhow::Result<AuthContext> {
    if let Some(auth_provider) = &builder.auth_provider {
        // Initialize the auth provider with the session context if needed
        // For example, you might want to set up any necessary state or configuration here
        Ok(AuthContext::new(auth_provider.clone()))
    } else {
        Ok(AuthContext::new(Arc::new(BasicAuthProvider::new())))
    }
}

async fn init_session_ctx(
    builder: &RuntimeBuilder,
    tokio_handle: Handle,
) -> anyhow::Result<Arc<SessionContext>> {
    let secrets_store = Arc::new(SecretStore::new());

    let config = build_session_config(builder, secrets_store.clone())?;
    let runtime_env = runtime_env_builder(builder, secrets_store.clone())?;

    let session_cell = new_session_cell();
    let session_state = build_session_state(builder, config, runtime_env, session_cell.clone())?;

    let session_ctx = Arc::new(SessionContext::new_with_state(session_state));

    let file_formats = register_file_formats(&session_ctx)?;

    register_functions(
        session_ctx.clone(),
        tokio_handle,
        file_formats.clone(),
        builder
            .default_store_url
            .clone()
            .unwrap_or_else(ObjectStoreUrl::local_filesystem),
    );

    let schema_provider = Arc::new(PersistentSchemaProvider::new(
        tokio::runtime::Handle::current(),
        session_ctx.clone(),
        DB_OBJECT_STORE_URL.clone(),
    ));

    session_ctx
        .catalog("beacon")
        .ok_or(anyhow::anyhow!("Failed to get catalog 'beacon'"))?
        .register_schema("public", schema_provider.clone())?;

    init_tables(&session_ctx, &schema_provider).await?;

    Ok(session_ctx)
}

fn register_file_formats(
    session_ctx: &Arc<SessionContext>,
) -> anyhow::Result<Vec<Arc<dyn FileFormatFactoryExt>>> {
    let state_ref = session_ctx.state_ref();
    let mut state = state_ref.write();

    let formats: Vec<Arc<dyn FileFormatFactoryExt>> = vec![
        Arc::new(ParquetFormatFactory),
        Arc::new(CsvFormatFactory),
        Arc::new(ArrowFormatFactory),
        // Arc::new(NetCDFFormatFactory::new(
        //     datasets_object_store.clone(),
        //     NetcdfOptions::default(),
        //     config.netcdf.clone(),
        // )),
        // Arc::new(AtlasFormatFactory::new(
        //     datasets_object_store.clone(),
        //     AtlasOptions::default(),
        //     config.atlas.clone(),
        // )),
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
    optimizer_rules.extend(datafusion_federation::default_optimizer_rules());

    let mut physical_optimizer_rules: Vec<
        Arc<dyn physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
    > = vec![];

    if builder.nd_pipeline {
        physical_optimizer_rules.push(Arc::new(NdProjectionPushdown::new()));
    }

    let state_builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .with_optimizer_rules(optimizer_rules)
        .with_physical_optimizer_rules(physical_optimizer_rules)
        .with_query_planner(Arc::new(BeaconQueryPlanner::new(session_cell.clone())));

    Ok(state_builder.build())
}

fn build_session_config(
    builder: &RuntimeBuilder,
    secrets_store: Arc<SecretStore>,
) -> anyhow::Result<SessionConfig> {
    let mut config = SessionConfig::new()
        .with_batch_size(builder.batch_size.unwrap_or(64 * 1024))
        .with_coalesce_batches(true)
        .with_target_partitions(builder.vm_cpu_limit.unwrap_or(num_cpus::get()))
        .with_information_schema(true)
        .with_collect_statistics(true)
        .with_spill_compression(datafusion::config::SpillCompression::Lz4Frame)
        .with_extension(secrets_store.clone())
        .with_extension(Arc::new(ArrowTypeWidening::new(
            builder
                .type_widening
                .clone()
                .unwrap_or_else(|| Arc::new(DefaultArrowTypeWidening)),
        )))
        .with_extension(Arc::new(ListingTableFactoryExt));

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
    let object_store_registry = LazyObjectStoreRegistry::new(secrets_store);

    if let Some(default_store_url) = &builder.default_store_url {
        if let Some(default_store) = &builder.default_store {
            object_store_registry.register_store(default_store_url.as_ref(), default_store.clone());
        } else {
            anyhow::bail!("default_store_url is set but default_store is None");
        }
    }

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
