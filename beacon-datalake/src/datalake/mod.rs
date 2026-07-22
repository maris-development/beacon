//! The data lake: beacon's application layer.
//!
//! A [`DataLake`] owns the datasets object store and the files in it, and hosts a
//! [`Runtime`] as the processing unit that queries them. The split is deliberate:
//!
//! - **beacon-core** is an embedded database. It authenticates a caller and runs
//!   a query; it does not know where files come from or how they get there.
//! - **beacon-datalake** is the application. It decides where the data lives,
//!   moves files in and out of that store, and hands the same store to the runtime
//!   as its default store — so a file uploaded here is immediately queryable
//!   through the very same object store instance, with no second view of it.
//!
//! Everything the transports need hangs off this type: the config, the store, and
//! the runtime.

pub mod files;

use std::sync::Arc;

use anyhow::Context;
use beacon_datalake_config::Config;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::settings::{SqlSettings, SqlStreamCoalesceSettings};
use beacon_datafusion_ext::listing_factory::RootStore;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;

/// The URL scheme bare dataset paths resolve against inside the runtime.
pub const DATASETS_STORE_URL: &str = "datasets://";

/// A running data lake: a datasets store, the files in it, and the runtime that
/// queries them.
pub struct DataLake {
    config: Arc<Config>,
    /// The datasets store. The same instance is registered on the runtime, so an
    /// upload through [`Self::store`] is visible to the next query with no cache
    /// or listing to invalidate between them.
    store: Arc<dyn ObjectStore>,
    runtime: Arc<Runtime>,
}

impl DataLake {
    /// Open the data lake described by `config`: build the datasets store, then
    /// start a runtime over it.
    pub async fn open(config: Arc<Config>) -> anyhow::Result<Self> {
        let datasets_dir = config.data.datasets.clone();
        let store: Arc<dyn ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&datasets_dir).with_context(
                || format!("failed to open the datasets store at {}", datasets_dir.display()),
            )?,
        );
        let store_url = ObjectStoreUrl::parse(DATASETS_STORE_URL)
            .context("invalid datasets store URL")?;

        let runtime = build_runtime(&config, store_url, datasets_dir, store.clone())
            .await
            .context("failed to start the beacon runtime")?;

        Ok(Self {
            config,
            store,
            runtime: Arc::new(runtime),
        })
    }

    /// The processing unit: authenticate a caller, then run queries.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// The application configuration.
    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// The datasets store files are read from and written to.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
}

/// Map the application config onto a runtime.
///
/// This is the single place `beacon_datalake_config::Config` meets `RuntimeBuilder`: the
/// runtime takes no config of its own, so every setting it needs is threaded
/// through here explicitly.
async fn build_runtime(
    config: &Config,
    store_url: ObjectStoreUrl,
    datasets_dir: std::path::PathBuf,
    store: Arc<dyn ObjectStore>,
) -> anyhow::Result<Runtime> {
    let mut builder = RuntimeBuilder::new()
        .with_runtime_handle(tokio::runtime::Handle::current())
        // The store the lake owns; the root is what native readers (netCDF-c)
        // translate object paths against.
        .with_default_store(store_url, RootStore::FileSystem(datasets_dir))
        .with_default_object_store(store)
        // The single-file tables store (catalog + managed data).
        .with_db_path(config.data.db_file.clone())
        .with_tmp_dir_path(config.data.tmp.clone())
        .with_admin_credentials(
            config.admin.username.clone(),
            config.admin.password.clone(),
        )
        .with_auth_enforcement(config.auth.enforce)
        .with_batch_size(config.runtime.batch_size)
        .with_vm_memory_limit(config.runtime.vm_memory_size)
        .with_crawler(config.crawler.clone())
        .with_netcdf_config(config.netcdf.clone())
        .with_sql_settings(SqlSettings {
            default_table: config.sql.default_table.clone(),
            enable_pushdown_projection: config.sql.enable_pushdown_projection,
            stream_coalesce: SqlStreamCoalesceSettings {
                enabled: config.sql.stream_coalesce.enabled,
                target_rows: config.sql.stream_coalesce.target_rows,
                flush_timeout_ms: config.sql.stream_coalesce.flush_timeout_ms,
                max_rows: config.sql.stream_coalesce.max_rows,
            },
        });

    if config.sql.enable_nd_pipeline {
        builder = builder.with_nd_pipeline();
    }
    if config.auth.anonymous_enabled {
        builder = builder.with_anonymous_user(ANONYMOUS_USERNAME);
    }
    if let Some(key) = config.secrets.master_key() {
        builder = builder.with_secrets_encryption(*key);
    }

    builder.build().await
}

/// The username unauthenticated requests resolve to when anonymous access is on.
pub const ANONYMOUS_USERNAME: &str = "anonymous";
