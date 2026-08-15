//! The Beacon server: the application layer over the beacon-db engine.
//!
//! A [`Server`] owns the datasets object store and the files in it, and hosts a
//! [`Runtime`] as the processing unit that queries them. The split is deliberate:
//!
//! - **beacon-core** is an embedded database. It authenticates a caller and runs
//!   a query; it does not know where files come from or how they get there.
//! - **beacon-server** is the application. It decides where the data lives,
//!   moves files in and out of that store, and hands the same store to the runtime
//!   as its default store — so a file uploaded here is immediately queryable
//!   through the very same object store instance, with no second view of it.
//!
//! Everything the transports need hangs off this type: the config, the store, and
//! the runtime.

pub mod catalog;
pub mod files;
pub mod sql;
pub mod sys;
pub mod uploads;

use std::sync::Arc;

use anyhow::Context;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::settings::{SqlSettings, SqlStreamCoalesceSettings};
use beacon_datafusion_ext::listing_factory::RootStore;
use beacon_server_config::Config;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;

/// The URL scheme bare dataset paths resolve against inside the runtime.
pub const DATASETS_STORE_URL: &str = "datasets://";

/// A running server: a datasets store, the files in it, and the runtime that
/// queries them.
pub struct Server {
    config: Arc<Config>,
    /// The datasets store. The same instance is registered on the runtime, so an
    /// upload through [`Self::store`] is visible to the next query with no cache
    /// or listing to invalidate between them.
    store: Arc<dyn ObjectStore>,
    runtime: Arc<Runtime>,
    /// Chunked uploads in flight. In memory: a multipart write cannot outlive the
    /// process that opened it.
    uploads: uploads::UploadSessions,
}

impl Server {
    /// Open the server described by `config`: build the datasets store, then
    /// start a runtime over it.
    ///
    /// A server is always persistent — the tables store is the single redb file at
    /// `config.data.db_file`. To get throwaway state, point `config` at a
    /// temporary directory; there is no separate in-memory mode.
    pub async fn open(config: Arc<Config>) -> anyhow::Result<Self> {
        let (store, root) = build_datasets_store(&config)?;
        let store_url =
            ObjectStoreUrl::parse(DATASETS_STORE_URL).context("invalid datasets store URL")?;

        let runtime = build_runtime(&config, store_url, root, store.clone())
            .await
            .context("failed to start the beacon runtime")?;

        Ok(Self {
            config,
            store,
            runtime: Arc::new(runtime),
            uploads: uploads::UploadSessions::default(),
        })
    }

    /// Host and build information for `GET /api/info`.
    pub fn system_info(&self) -> sys::SystemInfo {
        sys::SystemInfo::new(self.config.runtime.enable_sys_info)
    }

    /// The cap on a single upload, in bytes. `0` means unlimited.
    fn max_upload_bytes(&self) -> u64 {
        self.config.server.max_upload_bytes
    }

    /// Validate a caller-supplied dataset key.
    ///
    /// Path safety only — the file type is not restricted. A server holds whatever
    /// the operator puts in it, and a file beacon cannot read today it may read
    /// tomorrow.
    fn dataset_path(&self, path: &str) -> Result<object_store::path::Path, files::FileError> {
        files::validate_dataset_path(path)
    }

    /// Stream a file into the datasets store.
    pub async fn upload_dataset<S>(
        &self,
        path: &str,
        overwrite: bool,
        body: S,
    ) -> Result<files::UploadResult, files::FileError>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
    {
        let path = self.dataset_path(path)?;
        files::upload_dataset(
            self.store.as_ref(),
            &path,
            overwrite,
            self.max_upload_bytes(),
            body,
        )
        .await
    }

    /// Open a streaming read of a dataset file.
    pub async fn download_dataset(
        &self,
        path: &str,
    ) -> Result<object_store::GetResult, files::FileError> {
        // Read needs no extension check: anything already in the store is fair
        // game, and the path gate still applies.
        let path = files::validate_dataset_path(path)?;
        files::download_dataset(self.store.as_ref(), &path).await
    }

    /// Delete a dataset file.
    pub async fn delete_dataset(&self, path: &str) -> Result<(), files::FileError> {
        let path = files::validate_dataset_path(path)?;
        files::delete_dataset(self.store.as_ref(), &path).await
    }

    /// Begin a chunked upload, returning its session id and the part size clients
    /// should slice to.
    pub async fn initiate_dataset_upload(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<(uuid::Uuid, usize), files::FileError> {
        let path = self.dataset_path(path)?;
        if !overwrite {
            // Checked up front so a client is not told to upload gigabytes before
            // discovering the destination is taken.
            match object_store::ObjectStoreExt::head(self.store.as_ref(), &path).await {
                Ok(_) => return Err(files::FileError::AlreadyExists(path.to_string())),
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        let id = self
            .uploads
            .initiate(self.store.as_ref(), path, self.max_upload_bytes())
            .await?;
        Ok((id, uploads::PART_SIZE))
    }

    /// Append one part to a chunked upload.
    pub async fn upload_dataset_part(
        &self,
        id: uuid::Uuid,
        part_number: u32,
        bytes: bytes::Bytes,
    ) -> Result<(), files::FileError> {
        self.uploads.put_part(id, part_number, bytes).await
    }

    /// Finish a chunked upload.
    pub async fn complete_dataset_upload(
        &self,
        id: uuid::Uuid,
    ) -> Result<files::UploadResult, files::FileError> {
        self.uploads.complete(id).await
    }

    /// Discard a chunked upload.
    pub async fn abort_dataset_upload(&self, id: uuid::Uuid) -> Result<(), files::FileError> {
        self.uploads.abort(id).await
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

/// Build the datasets store: the bucket named by `BEACON_S3_BUCKET` when
/// `BEACON_S3_DATASETS` is set, otherwise the local `datasets/` directory.
///
/// Returns the store together with the [`RootStore`] describing the same data for
/// native readers (netCDF-c), which open by path/URL instead of going through the
/// object store. Both views must address the same bytes, so they are always
/// derived together here.
fn build_datasets_store(config: &Config) -> anyhow::Result<(Arc<dyn ObjectStore>, RootStore)> {
    let s3 = &config.s3;
    if !s3.datasets_on_s3 {
        let dir = config.data.datasets.clone();
        let store = object_store::local::LocalFileSystem::new_with_prefix(&dir)
            .with_context(|| format!("failed to open the datasets store at {}", dir.display()))?;
        return Ok((Arc::new(store), RootStore::FileSystem(dir)));
    }

    // `Config::load` rejects an S3 datasets store without a bucket, so this only fires for
    // a hand-built config.
    let bucket = s3
        .bucket
        .as_deref()
        .context("BEACON_S3_DATASETS is set but no bucket is configured")?;

    // Credentials, endpoint and region come from the standard AWS environment
    // chain; only the bucket and addressing are Beacon's own settings.
    let mut builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_allow_http(s3.allow_http);
    if s3.enable_virtual_hosting {
        builder = builder.with_virtual_hosted_style_request(true);
    }
    let store = builder
        .build()
        .with_context(|| format!("failed to open the S3 datasets store for bucket '{bucket}'"))?;

    let base = s3
        .native_base_url()
        .context("failed to derive the S3 datasets base URL")?;
    tracing::info!(bucket, base_url = %base, "datasets store backed by S3");

    Ok((Arc::new(store), RootStore::HttpsStore(base)))
}

/// Map the application config onto a runtime.
///
/// This is the single place `beacon_server_config::Config` meets `RuntimeBuilder`: the
/// runtime takes no config of its own, so every setting it needs is threaded
/// through here explicitly.
async fn build_runtime(
    config: &Config,
    store_url: ObjectStoreUrl,
    root: RootStore,
    store: Arc<dyn ObjectStore>,
) -> anyhow::Result<Runtime> {
    let mut builder = RuntimeBuilder::new()
        .with_runtime_handle(tokio::runtime::Handle::current())
        // The store the server owns; the root is what native readers (netCDF-c)
        // translate object paths against.
        .with_default_store(store_url, root)
        .with_default_object_store(store)
        .with_tmp_dir_path(config.data.tmp.clone())
        .with_admin_credentials(config.admin.username.clone(), config.admin.password.clone())
        .with_auth_enforcement(config.auth.enforce)
        .with_batch_size(config.runtime.batch_size)
        // `BEACON_VM_MEMORY_SIZE` is megabytes (default 8192 = 8 GiB); the
        // builder takes bytes. Passing it through unconverted yields an 8 KB
        // query pool, which fails the first ORDER BY that needs to sort.
        .with_vm_memory_limit(config.runtime.vm_memory_size.saturating_mul(1024 * 1024))
        .with_crawler(config.crawler.clone())
        .with_file_stats(config.file_stats.clone())
        .with_netcdf_config(config.netcdf.clone())
        .with_hdf5_config(config.hdf5.clone())
        .with_zarr_config(config.zarr.clone())
        .with_atlas_config(config.atlas.clone())
        .with_bbf_config(config.bbf.clone())
        .with_lance_config(config.lance.clone())
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

    // The single-file tables store: catalog, managed data, and the auth directory.
    builder = builder.with_db_path(config.data.db_file.clone());

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
