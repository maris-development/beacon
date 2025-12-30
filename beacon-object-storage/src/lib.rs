use std::{fmt::Display, path::PathBuf, process::exit, sync::Arc};

use futures::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
    aws::{AmazonS3, AmazonS3Builder},
    local::LocalFileSystem,
    path::Path,
};

pub mod error;
pub mod event;
pub mod event_handlers;
pub mod notify;
pub mod object_cache;

static DATASETS_OBJECT_STORE: tokio::sync::OnceCell<Arc<BeaconObjectStore>> =
    tokio::sync::OnceCell::const_new();
static TABLES_OBJECT_STORE: tokio::sync::OnceCell<Arc<dyn ObjectStore>> =
    tokio::sync::OnceCell::const_new();
static TMP_OBJECT_STORE: tokio::sync::OnceCell<Arc<dyn ObjectStore>> =
    tokio::sync::OnceCell::const_new();

pub async fn get_datasets_object_store() -> Arc<BeaconObjectStore> {
    BeaconObjectStore::datasets().await
}
pub async fn get_tables_object_store() -> Arc<dyn ObjectStore> {
    BeaconObjectStore::tables().await
}
pub async fn get_tmp_object_store() -> Arc<dyn ObjectStore> {
    BeaconObjectStore::tmp().await
}

#[derive(Debug, Clone)]
pub struct BeaconObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl BeaconObjectStore {
    pub async fn datasets() -> Arc<BeaconObjectStore> {
        match DATASETS_OBJECT_STORE
            .get_or_try_init(|| async { Ok::<_, String>(Arc::new(BeaconObjectStore::new()?)) })
            .await
        {
            Ok(store) => store.clone(),
            Err(e) => {
                tracing::error!("Failed to initialize global object store: {}. Exiting.", e);
                exit(1);
            }
        }
    }

    fn new() -> Result<Self, String> {
        if beacon_config::CONFIG.s3_data_lake {
            tracing::info!("Using S3 object store for datasets");
            let s3_store = Self::s3_object_store()?;
            let inner: Arc<dyn ObjectStore> = Arc::new(s3_store);
            Ok(BeaconObjectStore { inner })
        } else {
            tracing::info!("Using LocalFileSystem object store for datasets");
            let local_store = Self::local_fs_object_store();
            let inner: Arc<dyn ObjectStore> = Arc::new(local_store);
            Ok(BeaconObjectStore { inner })
        }
    }

    fn s3_object_store() -> Result<AmazonS3, String> {
        let bucket_name = beacon_config::CONFIG
            .s3_bucket
            .as_ref()
            .ok_or("S3 bucket name not configured".to_string())?;
        let builder = AmazonS3Builder::from_env()
            .with_allow_http(true)
            .with_bucket_name(bucket_name);

        builder
            .build()
            .map_err(|e| format!("Failed to build S3 object store: {}", e))
    }

    fn local_fs_object_store() -> LocalFileSystem {
        let path = PathBuf::from("./data/datasets");
        LocalFileSystem::new_with_prefix(path).expect("Failed to create LocalFileSystem")
    }

    pub async fn tables() -> Arc<dyn ObjectStore> {
        let tables_path = PathBuf::from("./data/tables");

        match TABLES_OBJECT_STORE
            .get_or_try_init(|| async {
                // Initialize the global object store
                Ok::<_, String>(Arc::new(
                    LocalFileSystem::new_with_prefix(tables_path)
                        .expect("Failed to create LocalFileSystem with prefix for tables"),
                ) as Arc<dyn ObjectStore>)
            })
            .await
        {
            Ok(store) => store.clone(),
            Err(e) => {
                tracing::error!("Failed to initialize global object store: {}. Exiting.", e);
                exit(1);
            }
        }
    }

    pub async fn tmp() -> Arc<dyn ObjectStore> {
        let tmp_path = PathBuf::from("./data/tmp");

        match TMP_OBJECT_STORE
            .get_or_try_init(|| async {
                // Initialize the global object store
                Ok::<_, String>(Arc::new(
                    LocalFileSystem::new_with_prefix(tmp_path)
                        .expect("Failed to create LocalFileSystem with prefix for tmp"),
                ) as Arc<dyn ObjectStore>)
            })
            .await
        {
            Ok(store) => store.clone(),
            Err(e) => {
                tracing::error!("Failed to initialize global object store: {}. Exiting.", e);
                exit(1);
            }
        }
    }
}

impl Display for BeaconObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BeaconObjectStore")
    }
}

impl BeaconObjectStore {}

#[async_trait::async_trait]
impl ObjectStore for BeaconObjectStore {
    /// Save the provided `payload` to `location` with the given options
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
