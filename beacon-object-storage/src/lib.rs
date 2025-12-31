//! Beacon object storage abstraction.
//!
//! This crate wraps implementations from [`object_store`] to provide a consistent
//! storage layer for Beacon.
//!
//! **Stores**
//! - **Datasets**: either local filesystem or S3-compatible storage (depending on config).
//! - **Tables** / **tmp**: local filesystem.
//!
//! **Optional event-driven caching**
//! When filesystem events are enabled for local datasets, we wrap the underlying
//! store in a [`NotifiedStore`] and attach a [`notify`] watcher.
//!
//! The watcher is best-effort:
//! - It should never panic the process.
//! - It should not turn transient filesystem races (e.g. delete-before-metadata)
//!   into hard failures.
//! - It only affects the in-memory cache used for listings; the underlying store
//!   remains the source of truth.
//!
//! **URL translation**
//! Beacon needs to provide URL strings to downstream libraries:
//! - [`DatasetsStore::object_url_path`] returns a general-purpose URL-like string
//!   for datasets (e.g. `file:///...` for local, and an HTTP(S) endpoint for S3).
//! - [`DatasetsStore::translate_netcdf_url_path`] returns a NetCDF-friendly URL:
//!   local datasets are returned as a plain filesystem path (no `file://`), while
//!   remote datasets are returned as an `http://`/`https://` URL with `#mode=bytes`
//!   appended for byte-range access.
//!
//! **S3 endpoint configuration**
//! When using S3, this crate relies on a configured endpoint for URL construction.
//! In this crate we currently read it from `AWS_ENDPOINT` (environment variable).
//! For virtual-hosted style, the bucket is expected to be part of the hostname
//! (e.g. `https://my-bucket.example.test`). For path-style, the bucket is appended
//! as a path segment (e.g. `https://example.test/my-bucket`).

use std::{env::temp_dir, fmt::Display, ops::Deref, path::PathBuf, sync::Arc};

use ::notify::{RecommendedWatcher, Watcher, recommended_watcher};
use futures::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, aws::AmazonS3Builder,
    local::LocalFileSystem, path::Path,
};

use crate::{
    event_handlers::fs::FileSystemEventHandler,
    notified_store::{NotifiedStore, StorageResult},
};

pub mod error;
pub mod event;
pub mod event_handlers;
pub mod notified_store;
pub mod object_cache;

/// Global datasets store.
///
/// Using a `OnceCell` ensures we build the store exactly once (important for
/// local FS watchers which must be kept alive for the lifetime of the process).
static DATASETS_OBJECT_STORE: tokio::sync::OnceCell<Arc<DatasetsStore>> =
    tokio::sync::OnceCell::const_new();
static TABLES_OBJECT_STORE: tokio::sync::OnceCell<Arc<dyn ObjectStore>> =
    tokio::sync::OnceCell::const_new();
static TMP_OBJECT_STORE: tokio::sync::OnceCell<Arc<dyn ObjectStore>> =
    tokio::sync::OnceCell::const_new();

pub async fn init_datastores() -> StorageResult<()> {
    let _ = get_datasets_object_store().await;
    let _ = get_tables_object_store().await;
    let _ = get_tmp_object_store().await;
    Ok(())
}

pub async fn get_datasets_object_store() -> Arc<DatasetsStore> {
    DATASETS_OBJECT_STORE
        .get_or_init(|| async {
            Arc::new(
                DatasetsStore::new()
                    .await
                    .expect("Failed to initialize datasets object store"),
            )
        })
        .await
        .clone()
}
pub async fn get_tables_object_store() -> Arc<dyn ObjectStore> {
    TABLES_OBJECT_STORE
        .get_or_init(|| async {
            // For tables, we always use the local filesystem
            Arc::new(
                LocalFileSystem::new_with_prefix(beacon_config::DATA_DIR.clone())
                    .expect("Failed to initialize tables object store"),
            ) as Arc<dyn ObjectStore>
        })
        .await
        .clone()
}
pub async fn get_tmp_object_store() -> Arc<dyn ObjectStore> {
    TMP_OBJECT_STORE
        .get_or_init(|| async {
            Arc::new(
                LocalFileSystem::new_with_prefix(temp_dir())
                    .expect("Failed to initialize tmp object store"),
            ) as Arc<dyn ObjectStore>
        })
        .await
        .clone()
}
/// The global datasets store.
///
/// This wrapper exists to:
/// - Select between local/S3 backends at startup.
/// - Optionally keep an event-driven cache warm via [`NotifiedStore`].
/// - Provide URL translation helpers for downstream consumers.
#[derive(Debug, Clone)]
pub struct DatasetsStore {
    inner: BeaconObjectStoreType,
    base: DatasetsStoreBase,
}

#[derive(Debug, Clone)]
enum DatasetsStoreBase {
    Local {
        root: PathBuf,
    },
    S3 {
        bucket: Option<String>,
        endpoint: String,
        is_virtual_hosted_style: bool,
    },
}

impl Display for DatasetsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            BeaconObjectStoreType::Default(store) => write!(f, "DatasetsStore wrapping: {}", store),
            BeaconObjectStoreType::Notified(store, _) => {
                write!(f, "DatasetsStore wrapping: {}", store)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum BeaconObjectStoreType {
    Default(Arc<dyn ObjectStore>),
    Notified(
        Arc<notified_store::NotifiedStore<Arc<dyn ObjectStore>>>,
        Option<Arc<RecommendedWatcher>>,
    ),
}

impl Deref for DatasetsStore {
    type Target = dyn ObjectStore;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            BeaconObjectStoreType::Default(store) => store.as_ref(),
            BeaconObjectStoreType::Notified(store, _) => store.as_ref(),
        }
    }
}

impl DatasetsStore {
    /// Build the datasets store using the process-wide Beacon configuration.
    ///
    /// - If `beacon_config::CONFIG.s3_data_lake` is true, initializes an S3 store.
    /// - Otherwise initializes a local filesystem store rooted at
    ///   `beacon_config::DATASETS_DIR_PATH`.
    pub(crate) async fn new() -> StorageResult<Self> {
        if beacon_config::CONFIG.s3_data_lake {
            tracing::info!("Using S3 object store for datasets");
            Ok(Self::new_s3().await?)
        } else {
            tracing::info!("Using LocalFileSystem object store for datasets");
            let local_store = Self::new_local().await?;
            Ok(local_store)
        }
    }

    async fn new_local() -> StorageResult<Self> {
        Self::new_local_at(
            beacon_config::DATASETS_DIR_PATH.clone(),
            beacon_config::CONFIG.enable_fs_events,
        )
        .await
    }

    async fn new_local_at(directory_path: PathBuf, enable_fs_events: bool) -> StorageResult<Self> {
        let root = directory_path.clone();
        let inner = if enable_fs_events {
            let local_store = LocalFileSystem::new_with_prefix(directory_path.clone())
                .map_err(|e| error::StorageError::InitializationError(format!("{}", e)))?;
            let notified_store =
                NotifiedStore::new(Arc::new(local_store) as Arc<dyn ObjectStore>).await;
            let arc_notified_store = Arc::new(notified_store);

            let watcher =
                Self::spawn_fs_watcher(directory_path.clone(), arc_notified_store.clone()).await?;
            // Keep the watcher alive by storing it in the store variant.
            BeaconObjectStoreType::Notified(arc_notified_store, Some(Arc::new(watcher)))
        } else {
            let local_store = LocalFileSystem::new_with_prefix(directory_path.clone())
                .map_err(|e| error::StorageError::InitializationError(format!("{}", e)))?;
            BeaconObjectStoreType::Default(Arc::new(local_store))
        };

        Ok(DatasetsStore {
            inner,
            base: DatasetsStoreBase::Local { root },
        })
    }

    async fn spawn_fs_watcher<O: ObjectStore>(
        directory_path: PathBuf,
        notified_store: Arc<NotifiedStore<O>>,
    ) -> StorageResult<RecommendedWatcher> {
        // NOTE: this watcher is an optimization. It updates the in-memory cache
        // used by `NotifiedStore` to avoid expensive rescans during list/lookup.
        // Failures should be handled robustly and must not panic the process.
        // We canonicalize the directory once and then try to translate event
        // paths relative to it. This is both cheaper and avoids relying on
        // canonicalization of files which may no longer exist (delete events).
        let abs_directory_path = std::fs::canonicalize(&directory_path).map_err(|e| {
            error::StorageError::InitializationError(format!(
                "Failed to canonicalize datasets directory {:?}: {}",
                directory_path, e
            ))
        })?;

        let (tx, rx) = flume::unbounded();

        let mut watcher = recommended_watcher(tx).map_err(|e| {
            error::StorageError::InitializationError(format!(
                "Failed to create filesystem watcher for {:?}: {}",
                directory_path, e
            ))
        })?;

        watcher
            .watch(&directory_path, ::notify::RecursiveMode::Recursive)
            .map_err(|e| {
                error::StorageError::InitializationError(format!(
                    "Failed to watch path {:?}: {}",
                    directory_path, e
                ))
            })?;

        std::thread::spawn(move || {
            // Keep the store alive and handle events synchronously. This is a
            // regular OS thread because `notify` callbacks are not async.
            let notified_store = notified_store.clone();
            while let Ok(event) = rx.recv() {
                // This can be very chatty in production; use debug-level.
                tracing::debug!("Filesystem event: {:?}", event);

                // Handle the event, e.g., update the object cache.
                let event = match event {
                    Ok(ev) => ev,
                    Err(e) => {
                        tracing::error!("Error receiving filesystem event: {:?}", e);
                        continue;
                    }
                };

                match &event.kind {
                    notify::EventKind::Create(_) => {
                        for obj_meta in event.paths.iter().filter_map(|path| {
                            match Self::object_meta_for_path(&abs_directory_path, path) {
                                Ok(Some(meta)) => Some(meta),
                                Ok(None) => None,
                                Err(e) => {
                                    tracing::error!(
                                        "Error translating created path to object meta: {}",
                                        e
                                    );
                                    None
                                }
                            }
                        }) {
                            let fs_event =
                                crate::event_handlers::fs::FileSystemEvent::Created(obj_meta);
                            if let Err(e) =
                                notified_store.handle_event::<FileSystemEventHandler, _>(fs_event)
                            {
                                tracing::error!("Error handling create event: {}", e);
                            }
                        }
                    }
                    notify::EventKind::Modify(_) => {
                        for obj_meta in event.paths.iter().filter_map(|path| {
                            match Self::object_meta_for_path(&abs_directory_path, path) {
                                Ok(Some(meta)) => Some(meta),
                                Ok(None) => None,
                                Err(e) => {
                                    tracing::error!(
                                        "Error translating modified path to object meta: {}",
                                        e
                                    );
                                    None
                                }
                            }
                        }) {
                            let fs_event =
                                crate::event_handlers::fs::FileSystemEvent::Modified(obj_meta);
                            if let Err(e) =
                                notified_store.handle_event::<FileSystemEventHandler, _>(fs_event)
                            {
                                tracing::error!("Error handling modify event: {}", e);
                            }
                        }
                    }
                    notify::EventKind::Remove(_) => {
                        // For remove events, the path may not exist anymore; do
                        // not attempt metadata lookup.
                        for location in event.paths.iter().filter_map(|path| {
                            match Self::object_location_for_path(&abs_directory_path, path) {
                                Ok(loc) => Some(loc),
                                Err(e) => {
                                    tracing::error!(
                                        "Error translating removed path to object location: {}",
                                        e
                                    );
                                    None
                                }
                            }
                        }) {
                            let fs_event =
                                crate::event_handlers::fs::FileSystemEvent::Deleted(location);
                            if let Err(e) =
                                notified_store.handle_event::<FileSystemEventHandler, _>(fs_event)
                            {
                                tracing::error!("Error handling delete event: {}", e);
                            }
                        }
                    }
                    _ => {}
                };
            }
        });

        Ok(watcher)
    }

    fn object_location_for_path(
        abs_directory_path: &std::path::Path,
        file_path: &std::path::Path,
    ) -> StorageResult<object_store::path::Path> {
        // Prefer using the reported path directly (works even if file is gone).
        // Fall back to canonicalization for cases where notify emits a path with
        // `..` segments.
        let rel_obj_path = match file_path.strip_prefix(abs_directory_path) {
            Ok(rel) => rel.to_path_buf(),
            Err(_) => {
                let abs_file_path = std::fs::canonicalize(file_path).map_err(|e| {
                    error::StorageError::InitializationError(format!(
                        "Failed to canonicalize file path {:?}: {}",
                        file_path, e
                    ))
                })?;
                abs_file_path
                    .strip_prefix(abs_directory_path)
                    .map_err(|e| {
                        error::StorageError::InitializationError(format!(
                            "Failed to get relative path for file {:?} with base {:?}: {}",
                            abs_file_path, abs_directory_path, e
                        ))
                    })?
                    .to_path_buf()
            }
        };

        Ok(object_store::path::Path::from(
            rel_obj_path.to_string_lossy().as_ref(),
        ))
    }

    fn object_meta_for_path(
        abs_directory_path: &std::path::Path,
        file_path: &std::path::Path,
    ) -> StorageResult<Option<ObjectMeta>> {
        let object_store_path = Self::object_location_for_path(abs_directory_path, file_path)?;

        let metadata = match std::fs::metadata(file_path) {
            Ok(m) => m,
            Err(e) => {
                // Racy but expected: the file could be removed/renamed between
                // event emission and metadata lookup. Treat this as a
                // non-fatal "no object".
                let _ = e;
                return Ok(None);
            }
        };

        if metadata.is_dir() {
            // We do not represent directories as objects.
            return Ok(None);
        }

        let modified_time = metadata.modified().map_err(|e| {
            error::StorageError::InitializationError(format!(
                "Failed to read modified time for path {:?}: {}",
                file_path, e
            ))
        })?;
        let datetime: chrono::DateTime<chrono::Utc> = modified_time.into();

        Ok(Some(ObjectMeta {
            location: object_store_path,
            size: metadata.len(),
            last_modified: datetime,
            e_tag: None,
            version: None,
        }))
    }

    async fn new_s3() -> StorageResult<Self> {
        let is_virtual_hosted_style = beacon_config::CONFIG.s3_enable_virtual_hosting;
        let mut builder = AmazonS3Builder::from_env()
            .with_allow_http(true)
            .with_virtual_hosted_style_request(is_virtual_hosted_style);

        let bucket_name = beacon_config::CONFIG.s3_bucket.as_ref();
        if !is_virtual_hosted_style {
            // Requires a bucket name to be specified in the environment.
            let bucket_name = bucket_name.ok_or(error::StorageError::InitializationError(
                "S3 bucket name not configured".to_string(),
            ))?;
            builder = builder.with_bucket_name(bucket_name);
        }

        let store = builder
            .build()
            .map_err(|e| error::StorageError::InitializationError(format!("{}", e)))?;

        // Get the endpoint used for URL translation.
        // We keep this in the store because downstream libraries (e.g. NetCDF)
        // often require HTTP/S URLs rather than `s3://...`.
        let endpoint =
            std::env::var("AWS_ENDPOINT").map_err(|e| error::StorageError::MissingEnvVar {
                var: "AWS_ENDPOINT",
                source: e,
            })?;
        if endpoint.trim().is_empty() {
            return Err(error::StorageError::InvalidConfig {
                key: "AWS_ENDPOINT",
                message: "must not be empty".to_string(),
            });
        }

        if beacon_config::CONFIG.enable_s3_events {
            let notified_store = NotifiedStore::new(Arc::new(store) as Arc<dyn ObjectStore>).await;

            return Ok(DatasetsStore {
                inner: BeaconObjectStoreType::Notified(Arc::new(notified_store), None),
                base: DatasetsStoreBase::S3 {
                    bucket: bucket_name.cloned(),
                    endpoint,
                    is_virtual_hosted_style,
                },
            });
        }

        Ok(DatasetsStore {
            inner: BeaconObjectStoreType::Default(Arc::new(store)),
            base: DatasetsStoreBase::S3 {
                bucket: bucket_name.cloned(),
                endpoint,
                is_virtual_hosted_style,
            },
        })
    }

    pub async fn push_event<H, I>(&self, input: I) -> StorageResult<()>
    where
        H: event::EventHandler<I>,
    {
        match &self.inner {
            BeaconObjectStoreType::Default(e) => Err(error::StorageError::EventHandlingError(
                format!("Event handling not enabled for this datasets store: {}.", e),
            )),
            BeaconObjectStoreType::Notified(store, _) => store.handle_event::<H, I>(input),
        }
    }

    /// Translate a given object path to a NetCDF-friendly URL/path.
    ///
    /// Behavior:
    /// - **Local datasets**: returns a plain filesystem path (no `file://`).
    /// - **S3 datasets**: returns an `http://`/`https://` URL and appends
    ///   `#mode=bytes` (required by some NetCDF libraries for byte-range access).
    ///
    /// This function intentionally never returns `s3://...`.
    pub fn translate_netcdf_url_path(
        &self,
        object: &object_store::path::Path,
    ) -> StorageResult<String> {
        match &self.base {
            DatasetsStoreBase::Local { .. } => {
                // NetCDF libraries accept local filesystem paths directly.
                let url = self.object_url_path(object)?;
                Ok(url
                    .strip_prefix("file://")
                    .unwrap_or(url.as_str())
                    .to_string())
            }
            DatasetsStoreBase::S3 { endpoint, .. } => {
                // NetCDF remote access requires an HTTP/S URL and `#mode=bytes`.
                // We intentionally do not return `s3://...` URLs here.
                if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                    return Err(error::StorageError::InvalidConfig {
                        key: "AWS_ENDPOINT",
                        message: format!(
                            "NetCDF URLs require an http:// or https:// endpoint, got: {endpoint}"
                        ),
                    });
                }

                let mut url = self.object_url_path(object)?;

                if !url.ends_with("#mode=bytes") {
                    url.push_str("#mode=bytes");
                }

                Ok(url)
            }
        }
    }

    /// Translate a given object path to a general-purpose URL-like string.
    ///
    /// Examples:
    /// - Local: `file:///abs/path/to/datasets/foo.nc`
    /// - S3 path-style: `https://endpoint/bucket/foo.nc`
    /// - S3 virtual-hosted-style: `https://bucket.endpoint/foo.nc`
    pub fn object_url_path(&self, object: &object_store::path::Path) -> StorageResult<String> {
        match &self.base {
            DatasetsStoreBase::Local { root } => {
                // LocalFileSystem is configured with a prefix. Object paths are
                // relative to that prefix, so we can join them.
                let root = std::fs::canonicalize(root).map_err(|e| {
                    error::StorageError::InitializationError(format!(
                        "Failed to canonicalize local datasets root {:?}: {}",
                        root, e
                    ))
                })?;

                let suffix = std::path::Path::new(object.as_ref());
                let full_path = root.join(suffix);
                // `full_path` is absolute on Unix, so `file://` + `/...` yields
                // `file:///...`.
                Ok(format!("file://{}", full_path.to_string_lossy()))
            }
            DatasetsStoreBase::S3 {
                bucket,
                endpoint,
                is_virtual_hosted_style,
            } => {
                let endpoint = endpoint.trim_end_matches('/');
                let key = object.as_ref().trim_start_matches('/');

                let url = if *is_virtual_hosted_style {
                    if key.is_empty() {
                        endpoint.to_string()
                    } else {
                        format!("{}/{}", endpoint, key)
                    }
                } else {
                    let bucket_name =
                        bucket.as_ref().ok_or(error::StorageError::MissingConfig {
                            key: "BEACON_S3_BUCKET",
                        })?;
                    if key.is_empty() {
                        format!("{}/{bucket_name}", endpoint)
                    } else {
                        format!("{}/{}/{}", endpoint, bucket_name, key)
                    }
                };

                Ok(url)
            }
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for DatasetsStore {
    /// Save the provided `payload` to `location` with the given options
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.deref().put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.deref().put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.deref().get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.deref().delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.deref().list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.deref().list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.deref().copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.deref().copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use crate::event_handlers::fs::{FileSystemEvent, FileSystemEventHandler};

    async fn list_locations(store: &impl ObjectStore, prefix: Option<&Path>) -> Vec<String> {
        let mut out = Vec::new();
        let mut stream = store.list(prefix);
        while let Some(item) = stream.next().await {
            let meta = item.expect("list item should be Ok");
            out.push(meta.location.to_string());
        }
        out.sort();
        out
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn datasets_store_local_notified_primes_cache_and_updates_on_events() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join("a")).expect("mkdir");
        std::fs::write(dir.path().join("a/existing.txt"), b"hi").expect("write");

        let store = DatasetsStore::new_local_at(dir.path().to_path_buf(), true)
            .await
            .expect("create datasets store");

        // Starts with the pre-existing file (primed from the underlying LocalFileSystem).
        let got = list_locations(&store, None).await;
        assert_eq!(got, vec!["a/existing.txt"]);

        // Create a new file and push a Created event so the cache-based list reflects it.
        std::fs::write(dir.path().join("a/new.txt"), b"ok").expect("write");
        store
            .push_event::<FileSystemEventHandler, _>(FileSystemEvent::Created(ObjectMeta {
                location: Path::from("a/new.txt"),
                size: 2,
                last_modified: Utc::now(),
                e_tag: None,
                version: None,
            }))
            .await
            .expect("push created");

        let got = list_locations(&store, Some(&Path::from("a/"))).await;
        assert_eq!(got, vec!["a/existing.txt", "a/new.txt"]);

        // Modify updates cached metadata.
        let modified_contents = b"much longer contents";
        std::fs::write(dir.path().join("a/new.txt"), modified_contents).expect("write");
        store
            .push_event::<FileSystemEventHandler, _>(FileSystemEvent::Modified(ObjectMeta {
                location: Path::from("a/new.txt"),
                size: modified_contents.len() as u64,
                last_modified: Utc::now(),
                e_tag: None,
                version: None,
            }))
            .await
            .expect("push modified");

        let mut stream = store.list(Some(&Path::from("a/")));
        let first = stream.next().await.expect("first").expect("ok");
        let second = stream.next().await.expect("second").expect("ok");
        let (meta_existing, meta_new) = if first.location.as_ref() == "a/new.txt" {
            (second, first)
        } else {
            (first, second)
        };
        assert_eq!(meta_existing.location.as_ref(), "a/existing.txt");
        assert_eq!(meta_new.location.as_ref(), "a/new.txt");
        assert_eq!(meta_new.size, modified_contents.len() as u64);

        // Delete removes from cache-backed listing.
        std::fs::remove_file(dir.path().join("a/new.txt")).expect("remove");

        store
            .push_event::<FileSystemEventHandler, _>(FileSystemEvent::Deleted(Path::from(
                "a/new.txt",
            )))
            .await
            .expect("push deleted");
        let got = list_locations(&store, Some(&Path::from("a/"))).await;
        assert_eq!(got, vec!["a/existing.txt"]);
    }

    #[test]
    fn object_url_path_local_uses_file_scheme() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DatasetsStore {
            // Dummy inner: object_url_path does not depend on it.
            inner: BeaconObjectStoreType::Default(Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"),
            )),
            base: DatasetsStoreBase::Local {
                root: dir.path().to_path_buf(),
            },
        };

        let url = store.object_url_path(&Path::from("a/b.txt")).expect("url");
        assert!(url.starts_with("file:///"), "url={}", url);
        assert!(url.ends_with("/a/b.txt"), "url={}", url);
    }

    #[test]
    fn object_url_path_s3_uses_s3_scheme() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DatasetsStore {
            // Dummy inner: object_url_path does not depend on it.
            inner: BeaconObjectStoreType::Default(Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"),
            )),
            base: DatasetsStoreBase::S3 {
                bucket: Some("my-bucket".to_string()),
                endpoint: "https://example.test".to_string(),
                is_virtual_hosted_style: false,
            },
        };

        let url = store
            .object_url_path(&Path::from("datasets/foo.nc"))
            .expect("url");
        assert_eq!(url, "https://example.test/my-bucket/datasets/foo.nc");
    }

    #[test]
    fn translate_netcdf_url_path_local_is_plain_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DatasetsStore {
            // Dummy inner: translate_netcdf_url_path does not depend on it.
            inner: BeaconObjectStoreType::Default(Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"),
            )),
            base: DatasetsStoreBase::Local {
                root: dir.path().to_path_buf(),
            },
        };

        let path = store
            .translate_netcdf_url_path(&Path::from("a/b.nc"))
            .expect("path");
        assert!(!path.starts_with("file://"), "path={}", path);
        assert!(path.ends_with("/a/b.nc"), "path={}", path);
    }

    #[test]
    fn translate_netcdf_url_path_s3_adds_mode_bytes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DatasetsStore {
            // Dummy inner: translate_netcdf_url_path does not depend on it.
            inner: BeaconObjectStoreType::Default(Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"),
            )),
            base: DatasetsStoreBase::S3 {
                bucket: Some("my-bucket".to_string()),
                endpoint: "https://example.test".to_string(),
                is_virtual_hosted_style: false,
            },
        };

        let url = store
            .translate_netcdf_url_path(&Path::from("datasets/foo.nc"))
            .expect("url");
        assert_eq!(
            url,
            "https://example.test/my-bucket/datasets/foo.nc#mode=bytes"
        );
    }

    #[test]
    fn translate_netcdf_url_path_s3_virtual_hosted_style_adds_mode_bytes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DatasetsStore {
            // Dummy inner: translate_netcdf_url_path does not depend on it.
            inner: BeaconObjectStoreType::Default(Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"),
            )),
            base: DatasetsStoreBase::S3 {
                // In virtual-hosted style, the bucket is encoded in the hostname.
                bucket: None,
                endpoint: "https://my-bucket.example.test".to_string(),
                is_virtual_hosted_style: true,
            },
        };

        let url = store
            .translate_netcdf_url_path(&Path::from("datasets/foo.nc"))
            .expect("url");
        assert_eq!(
            url,
            "https://my-bucket.example.test/datasets/foo.nc#mode=bytes"
        );
    }

    #[test]
    fn translate_netcdf_url_path_rejects_non_http_endpoint() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DatasetsStore {
            inner: BeaconObjectStoreType::Default(Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"),
            )),
            base: DatasetsStoreBase::S3 {
                bucket: Some("my-bucket".to_string()),
                endpoint: "s3://my-bucket".to_string(),
                is_virtual_hosted_style: true,
            },
        };

        let err = store
            .translate_netcdf_url_path(&Path::from("datasets/foo.nc"))
            .expect_err("should error");
        let msg = err.to_string();
        assert!(msg.contains("AWS_ENDPOINT"), "msg={}", msg);
        assert!(msg.contains("http"), "msg={}", msg);
    }

    #[test]
    fn object_url_path_non_virtual_requires_bucket() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DatasetsStore {
            inner: BeaconObjectStoreType::Default(Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"),
            )),
            base: DatasetsStoreBase::S3 {
                bucket: None,
                endpoint: "https://example.test".to_string(),
                is_virtual_hosted_style: false,
            },
        };

        let err = store
            .object_url_path(&Path::from("datasets/foo.nc"))
            .expect_err("should error");
        assert!(err.to_string().contains("BEACON_S3_BUCKET"));
    }
}
