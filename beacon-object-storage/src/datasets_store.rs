use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    sync::Arc,
};

use crate::config::StorageConfig;

use futures::{StreamExt, future::BoxFuture, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, aws::AmazonS3Builder,
    local::LocalFileSystem, path::Path, prefix::PrefixStore,
};
use parking_lot::RwLock;
use tokio::{sync::broadcast, task::JoinHandle};

use crate::{
    error::{self, StorageResult},
    event::ObjectEvent,
    fs_event_listener::FsEventListener,
    object_cache::ObjectCache,
};

/// Build the datasets [`DatasetsStore`] from the given storage configuration.
///
/// The backing object store is selected from `storage`:
/// - When `storage.s3.data_lake` is set, an S3-compatible store is configured
///   from the standard AWS environment variables (see [`AmazonS3Builder::from_env`]).
///   For path-style requests a bucket name (`BEACON_S3_BUCKET`) is required; for
///   virtual-hosted-style the bucket is encoded in the endpoint host.
/// - Otherwise a local filesystem store rooted at `datasets_dir` is used.
///
/// The resolved `storage`/`datasets_dir` are retained on the returned store so
/// [`DatasetsStore::translate_netcdf_url_path`] uses the same backend selection
/// without consulting any process-global config.
/// Build a [`DatasetsStore`] backed by the local filesystem rooted at
/// `datasets_dir`, using default storage settings (no S3, no filesystem events).
///
/// Intended for tests and embedders that only need a plain local datasets store
/// without composing a full [`crate::ObjectStores`].
pub async fn local_datasets_store(datasets_dir: PathBuf) -> StorageResult<DatasetsStore> {
    create_datasets_store(&StorageConfig::default(), datasets_dir).await
}

pub(crate) async fn create_datasets_store(
    storage: &StorageConfig,
    datasets_dir: PathBuf,
) -> StorageResult<DatasetsStore> {
    let (inner, event_listener): (
        Arc<dyn ObjectStore + Send + Sync>,
        Option<Arc<dyn EventListener>>,
    ) = if storage.s3.data_lake {
        tracing::info!("Using S3 object store for datasets");

        let s3 = &storage.s3;
        let mut builder = AmazonS3Builder::from_env()
            .with_allow_http(true)
            .with_virtual_hosted_style_request(s3.enable_virtual_hosting);

        if !s3.enable_virtual_hosting {
            // Path-style requests need an explicit bucket name.
            let bucket = s3.bucket.as_ref().ok_or(error::StorageError::MissingConfig {
                key: "BEACON_S3_BUCKET",
            })?;
            builder = builder.with_bucket_name(bucket);
        }

        // `object_store::Error` converts into `StorageError::ObjectStore` via `?`.
        let store = builder.build()?;

        // TODO: wire S3 change notifications into an `EventListener` so the
        // cache-backed fast path can be enabled for the S3 backend too.
        (Arc::new(store), None)
    } else {
        tracing::info!("Using local filesystem object store for datasets");

        let root = datasets_dir.clone();
        let store = LocalFileSystem::new_with_prefix(&root)?.with_automatic_cleanup(true);
        let inner = Arc::new(store) as Arc<dyn ObjectStore + Send + Sync>;

        // Watch the datasets directory for changes (on by default; see
        // `storage.enable_fs_events`). Failure to start the watcher is
        // non-fatal: the store still works, just without the cache-backed fast
        // path and event subscriptions.
        let event_listener: Option<Arc<dyn EventListener>> = if storage.enable_fs_events {
            match FsEventListener::new(root) {
                Ok(listener) => Some(Arc::new(listener) as Arc<dyn EventListener>),
                Err(e) => {
                    tracing::warn!(
                        "Failed to start filesystem event listener, continuing without it: {e}"
                    );
                    None
                }
            }
        } else {
            tracing::info!("Filesystem events disabled via BEACON_ENABLE_FS_EVENTS");
            None
        };

        (inner, event_listener)
    };

    Ok(DatasetsStore::new(inner, event_listener)
        .await
        .with_storage(storage.clone(), datasets_dir))
}

pub trait EventListener: Send + Sync {
    fn poll(&self) -> BoxFuture<'_, Vec<ObjectEvent>>;
}

// Prefix that Beacon uses to write to for locally produced data (e.g. compaction output, materialized views, etc.).
// This is to avoid conflicts with user data. Listing, head requests, etc. for this prefix should be blocked.
pub const DATASETS_WRITEABLE_PREFIX: &str = "__beacon__";

/// Capacity of each per-prefix broadcast channel. A subscriber that lags further
/// behind than this will observe a [`broadcast::error::RecvError::Lagged`] and
/// should fall back to re-listing the affected prefix.
const EVENT_CHANNEL_CAPACITY: usize = 4096;

/// Map of subscription prefix to the broadcast sender feeding all subscribers of
/// that prefix. Shared between the store and its background poll task.
type Subscribers = Arc<RwLock<HashMap<String, broadcast::Sender<ObjectEvent>>>>;

pub struct DatasetsStore {
    inner: Arc<dyn ObjectStore + Send + Sync>,
    object_cache: Option<Arc<RwLock<ObjectCache>>>,
    event_listener: Option<Arc<dyn EventListener>>,
    /// Per-prefix broadcast senders handed out by [`Self::subscribe_events`].
    subscribers: Subscribers,
    /// Background task that drains [`EventListener::poll`] into `object_cache`
    /// and fans events out to `subscribers`. Aborted when the store is dropped.
    poll_task: Option<JoinHandle<()>>,
    /// Storage configuration this store was built from, retained for NetCDF URL
    /// translation. Defaults to a local configuration.
    storage: StorageConfig,
    /// Root directory for the local datasets store (used by NetCDF URL
    /// translation in local mode).
    datasets_dir: PathBuf,
}

impl DatasetsStore {
    pub async fn new(
        object_store: Arc<dyn ObjectStore + Send + Sync>,
        event_listener: Option<Arc<dyn EventListener>>,
    ) -> Self {
        let object_cache = if event_listener.is_some() {
            // If we have an event listener, we can keep an in-memory cache of the object metadata.
            // This allows us to efficiently support point lookups and prefix listings without
            // repeatedly scanning the backing store.
            //
            // Prime it with a full listing of the backing store so cache-backed
            // listings are correct from the start; the poll task then keeps it in
            // sync as events arrive.
            let mut objects = Vec::new();
            let mut stream = object_store.list(None);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(meta) => objects.push(meta),
                    Err(e) => {
                        tracing::warn!("Failed to list object while priming datasets cache: {e}")
                    }
                }
            }
            Some(Arc::new(RwLock::new(ObjectCache::new(objects))))
        } else {
            // If we don't have an event listener, we won't be able to keep the cache up-to-date,
            // so we won't use it.
            None
        };

        let subscribers: Subscribers = Arc::new(RwLock::new(HashMap::new()));

        // When both a listener and a cache exist, spawn a background task that
        // keeps the cache in sync with events reported by the listener and fans
        // those events out to any prefix subscribers.
        let poll_task = match (event_listener.as_ref(), object_cache.as_ref()) {
            (Some(listener), Some(cache)) => Some(Self::spawn_poll_task(
                listener.clone(),
                cache.clone(),
                subscribers.clone(),
            )),
            _ => None,
        };

        DatasetsStore {
            inner: object_store,
            object_cache,
            event_listener,
            subscribers,
            poll_task,
            storage: StorageConfig::default(),
            datasets_dir: PathBuf::new(),
        }
    }

    /// Attach the storage config + datasets directory used to build this store so
    /// [`Self::translate_netcdf_url_path`] resolves URLs without a global config.
    pub fn with_storage(mut self, storage: StorageConfig, datasets_dir: PathBuf) -> Self {
        self.storage = storage;
        self.datasets_dir = datasets_dir;
        self
    }

    /// Continuously drain events from `listener`, apply them to `cache`, and fan
    /// them out to matching `subscribers`.
    ///
    /// The task is expected to live for as long as the store; it is aborted in
    /// [`Drop`]. `poll` is awaited in a loop, so the listener is responsible for
    /// suspending until events are available rather than returning empty batches
    /// in a tight loop.
    fn spawn_poll_task(
        listener: Arc<dyn EventListener>,
        cache: Arc<RwLock<ObjectCache>>,
        subscribers: Subscribers,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let events = listener.poll().await;
                if events.is_empty() {
                    continue;
                }

                // Update the cache first. Hold the write lock only for the
                // duration of the update; never across an `.await`.
                {
                    let mut guard = cache.write();
                    for event in &events {
                        match event {
                            ObjectEvent::Created(meta) | ObjectEvent::Modified(meta) => {
                                guard.insert(meta.clone());
                            }
                            ObjectEvent::Deleted(path) => {
                                guard.remove(path);
                            }
                        }
                    }
                }

                // Publish after the cache update so a subscriber that re-lists in
                // response observes the change.
                Self::dispatch_events(&subscribers, &events);
            }
        })
    }

    /// Fan each event out to every subscriber whose prefix matches the event's
    /// object path. Send errors (no live receivers) are ignored; such senders are
    /// pruned lazily by [`Self::subscribe_events`].
    fn dispatch_events(subscribers: &Subscribers, events: &[ObjectEvent]) {
        let subs = subscribers.read();
        if subs.is_empty() {
            return;
        }
        for event in events {
            let location = event_location(event);
            // Internal Beacon objects are never exposed to subscribers, just as
            // they are hidden from listings and reads.
            if is_internal_location(location) {
                continue;
            }
            let path = location.as_ref();
            for (prefix, tx) in subs.iter() {
                if path.starts_with(prefix.as_str()) && tx.send(event.clone()).is_err() {
                    // No live receivers for this prefix; the sender is pruned
                    // lazily by `subscribe_events`. Logged at TRACE since a
                    // subscriber dropping is normal.
                    tracing::trace!(prefix = %prefix, "no live subscribers; event dropped");
                }
            }
        }
    }

    /// Subscribe to object events whose path starts with `prefix`.
    ///
    /// This is intended for downstream consumers such as external tables that
    /// need to know when files were added to or removed from their dataset
    /// (identified by a path prefix). Subscribers for the same prefix share a
    /// single broadcast channel.
    ///
    /// Note: events are only ever produced when the store was built with an
    /// [`EventListener`]; otherwise the returned receiver simply never yields.
    pub fn subscribe_events(&self, prefix: &str) -> broadcast::Receiver<ObjectEvent> {
        let mut subs = self.subscribers.write();

        // Reuse the existing sender for this prefix if one is already live.
        if let Some(tx) = subs.get(prefix) {
            return tx.subscribe();
        }

        // Drop senders whose receivers have all gone away so the map stays
        // bounded by the number of distinct active prefixes.
        subs.retain(|_, tx| tx.receiver_count() > 0);

        let (tx, rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        subs.insert(prefix.to_string(), tx);
        rx
    }

    /// Returns a store rooted at the Beacon-internal writeable prefix
    /// (`__beacon__`).
    ///
    /// Objects under this prefix (compaction output, materialized views, etc.)
    /// are deliberately hidden from the public-facing `DatasetsStore` surface:
    /// they are filtered out of listings and subscriptions, and reads/heads for
    /// them return [`object_store::Error::NotFound`]. This method is the
    /// counterpart for Beacon's own machinery to read and write that data: the
    /// returned [`PrefixStore`] transparently prepends `__beacon__/` to every
    /// path, so a caller works with prefix-relative paths (e.g. `compaction/x`)
    /// while objects land under `__beacon__/compaction/x` in the backing store.
    ///
    /// The returned store wraps the same backing store and so performs no
    /// hiding of its own — everything written through it is fully accessible
    /// through it.
    pub fn internal_store(&self) -> Arc<dyn ObjectStore> {
        Arc::new(PrefixStore::new(
            self.inner.clone(),
            DATASETS_WRITEABLE_PREFIX,
        )) as Arc<dyn ObjectStore>
    }

    /// Translate an object path into a NetCDF-friendly location string.
    ///
    /// The backend is determined from the storage config this store was built
    /// with (retained via [`Self::with_storage`]):
    /// - **Local datasets**: a plain filesystem path rooted at the configured
    ///   datasets directory (no `file://` scheme — NetCDF libraries open local
    ///   paths directly).
    /// - **S3 datasets**: an `http://`/`https://` URL built from `AWS_ENDPOINT`
    ///   and the configured bucket, with `#mode=bytes` appended (required by some
    ///   NetCDF libraries for byte-range access).
    ///
    /// This function intentionally never returns an `s3://...` URL.
    pub fn translate_netcdf_url_path(&self, object: &Path) -> StorageResult<String> {
        let storage = &self.storage;
        if storage.s3.data_lake {
            let endpoint = s3_endpoint()?;
            let url = s3_object_url(
                &endpoint,
                storage.s3.bucket.as_deref(),
                storage.s3.enable_virtual_hosting,
                object,
            )?;
            Ok(append_mode_bytes(url))
        } else {
            local_object_path(&self.datasets_dir, object)
        }
    }
}

/// Reads and validates the S3 endpoint used to build NetCDF URLs.
fn s3_endpoint() -> StorageResult<String> {
    let endpoint =
        std::env::var("AWS_ENDPOINT").map_err(|source| error::StorageError::MissingEnvVar {
            var: "AWS_ENDPOINT",
            source,
        })?;
    if endpoint.trim().is_empty() {
        return Err(error::StorageError::InvalidConfig {
            key: "AWS_ENDPOINT",
            message: "must not be empty".to_string(),
        });
    }
    Ok(endpoint)
}

/// Builds the `http(s)` URL for an object served from an S3-compatible backend.
///
/// - Path-style: `{endpoint}/{bucket}/{key}` (requires a bucket name).
/// - Virtual-hosted-style: `{endpoint}/{key}` (the bucket is encoded in the
///   endpoint host, so no bucket segment is added).
fn s3_object_url(
    endpoint: &str,
    bucket: Option<&str>,
    is_virtual_hosted_style: bool,
    object: &Path,
) -> StorageResult<String> {
    // NetCDF remote access requires an HTTP/S URL; never emit `s3://...`.
    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        return Err(error::StorageError::InvalidConfig {
            key: "AWS_ENDPOINT",
            message: format!(
                "NetCDF URLs require an http:// or https:// endpoint, got: {endpoint}"
            ),
        });
    }

    let endpoint = endpoint.trim_end_matches('/');
    let key = object.as_ref().trim_start_matches('/');

    let url = if is_virtual_hosted_style {
        if key.is_empty() {
            endpoint.to_string()
        } else {
            format!("{endpoint}/{key}")
        }
    } else {
        let bucket = bucket.ok_or(error::StorageError::MissingConfig {
            key: "BEACON_S3_BUCKET",
        })?;
        if key.is_empty() {
            format!("{endpoint}/{bucket}")
        } else {
            format!("{endpoint}/{bucket}/{key}")
        }
    };

    Ok(url)
}

/// Builds an absolute local filesystem path for an object under `root`.
fn local_object_path(root: &std::path::Path, object: &Path) -> StorageResult<String> {
    // The local store is rooted at `root` and object paths are relative to it.
    let root = std::fs::canonicalize(root).map_err(|e| {
        error::StorageError::InitializationError(format!(
            "Failed to canonicalize local datasets root {root:?}: {e}"
        ))
    })?;
    let full_path = root.join(object.as_ref());
    Ok(full_path.to_string_lossy().into_owned())
}

/// Appends the NetCDF byte-range fragment to `url` if not already present.
fn append_mode_bytes(mut url: String) -> String {
    if !url.ends_with("#mode=bytes") {
        url.push_str("#mode=bytes");
    }
    url
}

/// The object path an event refers to.
fn event_location(event: &ObjectEvent) -> &Path {
    match event {
        ObjectEvent::Created(meta) | ObjectEvent::Modified(meta) => &meta.location,
        ObjectEvent::Deleted(path) => path,
    }
}

impl Drop for DatasetsStore {
    fn drop(&mut self) {
        if let Some(task) = self.poll_task.take() {
            task.abort();
        }
    }
}

/// Returns `true` if `location` falls under the Beacon-internal writeable prefix
/// (the prefix itself or anything nested below it).
///
/// Objects under this prefix are produced by Beacon (compaction output,
/// materialized views, etc.) and must not leak into the user-facing surface, so
/// listings and head/get requests for them are blocked.
fn is_internal_location(location: &Path) -> bool {
    match location.as_ref().strip_prefix(DATASETS_WRITEABLE_PREFIX) {
        // Exact match (the prefix node itself) or a child path segment.
        Some(rest) => rest.is_empty() || rest.starts_with('/'),
        None => false,
    }
}

#[async_trait::async_trait]
impl ObjectStore for DatasetsStore {
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
        // Hide internal objects from reads (this also covers `head`, which is a
        // provided method implemented on top of `get_opts`).
        if is_internal_location(location) {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "path is under the Beacon-internal prefix".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Some(cache) = &self.object_cache {
            // An event listener keeps the cache up-to-date, so we can serve
            // listings from memory without scanning the backing store. Collect
            // under the read lock and release it before building the stream.
            let guard = cache.read();
            let objects = guard
                .list_prefix(prefix.map(|p| p.as_ref()).unwrap_or(""))
                .filter(|meta| !is_internal_location(&meta.location))
                .map(Ok)
                .collect::<Vec<_>>();
            drop(guard);
            Box::pin(futures::stream::iter(objects))
        } else {
            // No cache: stream from the backing store, dropping internal objects.
            Box::pin(self.inner.list(prefix).filter(|result| {
                let keep = match result {
                    Ok(meta) => !is_internal_location(&meta.location),
                    // Preserve errors so callers can observe listing failures.
                    Err(_) => true,
                };
                async move { keep }
            }))
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        if let Some(cache) = &self.object_cache {
            // Serve from the cache for consistency with `list`. We replicate
            // object_store's delimiter semantics: among the objects under
            // `prefix`, those that are a direct child are returned as objects,
            // while deeper paths contribute their first segment as a common
            // prefix (a "directory"). `Path::prefix_match` enforces path-segment
            // boundaries, so e.g. `foo/bar` is not treated as a child of `foo/ba`.
            let prefix = prefix.cloned().unwrap_or_default();

            let mut objects = Vec::new();
            let mut common_prefixes = BTreeSet::new();

            let guard = cache.read();
            for meta in guard.list_prefix(prefix.as_ref()) {
                // Internal objects (and any "directory" derived from them) stay
                // hidden, matching `list`/`get`.
                if is_internal_location(&meta.location) {
                    continue;
                }
                // Decide grouping in an inner scope so the borrow of `meta` (via
                // `prefix_match`) is released before we move `meta` into `objects`.
                // `Some(cp)` => nested directory; `None` => direct child object.
                let common_prefix = {
                    let mut parts = match meta.location.prefix_match(&prefix) {
                        Some(parts) => parts,
                        None => continue,
                    };
                    // First remaining segment after the prefix.
                    let Some(first) = parts.next() else {
                        // Object equals the prefix exactly; not a child.
                        continue;
                    };
                    // More segments follow => nested "directory" common prefix.
                    parts.next().is_some().then(|| prefix.clone().join(first))
                };
                match common_prefix {
                    Some(cp) => {
                        common_prefixes.insert(cp);
                    }
                    None => objects.push(meta),
                }
            }
            drop(guard);

            Ok(ListResult {
                objects,
                common_prefixes: common_prefixes.into_iter().collect(),
            })
        } else {
            // No cache: delegate to the backing store and drop internal entries.
            let mut result = self.inner.list_with_delimiter(prefix).await?;
            result
                .objects
                .retain(|meta| !is_internal_location(&meta.location));
            result
                .common_prefixes
                .retain(|prefix| !is_internal_location(prefix));
            Ok(result)
        }
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

impl std::fmt::Debug for DatasetsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetsStore")
            .field("inner", &self.inner)
            .field("cached", &self.object_cache.is_some())
            .field("event_driven", &self.event_listener.is_some())
            .finish()
    }
}

impl std::fmt::Display for DatasetsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DatasetsStore wrapping: {}", self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use object_store::{ObjectStoreExt, local::LocalFileSystem};
    use std::time::Duration;

    /// Test listener that yields batches pushed onto a channel, suspending when
    /// the channel is empty (and parking forever once the sender is dropped).
    struct ChannelListener {
        rx: flume::Receiver<Vec<ObjectEvent>>,
    }

    impl EventListener for ChannelListener {
        fn poll(&self) -> BoxFuture<'_, Vec<ObjectEvent>> {
            Box::pin(async move {
                match self.rx.recv_async().await {
                    Ok(events) => events,
                    // Sender dropped: park forever so the poll loop suspends
                    // instead of spinning on empty batches.
                    Err(_) => std::future::pending().await,
                }
            })
        }
    }

    fn meta(path: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            size,
            last_modified: Utc::now(),
            e_tag: None,
            version: None,
        }
    }

    async fn list_locations(store: &DatasetsStore, prefix: Option<&Path>) -> Vec<String> {
        let mut out = Vec::new();
        let mut stream = store.list(prefix);
        while let Some(item) = stream.next().await {
            out.push(item.expect("list item should be Ok").location.to_string());
        }
        out.sort();
        out
    }

    /// Poll the cache-backed listing until `predicate` holds or we time out.
    async fn eventually(
        store: &DatasetsStore,
        predicate: impl Fn(&[String]) -> bool,
    ) -> Vec<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let got = list_locations(store, None).await;
            if predicate(&got) {
                return got;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for cache to converge, last saw: {got:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn cache_is_primed_from_existing_objects() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;

        // Write objects into the backing store *before* building the store.
        inner
            .put(&Path::from("a/existing.txt"), b"hi".to_vec().into())
            .await
            .unwrap();
        inner
            .put(&Path::from("b/other.txt"), b"yo".to_vec().into())
            .await
            .unwrap();

        let (_tx, rx) = flume::unbounded::<Vec<ObjectEvent>>();
        let store = DatasetsStore::new(inner, Some(Arc::new(ChannelListener { rx }))).await;

        // No events have been delivered, yet the cache already reflects the
        // pre-existing objects because it was primed at construction.
        let got = list_locations(&store, None).await;
        assert_eq!(got, vec!["a/existing.txt", "b/other.txt"]);
    }

    #[tokio::test]
    async fn poll_task_applies_create_modify_delete_to_cache() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        let (tx, rx) = flume::unbounded();
        let store = DatasetsStore::new(inner, Some(Arc::new(ChannelListener { rx }))).await;

        // Cache starts empty.
        assert!(list_locations(&store, None).await.is_empty());

        // Created.
        tx.send(vec![ObjectEvent::Created(meta("a/file.txt", 2))])
            .unwrap();
        let got = eventually(&store, |g| g == ["a/file.txt"]).await;
        assert_eq!(got, vec!["a/file.txt"]);

        // Modified overwrites metadata; observable via the cached size.
        tx.send(vec![ObjectEvent::Modified(meta("a/file.txt", 999))])
            .unwrap();
        loop {
            // Confine the read guard so it is released before the `.await`.
            let size = {
                let guard = store.object_cache.as_ref().unwrap().read();
                guard.list_prefix("a/").next().map(|m| m.size)
            };
            if size == Some(999) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Deleted removes it from the cache-backed listing.
        tx.send(vec![ObjectEvent::Deleted(Path::from("a/file.txt"))])
            .unwrap();
        let got = eventually(&store, |g| g.is_empty()).await;
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn list_matches_on_segment_boundaries() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        for path in ["a/file.txt", "a/nested/deep.txt", "ab/sibling.txt"] {
            inner
                .put(&Path::from(path), b"x".to_vec().into())
                .await
                .unwrap();
        }

        let (_tx, rx) = flume::unbounded::<Vec<ObjectEvent>>();
        let store = DatasetsStore::new(inner, Some(Arc::new(ChannelListener { rx }))).await;

        // Recursive list under "a" returns everything beneath it but must exclude
        // the "ab/" sibling — consistent with list_with_delimiter.
        let got = list_locations(&store, Some(&Path::from("a"))).await;
        assert_eq!(got, vec!["a/file.txt", "a/nested/deep.txt"]);
    }

    #[tokio::test]
    async fn list_with_delimiter_uses_cache_and_groups_by_segment() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;

        // Pre-populate the backing store so the cache is primed at construction.
        for path in [
            "root.txt",          // direct object at root
            "a/file1.txt",       // under "a/"
            "a/file2.txt",       // under "a/"
            "a/nested/deep.txt", // contributes common prefix "a/nested"
            "ab/sibling.txt",    // must NOT be grouped under "a"
            "__beacon__/x.txt",  // internal: hidden
        ] {
            inner
                .put(&Path::from(path), b"x".to_vec().into())
                .await
                .unwrap();
        }

        let (_tx, rx) = flume::unbounded::<Vec<ObjectEvent>>();
        let store = DatasetsStore::new(inner, Some(Arc::new(ChannelListener { rx }))).await;

        // Root level: one direct object + common prefixes "a" and "ab"; the
        // internal prefix is absent.
        let root = store.list_with_delimiter(None).await.unwrap();
        let mut objs: Vec<_> = root
            .objects
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        objs.sort();
        let mut cps: Vec<_> = root.common_prefixes.iter().map(|p| p.to_string()).collect();
        cps.sort();
        assert_eq!(objs, vec!["root.txt"]);
        assert_eq!(cps, vec!["a", "ab"]);

        // Under "a/": two direct objects and the "a/nested" common prefix. The
        // segment boundary means "ab/..." is excluded.
        let a = store
            .list_with_delimiter(Some(&Path::from("a")))
            .await
            .unwrap();
        let mut objs: Vec<_> = a.objects.iter().map(|m| m.location.to_string()).collect();
        objs.sort();
        let cps: Vec<_> = a.common_prefixes.iter().map(|p| p.to_string()).collect();
        assert_eq!(objs, vec!["a/file1.txt", "a/file2.txt"]);
        assert_eq!(cps, vec!["a/nested"]);
    }

    #[tokio::test]
    async fn internal_prefix_hidden_from_listing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        let (tx, rx) = flume::unbounded();
        let store = DatasetsStore::new(inner, Some(Arc::new(ChannelListener { rx }))).await;

        tx.send(vec![
            ObjectEvent::Created(meta("user/data.parquet", 1)),
            ObjectEvent::Created(meta("__beacon__/compaction/out.parquet", 1)),
        ])
        .unwrap();

        // Only the user object is ever visible; the internal one is filtered out.
        let got = eventually(&store, |g| g == ["user/data.parquet"]).await;
        assert_eq!(got, vec!["user/data.parquet"]);
    }

    #[tokio::test]
    async fn get_opts_blocks_internal_prefix() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        let store = DatasetsStore::new(inner.clone(), None).await;

        // Write directly to the backing store under the internal prefix.
        inner
            .put(&Path::from("__beacon__/secret.bin"), b"x".to_vec().into())
            .await
            .unwrap();

        // The backing store can see it, but the facade blocks reads/head.
        assert!(
            inner
                .head(&Path::from("__beacon__/secret.bin"))
                .await
                .is_ok()
        );
        let err = store
            .get(&Path::from("__beacon__/secret.bin"))
            .await
            .expect_err("internal reads must be blocked");
        assert!(matches!(err, object_store::Error::NotFound { .. }));
    }

    #[tokio::test]
    async fn internal_store_maps_paths_under_writeable_prefix() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        let store = DatasetsStore::new(inner.clone(), None).await;

        let internal = store.internal_store();

        // Write through the prefixed store using a prefix-relative path.
        internal
            .put(
                &Path::from("compaction/out.parquet"),
                b"data".to_vec().into(),
            )
            .await
            .unwrap();

        // The object physically lands under `__beacon__/` in the backing store.
        let meta = inner
            .head(&Path::from("__beacon__/compaction/out.parquet"))
            .await
            .expect("object should exist under the internal prefix");
        assert_eq!(meta.size, 4);

        // It round-trips through the prefixed store at the relative path.
        let got = internal
            .get(&Path::from("compaction/out.parquet"))
            .await
            .expect("read back through prefixed store")
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"data");

        // ...but the public facade hides it: the absolute internal path is blocked.
        let err = store
            .get(&Path::from("__beacon__/compaction/out.parquet"))
            .await
            .expect_err("facade must block internal reads");
        assert!(matches!(err, object_store::Error::NotFound { .. }));
    }

    #[test]
    fn s3_object_url_path_style_includes_bucket() {
        let url = s3_object_url(
            "https://example.test",
            Some("my-bucket"),
            false,
            &Path::from("a/b.nc"),
        )
        .unwrap();
        assert_eq!(url, "https://example.test/my-bucket/a/b.nc");
    }

    #[test]
    fn s3_object_url_virtual_hosted_omits_bucket() {
        // Bucket lives in the endpoint host; no bucket segment is added, and a
        // trailing slash on the endpoint is trimmed.
        let url = s3_object_url(
            "https://my-bucket.example.test/",
            None,
            true,
            &Path::from("a/b.nc"),
        )
        .unwrap();
        assert_eq!(url, "https://my-bucket.example.test/a/b.nc");
    }

    #[test]
    fn s3_object_url_requires_http_endpoint() {
        let err = s3_object_url(
            "s3://my-bucket",
            Some("my-bucket"),
            false,
            &Path::from("a.nc"),
        )
        .expect_err("non-http endpoint must be rejected");
        assert!(matches!(
            err,
            error::StorageError::InvalidConfig {
                key: "AWS_ENDPOINT",
                ..
            }
        ));
    }

    #[test]
    fn s3_object_url_path_style_requires_bucket() {
        let err = s3_object_url("https://example.test", None, false, &Path::from("a.nc"))
            .expect_err("path-style without bucket must be rejected");
        assert!(matches!(
            err,
            error::StorageError::MissingConfig {
                key: "BEACON_S3_BUCKET"
            }
        ));
    }

    #[test]
    fn append_mode_bytes_is_idempotent() {
        assert_eq!(
            append_mode_bytes("http://x/y".to_string()),
            "http://x/y#mode=bytes"
        );
        assert_eq!(
            append_mode_bytes("http://x/y#mode=bytes".to_string()),
            "http://x/y#mode=bytes"
        );
    }

    #[test]
    fn local_object_path_joins_object_under_root() {
        let dir = tempfile::tempdir().expect("temp dir");
        let got = local_object_path(dir.path(), &Path::from("a/b.nc")).unwrap();

        // Should be the canonicalized root joined with the object path; compare
        // against the same construction to stay platform-agnostic.
        let expected = std::fs::canonicalize(dir.path())
            .unwrap()
            .join("a/b.nc")
            .to_string_lossy()
            .into_owned();
        assert_eq!(got, expected);
        // Never a URL scheme — NetCDF opens local paths directly.
        assert!(!got.starts_with("file://"));
    }

    #[test]
    fn is_internal_location_matches_prefix_and_children_only() {
        assert!(is_internal_location(&Path::from("__beacon__")));
        assert!(is_internal_location(&Path::from("__beacon__/a/b")));
        // A sibling with the same leading text is not internal.
        assert!(!is_internal_location(&Path::from("__beacon__extra/a")));
        assert!(!is_internal_location(&Path::from("user/data")));
    }

    /// Receive the next event from `rx` or fail the test on timeout.
    async fn recv(rx: &mut broadcast::Receiver<ObjectEvent>) -> ObjectEvent {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("broadcast channel closed")
    }

    #[tokio::test]
    async fn subscribe_events_delivers_only_matching_prefix() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        let (tx, rx) = flume::unbounded();
        let store = DatasetsStore::new(inner, Some(Arc::new(ChannelListener { rx }))).await;

        let mut sub = store.subscribe_events("table_a/");

        // An event under a different prefix must not be delivered, while the
        // matching one is. Send both so we can assert ordering/filtering.
        tx.send(vec![
            ObjectEvent::Created(meta("table_b/other.parquet", 1)),
            ObjectEvent::Created(meta("table_a/part-0.parquet", 2)),
        ])
        .unwrap();

        match recv(&mut sub).await {
            ObjectEvent::Created(m) => assert_eq!(m.location.as_ref(), "table_a/part-0.parquet"),
            other => panic!("unexpected event: {other:?}"),
        }

        // A delete under the watched prefix is delivered too.
        tx.send(vec![ObjectEvent::Deleted(Path::from(
            "table_a/part-0.parquet",
        ))])
        .unwrap();
        match recv(&mut sub).await {
            ObjectEvent::Deleted(p) => assert_eq!(p.as_ref(), "table_a/part-0.parquet"),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscribe_events_skips_internal_prefix() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        let (tx, rx) = flume::unbounded();
        let store = DatasetsStore::new(inner, Some(Arc::new(ChannelListener { rx }))).await;

        // Subscribe to everything; internal objects must still be withheld.
        let mut sub = store.subscribe_events("");

        tx.send(vec![
            ObjectEvent::Created(meta("__beacon__/compaction/out.parquet", 1)),
            ObjectEvent::Created(meta("user/data.parquet", 2)),
        ])
        .unwrap();

        // The internal event is filtered out, so the first delivered event is the
        // user one.
        match recv(&mut sub).await {
            ObjectEvent::Created(m) => assert_eq!(m.location.as_ref(), "user/data.parquet"),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscribe_events_same_prefix_shares_channel() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        let (tx, rx) = flume::unbounded();
        let store = DatasetsStore::new(inner, Some(Arc::new(ChannelListener { rx }))).await;

        let mut a = store.subscribe_events("t/");
        let mut b = store.subscribe_events("t/");

        // Only one sender should exist for the shared prefix.
        assert_eq!(store.subscribers.read().len(), 1);

        tx.send(vec![ObjectEvent::Created(meta("t/x.parquet", 1))])
            .unwrap();

        // Both receivers observe the same event.
        for sub in [&mut a, &mut b] {
            match recv(sub).await {
                ObjectEvent::Created(m) => assert_eq!(m.location.as_ref(), "t/x.parquet"),
                other => panic!("unexpected event: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn subscribe_events_prunes_dead_senders() {
        let dir = tempfile::tempdir().expect("temp dir");
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"))
            as Arc<dyn ObjectStore + Send + Sync>;
        let store = DatasetsStore::new(inner, None).await;

        // Subscribe then drop the receiver, leaving a sender with no consumers.
        drop(store.subscribe_events("dead/"));
        assert_eq!(store.subscribers.read().len(), 1);

        // Subscribing to a different prefix prunes the now-dead sender.
        let _live = store.subscribe_events("live/");
        let subs = store.subscribers.read();
        assert_eq!(subs.len(), 1);
        assert!(subs.contains_key("live/"));
    }
}
