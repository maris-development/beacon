//! Test source store that counts, delays and fails calls.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use beacon_object_store_cache::{CachedObjectStore, StoreCacheConfig};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OsResult,
};
use tokio::sync::Notify;

/// In-memory source that counts the calls that reach it.
#[derive(Debug, Default)]
pub struct CountingStore {
    pub inner: InMemory,
    pub lists: AtomicUsize,
    pub gets: AtomicUsize,
    /// When set, a listing waits for `release` after it read the source.
    pub hold_lists: AtomicBool,
    pub release: Arc<Notify>,
    pub fail_lists: AtomicBool,
}

impl CountingStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn lists(&self) -> usize {
        self.lists.load(Ordering::SeqCst)
    }

    pub fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }

    /// Write an object without the cache, as a different program does.
    pub async fn external_put(&self, path: &str, data: &'static str) {
        self.inner.put(&Path::from(path), data.into()).await.unwrap();
    }

    pub async fn external_delete(&self, path: &str) {
        self.inner.delete(&Path::from(path)).await.unwrap();
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        if self.fail_lists.load(Ordering::SeqCst) {
            let error = Error::Generic {
                store: "CountingStore",
                source: "listing failed on request".into(),
            };
            return stream::once(async move { Err(error) }).boxed();
        }
        let listing = self.inner.list(prefix);
        if !self.hold_lists.load(Ordering::SeqCst) {
            return listing;
        }
        let release = Arc::clone(&self.release);
        stream::once(async move {
            // The snapshot is complete before the wait starts.
            let snapshot: Vec<OsResult<ObjectMeta>> = listing.collect().await;
            release.notified().await;
            stream::iter(snapshot)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Cache settings for tests: enabled, 60 second sync.
pub fn config() -> StoreCacheConfig {
    StoreCacheConfig {
        enable: true,
        sync_interval: Duration::from_secs(60),
        ..StoreCacheConfig::default()
    }
}

pub fn cached(source: &Arc<CountingStore>, config: &StoreCacheConfig) -> CachedObjectStore {
    CachedObjectStore::new(Arc::clone(source) as Arc<dyn ObjectStore>, config)
}

/// Paths of a full listing below `prefix`, in the order the store returns them.
pub async fn paths(store: &dyn ObjectStore, prefix: Option<&str>) -> Vec<String> {
    let prefix = prefix.map(Path::from);
    store
        .list(prefix.as_ref())
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .unwrap()
}
