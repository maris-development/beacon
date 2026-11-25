//! A local store wrapper that keeps file handles open and caches blocks.

mod blocks;
mod handles;
mod options;
mod read;
mod version;

use std::collections::VecDeque;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result, UploadPart,
};

pub use options::CachedLocalOptions;
use read::Reader;

/// The counters of a [`CachedLocalFileSystem`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// The open attempts of the handle cache.
    pub opens: u64,
    /// The blocks that the block cache answered.
    pub block_hits: u64,
    /// The blocks loaded from disk into the block cache.
    pub block_misses: u64,
    /// The positional reads from disk, failed reads included.
    pub disk_reads: u64,
    /// The bytes of the positional reads from disk.
    pub disk_bytes: u64,
}

/// A `LocalFileSystem` that keeps file handles open and reads with positional reads.
pub struct CachedLocalFileSystem {
    inner: LocalFileSystem,
    opts: CachedLocalOptions,
    reader: Arc<Reader>,
}

impl CachedLocalFileSystem {
    /// Wraps `inner` with the given options.
    pub fn new(inner: LocalFileSystem, opts: CachedLocalOptions) -> Self {
        let reader = Arc::new(Reader::new(&opts));
        if tokio::runtime::Handle::try_current().is_ok() {
            spawn_stats_log(Arc::downgrade(&reader), STATS_LOG_INTERVAL);
            spawn_maintenance(Arc::downgrade(&reader), maintenance_interval(&opts));
        }
        Self {
            inner,
            opts,
            reader,
        }
    }

    /// Returns the current counters.
    pub fn stats(&self) -> CacheStats {
        stats_of(&self.reader)
    }

    fn evictor(&self) -> Evictor {
        Evictor {
            inner: self.inner.clone(),
            reader: self.reader.clone(),
        }
    }

    /// Drops the cached handle and all cached blocks of `location`.
    async fn evict(&self, location: &Path) {
        self.evictor().evict(location).await
    }

    /// Hides `.nfs` files of `entries` and evicts each listed file whose cached version differs.
    fn check_listing(
        &self,
        entries: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let evictor = self.evictor();
        entries
            .try_filter(|meta| std::future::ready(!is_nfs(&meta.location)))
            .and_then(move |meta| {
                let evictor = evictor.clone();
                // Only in-memory work: a cache lookup and, for a changed file, an eviction.
                async move {
                    evictor.evict_if_changed(&meta).await;
                    Ok(meta)
                }
            })
            .boxed()
    }
}

/// The time between two debug logs of the counters.
const STATS_LOG_INTERVAL: Duration = Duration::from_secs(60);

fn stats_of(reader: &Reader) -> CacheStats {
    let (disk_reads, disk_bytes) = reader.disk_counters();
    let (block_hits, block_misses) = reader.block_counters();
    CacheStats {
        opens: reader.handles.opens(),
        block_hits,
        block_misses,
        disk_reads,
        disk_bytes,
    }
}

/// Logs the counters at debug level every `every` when they changed, until the store drops.
fn spawn_stats_log(reader: Weak<Reader>, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last = CacheStats::default();
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        loop {
            ticks.tick().await;
            let Some(reader) = reader.upgrade() else {
                return;
            };
            let stats = stats_of(&reader);
            if stats != last {
                tracing::debug!(
                    opens = stats.opens,
                    block_hits = stats.block_hits,
                    block_misses = stats.block_misses,
                    disk_reads = stats.disk_reads,
                    disk_bytes = stats.disk_bytes,
                    "cached local store counters"
                );
                last = stats;
            }
        }
    })
}

/// The time between two maintenance runs: the handle idle time, from 1 s to 5 s.
fn maintenance_interval(opts: &CachedLocalOptions) -> Duration {
    opts.handle_idle
        .clamp(Duration::from_secs(1), Duration::from_secs(5))
}

/// Closes idle handles and removes dropped blocks every `every`, until the store drops.
///
/// The caches run this work only on a cache call, so an idle server needs this task.
fn spawn_maintenance(reader: Weak<Reader>, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        loop {
            ticks.tick().await;
            let Some(reader) = reader.upgrade() else {
                return;
            };
            reader.run_pending_tasks().await;
        }
    })
}

/// True for an NFS silly-rename file, which a listing hides.
fn is_nfs(location: &Path) -> bool {
    location
        .filename()
        .is_some_and(|name| name.starts_with(".nfs"))
}

/// The parts of the store that an eviction needs, for streams and uploads that outlive a call.
#[derive(Clone)]
struct Evictor {
    inner: LocalFileSystem,
    reader: Arc<Reader>,
}

impl Evictor {
    /// Drops the cached handle and, through its change callback, all cached blocks of
    /// `location`.
    async fn evict(&self, location: &Path) {
        // An invalid path has no cached handle; the inner store reports the error.
        if let Ok(path) = self.inner.path_to_filesystem(location) {
            tracing::debug!(path = %path.display(), "write evicts cached file");
            self.reader.handles.invalidate(&path).await;
        }
    }

    /// Evicts the file of `meta` when its cached handle has another size or mtime.
    async fn evict_if_changed(&self, meta: &ObjectMeta) {
        let Ok(path) = self.inner.path_to_filesystem(&meta.location) else {
            return;
        };
        if let Some(file) = self.reader.handles.peek(&path).await
            && (file.version.size != meta.size || file.version.last_modified != meta.last_modified)
        {
            tracing::debug!(path = %path.display(), "listing shows a changed file");
            self.reader.handles.invalidate(&path).await;
        }
    }
}

/// A multipart upload that evicts its location when it completes or aborts.
struct EvictingUpload {
    upload: Box<dyn MultipartUpload>,
    evictor: Evictor,
    location: Path,
}

impl fmt::Debug for EvictingUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvictingUpload")
            .field("upload", &self.upload)
            .field("location", &self.location)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for EvictingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.upload.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.evictor.evict(&self.location).await;
        let result = self.upload.complete().await;
        self.evictor.evict(&self.location).await;
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.upload.abort().await;
        self.evictor.evict(&self.location).await;
        result
    }
}

impl fmt::Display for CachedLocalFileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachedLocalFileSystem({})", self.inner)
    }
}

impl fmt::Debug for CachedLocalFileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedLocalFileSystem")
            .field("inner", &self.inner)
            .field("opts", &self.opts)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for CachedLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.evict(location).await;
        let result = self.inner.put_opts(location, payload, opts).await;
        self.evict(location).await;
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.evict(location).await;
        let result = self.inner.put_multipart_opts(location, opts).await;
        self.evict(location).await;
        Ok(Box::new(EvictingUpload {
            upload: result?,
            evictor: self.evictor(),
            location: location.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let path = self.inner.path_to_filesystem(location)?;
        self.reader.get_opts(path, location.clone(), options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let path = self.inner.path_to_filesystem(location)?;
        self.reader.get_ranges(&path, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // The inner stream answers each location once and in order, so a queue pairs them.
        let pending = Arc::new(parking_lot::Mutex::new(VecDeque::<Option<Path>>::new()));
        let before = (self.evictor(), pending.clone());
        let locations = locations
            .then(move |location| {
                let (evictor, pending) = before.clone();
                async move {
                    if let Ok(path) = &location {
                        evictor.evict(path).await;
                    }
                    pending.lock().push_back(location.as_ref().ok().cloned());
                    location
                }
            })
            .boxed();
        let evictor = self.evictor();
        self.inner
            .delete_stream(locations)
            .then(move |result| {
                let evictor = evictor.clone();
                let location = pending.lock().pop_front().flatten();
                async move {
                    if let Some(path) = location {
                        evictor.evict(&path).await;
                    }
                    result
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.check_listing(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.check_listing(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects.retain(|meta| !is_nfs(&meta.location));
        result.common_prefixes.retain(|p| !is_nfs(p));
        let evictor = self.evictor();
        for meta in &result.objects {
            evictor.evict_if_changed(meta).await;
        }
        Ok(result)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.evict(to).await;
        let result = self.inner.copy_opts(from, to, options).await;
        self.evict(to).await;
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.evict(from).await;
        self.evict(to).await;
        let result = self.inner.rename_opts(from, to, options).await;
        self.evict(from).await;
        self.evict(to).await;
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;

    use object_store::ObjectStoreExt;

    use super::*;

    #[tokio::test]
    async fn stats_log_ends_with_the_store() {
        let reader = Arc::new(Reader::new(&CachedLocalOptions::default()));
        let task = spawn_stats_log(Arc::downgrade(&reader), Duration::from_millis(10));
        drop(reader);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("stats log task still runs")
            .unwrap();
    }

    #[test]
    fn maintenance_interval_is_one_to_five_seconds() {
        let interval = |handle_idle| {
            maintenance_interval(&CachedLocalOptions {
                handle_idle,
                ..Default::default()
            })
        };
        assert_eq!(interval(Duration::from_millis(100)), Duration::from_secs(1));
        assert_eq!(interval(Duration::from_secs(3)), Duration::from_secs(3));
        assert_eq!(interval(Duration::from_secs(30)), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn idle_handle_closes_without_store_calls() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, vec![1u8; 1000]).unwrap();
        let opts = CachedLocalOptions {
            handle_idle: Duration::from_millis(100),
            block_size: 4096,
            ..Default::default()
        };
        let inner = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let store = CachedLocalFileSystem::new(inner, opts);
        store.get_range(&Path::from("a.bin"), 0..10).await.unwrap();
        let handle = Arc::downgrade(&store.reader.handles.peek(&p).await.unwrap());
        assert!(handle.upgrade().is_some());
        // The handle expires after 100 ms; the maintenance task runs after 1 s.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(store.reader.handles.entry_count(), 0);
        assert!(handle.upgrade().is_none(), "the file is still open");
        std::fs::remove_file(&p).unwrap();
    }

    #[tokio::test]
    async fn maintenance_ends_with_the_store() {
        let reader = Arc::new(Reader::new(&CachedLocalOptions::default()));
        let task = spawn_maintenance(Arc::downgrade(&reader), Duration::from_millis(10));
        drop(reader);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("maintenance task still runs")
            .unwrap();
    }

    #[tokio::test]
    async fn writes_to_unread_paths_drop_no_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let opts = CachedLocalOptions {
            block_size: 4096,
            ..Default::default()
        };
        let inner = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let store = CachedLocalFileSystem::new(inner, opts);
        for i in 0..500 {
            store
                .put(&Path::from(format!("f{i}.bin")), vec![1u8; 10].into())
                .await
                .unwrap();
        }
        for i in 0..500 {
            store
                .delete(&Path::from(format!("f{i}.bin")))
                .await
                .unwrap();
        }
        let changes = || store.reader.handles.changes.load(Ordering::Relaxed);
        assert_eq!(changes(), 0);
        // A write to a read path drops its blocks once.
        let loc = Path::from("a.bin");
        store.put(&loc, vec![1u8; 10].into()).await.unwrap();
        store.get_range(&loc, 0..10).await.unwrap();
        store.put(&loc, vec![2u8; 10].into()).await.unwrap();
        assert_eq!(changes(), 1);
    }

    #[tokio::test]
    async fn new_spawns_no_task_without_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let inner = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        // A plain thread has no tokio runtime, so `new` must not spawn.
        std::thread::spawn(move || CachedLocalFileSystem::new(inner, Default::default()))
            .join()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn open_racing_put_is_not_kept() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.bin"), vec![b'A'; 1000]).unwrap();
        let opts = CachedLocalOptions {
            revalidate_after: Duration::from_secs(3600),
            ..Default::default()
        };
        let inner = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let store = Arc::new(CachedLocalFileSystem::new(inner, opts));
        let loc = Path::from("a.bin");
        let (opened_tx, opened_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        *store.reader.handles.pause_after_open.lock() = Some((opened_tx, resume_rx));
        let first = tokio::spawn({
            let (store, loc) = (store.clone(), loc.clone());
            async move { store.get_range(&loc, 0..1000).await.unwrap() }
        });
        // The load has opened the old file and waits.
        tokio::task::spawn_blocking(move || opened_rx.recv().unwrap())
            .await
            .unwrap();
        // Same length, so that only an eviction can show the new bytes.
        store.put(&loc, vec![b'B'; 1000].into()).await.unwrap();
        resume_tx.send(()).unwrap();
        first.await.unwrap();
        let got = store.get_range(&loc, 0..1000).await.unwrap();
        assert!(got.iter().all(|&x| x == b'B'), "stale bytes after put");
    }
}
