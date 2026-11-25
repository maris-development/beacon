//! The cache of open file handles.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use moka::future::Cache;

use super::CachedLocalOptions;
use super::version::FileVersion;

/// The store name in `object_store::Error::Generic`.
pub(super) const STORE: &str = "LocalFileSystem";

/// The source of handle epochs. Each open takes a new one; epochs are never reused.
static NEXT_EPOCH: AtomicU64 = AtomicU64::new(1);

/// A callback for a path whose cached handle became stale or was invalidated.
pub(crate) type OnChange = Arc<dyn Fn(&Path) + Send + Sync>;

/// An open file and the version it had when it was last checked.
#[derive(Debug)]
pub(crate) struct OpenFile {
    /// The open file.
    pub file: std::fs::File,
    /// The file version at open time.
    pub version: FileVersion,
    /// The epoch of this open. No other handle has it.
    pub epoch: u64,
    /// The generation slot of the path.
    slot: usize,
    /// The slot generation read before the open.
    generation: u64,
    /// The last version check, in microseconds since the `HandleCache` start.
    checked_at: AtomicU64,
    /// Lets one revalidation of this handle run at a time.
    revalidating: tokio::sync::Mutex<()>,
}

/// An error of a load, cloneable so that all joined callers get it.
#[derive(Clone)]
enum LoadError {
    NotFound(PathBuf, Arc<io::Error>),
    Io(Arc<io::Error>),
}

impl From<LoadError> for object_store::Error {
    fn from(e: LoadError) -> Self {
        match e {
            LoadError::NotFound(path, source) => object_store::Error::NotFound {
                path: path.to_string_lossy().to_string(),
                source: Box::new(source),
            },
            LoadError::Io(source) => object_store::Error::Generic {
                store: STORE,
                source: Box::new(source),
            },
        }
    }
}

fn not_found(path: &Path, source: io::Error) -> LoadError {
    LoadError::NotFound(path.to_path_buf(), Arc::new(source))
}

fn is_directory(path: &Path) -> LoadError {
    not_found(
        path,
        io::Error::new(io::ErrorKind::NotFound, "is directory"),
    )
}

/// Maps an I/O error on `path`, with a directory as not found.
fn map_io_error(path: &Path, e: io::Error) -> LoadError {
    if e.kind() == io::ErrorKind::NotFound {
        return not_found(path, e);
    }
    // Opening a directory can fail with another error kind, for example on Windows.
    if std::fs::metadata(path).is_ok_and(|m| m.is_dir()) {
        return is_directory(path);
    }
    LoadError::Io(Arc::new(e))
}

/// The number of invalidation generation slots of a [`HandleCache`].
const GENERATION_SLOTS: usize = 4096;

/// A bounded cache of open file handles, keyed by filesystem path.
pub(crate) struct HandleCache {
    cache: Cache<PathBuf, Arc<OpenFile>>,
    /// Invalidation counters, one per path hash slot. A handle opened before the last
    /// invalidation of its slot is never returned.
    generations: Arc<[AtomicU64]>,
    hasher: std::hash::RandomState,
    revalidate_after: Duration,
    on_change: Option<OnChange>,
    opens: Arc<AtomicU64>,
    start: Instant,
    /// Test seam: the next revalidation stat fails with a permission error.
    #[cfg(test)]
    fail_stat: std::sync::atomic::AtomicBool,
    /// Test seam: the next open signals the sender after the open and waits on the receiver.
    #[cfg(test)]
    pub(crate) pause_after_open: parking_lot::Mutex<Option<OpenGate>>,
}

/// The signal and the resume channel of a paused open.
#[cfg(test)]
pub(crate) type OpenGate = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);

impl HandleCache {
    /// Creates the cache. `on_change` runs for a path whose revalidation found a new
    /// version or a missing file, and for each [`HandleCache::invalidate`].
    pub fn new(opts: &CachedLocalOptions, on_change: Option<OnChange>) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(opts.max_open_files)
                .time_to_idle(opts.handle_idle)
                .build(),
            generations: (0..GENERATION_SLOTS).map(|_| AtomicU64::new(0)).collect(),
            hasher: Default::default(),
            revalidate_after: opts.revalidate_after,
            on_change,
            opens: Arc::new(AtomicU64::new(0)),
            start: Instant::now(),
            #[cfg(test)]
            fail_stat: Default::default(),
            #[cfg(test)]
            pause_after_open: Default::default(),
        }
    }

    /// Returns the open file at `path`, and checks its version when the last check is
    /// older than `revalidate_after`.
    pub async fn get(&self, path: &Path) -> Result<Arc<OpenFile>, object_store::Error> {
        let max_age = self.revalidate_after.as_micros() as u64;
        self.get_checked(path, |checked_at, now| {
            now.saturating_sub(checked_at) < max_age
        })
        .await
    }

    /// Returns the open file at `path` after a version check that starts after this call.
    pub async fn get_fresh(&self, path: &Path) -> Result<Arc<OpenFile>, object_store::Error> {
        let called_at = self.now();
        self.get_checked(path, |checked_at, _| checked_at > called_at)
            .await
    }

    /// Returns the cached handle of `path`, with no open and no version check.
    pub async fn peek(&self, path: &Path) -> Option<Arc<OpenFile>> {
        self.cache.get(path).await
    }

    /// Drops the cached handle of `path`, also one that an open in progress inserts later,
    /// and runs `on_change`.
    pub async fn invalidate(&self, path: &Path) {
        self.generations[self.slot(path)].fetch_add(1, Ordering::SeqCst);
        self.cache.invalidate(path).await;
        if let Some(on_change) = &self.on_change {
            on_change(path);
        }
    }

    /// The generation slot of `path`.
    fn slot(&self, path: &Path) -> usize {
        use std::hash::BuildHasher;
        (self.hasher.hash_one(path) % GENERATION_SLOTS as u64) as usize
    }

    /// True when `file` was opened before the last invalidation of its slot.
    fn is_outdated(&self, file: &OpenFile) -> bool {
        self.generations[file.slot].load(Ordering::SeqCst) != file.generation
    }

    /// Drops `file`, an outdated handle of `path`, if it is cached, without a new generation.
    async fn discard(&self, path: &Path, file: &Arc<OpenFile>) {
        if self
            .cache
            .get(path)
            .await
            .is_some_and(|current| Arc::ptr_eq(&current, file))
        {
            self.cache.invalidate(path).await;
            if let Some(on_change) = &self.on_change {
                on_change(path);
            }
        }
    }

    /// Invalidates `path` unless its cached handle is another one than `file`.
    pub async fn invalidate_if_current(&self, path: &Path, file: &Arc<OpenFile>) {
        match self.cache.get(path).await {
            Some(current) if !Arc::ptr_eq(&current, file) => {}
            _ => self.invalidate(path).await,
        }
    }

    /// The number of open attempts of this cache.
    pub fn opens(&self) -> u64 {
        self.opens.load(Ordering::Relaxed)
    }

    fn now(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }

    /// Returns the cached or a new handle, and revalidates it unless `fresh(checked_at, now)`.
    async fn get_checked(
        &self,
        path: &Path,
        fresh: impl Fn(u64, u64) -> bool,
    ) -> Result<Arc<OpenFile>, object_store::Error> {
        let called_at = self.generations[self.slot(path)].load(Ordering::SeqCst);
        loop {
            let file = self.load(path).await?;
            if self.is_outdated(&file) {
                self.discard(path, &file).await;
                // An open after all invalidations before this call can serve this call, uncached.
                if file.generation >= called_at
                    && fresh(file.checked_at.load(Ordering::Acquire), self.now())
                {
                    return Ok(file);
                }
                continue;
            }
            if fresh(file.checked_at.load(Ordering::Acquire), self.now())
                || self.revalidate(path, &file, &fresh).await?
            {
                return Ok(file);
            }
        }
    }

    /// Checks the version of `file` at `path`. Returns false when the caller must load again.
    async fn revalidate(
        &self,
        path: &Path,
        file: &OpenFile,
        fresh: impl Fn(u64, u64) -> bool,
    ) -> Result<bool, object_store::Error> {
        let _guard = file.revalidating.lock().await;
        // A revalidation that held the lock can make the handle fresh or replace it.
        if fresh(file.checked_at.load(Ordering::Acquire), self.now()) {
            return Ok(true);
        }
        match self.cache.get(path).await {
            Some(current) if std::ptr::eq(&*current, file) => {}
            _ => return Ok(false),
        }
        let checked_at = self.now();
        let owned = path.to_path_buf();
        let stat = tokio::task::spawn_blocking(move || std::fs::metadata(owned))
            .await
            .map_err(|e| object_store::Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?;
        #[cfg(test)]
        let stat = match self.fail_stat.swap(false, Ordering::Relaxed) {
            true => Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected")),
            false => stat,
        };
        match stat {
            Ok(m) if !m.is_dir() && FileVersion::from_metadata(&m) == file.version => {
                file.checked_at.store(checked_at, Ordering::Release);
                Ok(true)
            }
            Ok(m) if !m.is_dir() => {
                self.invalidate(path).await;
                Ok(false)
            }
            Ok(_) => {
                self.invalidate(path).await;
                Err(is_directory(path).into())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.invalidate(path).await;
                Err(not_found(path, e).into())
            }
            Err(e) => {
                // A handle that cannot be checked must not keep the file open.
                self.invalidate(path).await;
                Err(LoadError::Io(Arc::new(e)).into())
            }
        }
    }

    /// Returns the cached handle, or opens the file once for all concurrent callers.
    async fn load(&self, path: &Path) -> Result<Arc<OpenFile>, object_store::Error> {
        let owned = path.to_path_buf();
        let opens = self.opens.clone();
        let start = self.start;
        let slot = self.slot(path);
        let generations = self.generations.clone();
        #[cfg(test)]
        let gate = self.pause_after_open.lock().take();
        self.cache
            .try_get_with_by_ref(path, async move {
                // Read before the open, so that a later invalidation outdates this handle.
                let generation = generations[slot].load(Ordering::SeqCst);
                let opened = tokio::task::spawn_blocking(move || {
                    let opened = open(&owned, &opens, start, slot, generation);
                    #[cfg(test)]
                    if let Some((opened_tx, resume_rx)) = gate {
                        opened_tx.send(()).unwrap();
                        resume_rx.recv().unwrap();
                    }
                    opened
                })
                .await
                .map_err(|e| LoadError::Io(Arc::new(io::Error::other(e))))?;
                opened.map(Arc::new)
            })
            .await
            .map_err(|e: Arc<LoadError>| (*e).clone().into())
    }
}

/// Opens the file at `path` and reads its version.
fn open(
    path: &Path,
    opens: &AtomicU64,
    start: Instant,
    slot: usize,
    generation: u64,
) -> Result<OpenFile, LoadError> {
    opens.fetch_add(1, Ordering::Relaxed);
    let file = std::fs::File::open(path).map_err(|e| map_io_error(path, e))?;
    let m = file.metadata().map_err(|e| map_io_error(path, e))?;
    if m.is_dir() {
        return Err(is_directory(path));
    }
    Ok(OpenFile {
        file,
        version: FileVersion::from_metadata(&m),
        epoch: NEXT_EPOCH.fetch_add(1, Ordering::Relaxed),
        slot,
        generation,
        checked_at: AtomicU64::new(start.elapsed().as_micros() as u64),
        revalidating: tokio::sync::Mutex::new(()),
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::Mutex;

    use super::super::CachedLocalOptions;
    use super::*;

    fn opts(revalidate_after: Duration) -> CachedLocalOptions {
        CachedLocalOptions {
            revalidate_after,
            ..Default::default()
        }
    }

    /// Writes `data` to a temporary file and renames it over `path`.
    fn replace(path: &std::path::Path, data: &[u8]) {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, data).unwrap();
        std::fs::rename(&tmp, path).unwrap();
    }

    #[tokio::test]
    async fn one_open_for_many_reads() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"abc").unwrap();
        let cache = HandleCache::new(&opts(Duration::from_secs(3600)), None);
        for _ in 0..100 {
            let f = cache.get(&p).await.unwrap();
            assert_eq!(f.version.size, 3);
        }
        assert_eq!(cache.opens(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_first_reads_open_once() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"abc").unwrap();
        let cache = Arc::new(HandleCache::new(&opts(Duration::from_secs(3600)), None));
        let tasks: Vec<_> = (0..64)
            .map(|_| {
                let cache = cache.clone();
                let p = p.clone();
                tokio::spawn(async move { cache.get(&p).await.unwrap() })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(cache.opens(), 1);
    }

    #[tokio::test]
    async fn capacity_is_respected() {
        let dir = tempfile::tempdir().unwrap();
        let o = CachedLocalOptions {
            max_open_files: 2,
            ..Default::default()
        };
        let cache = HandleCache::new(&o, None);
        for i in 0..10 {
            let p = dir.path().join(format!("{i}.bin"));
            std::fs::write(&p, b"x").unwrap();
            cache.get(&p).await.unwrap();
        }
        cache.cache.run_pending_tasks().await;
        assert!(cache.cache.entry_count() <= 2);
    }

    #[tokio::test]
    async fn missing_and_directory_are_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HandleCache::new(&CachedLocalOptions::default(), None);
        let err = cache
            .get(&dir.path().join("missing.bin"))
            .await
            .unwrap_err();
        assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");
        let err = cache.get(dir.path()).await.unwrap_err();
        assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");
    }

    #[tokio::test]
    async fn replaced_file_shows_after_revalidate() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"abc").unwrap();
        let cache = HandleCache::new(&opts(Duration::from_millis(500)), None);
        assert_eq!(cache.get(&p).await.unwrap().version.size, 3);
        replace(&p, b"abcdef");
        assert_eq!(cache.get(&p).await.unwrap().version.size, 3);
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(cache.get(&p).await.unwrap().version.size, 6);
        assert_eq!(cache.opens(), 2);
    }

    #[tokio::test]
    async fn get_fresh_ignores_interval() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"abc").unwrap();
        let cache = HandleCache::new(&opts(Duration::from_secs(3600)), None);
        assert_eq!(cache.get(&p).await.unwrap().version.size, 3);
        replace(&p, b"abcdef");
        assert_eq!(cache.get_fresh(&p).await.unwrap().version.size, 6);
    }

    #[tokio::test]
    async fn unchanged_file_keeps_its_handle() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"abc").unwrap();
        let cache = HandleCache::new(&opts(Duration::from_secs(3600)), None);
        let first = cache.get(&p).await.unwrap();
        let fresh = cache.get_fresh(&p).await.unwrap();
        assert!(Arc::ptr_eq(&first, &fresh));
        assert_eq!(cache.opens(), 1);
    }

    #[tokio::test]
    async fn deleted_file_is_not_found_after_revalidate() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"abc").unwrap();
        let cache = HandleCache::new(&opts(Duration::from_secs(3600)), None);
        cache.get(&p).await.unwrap();
        std::fs::remove_file(&p).unwrap();
        let err = cache.get_fresh(&p).await.unwrap_err();
        assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");
        assert!(!cache.cache.contains_key(&p));
    }

    #[tokio::test]
    async fn change_runs_on_change() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bin");
        let b = dir.path().join("b.bin");
        std::fs::write(&a, b"abc").unwrap();
        std::fs::write(&b, b"abc").unwrap();
        let seen = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
        let record = seen.clone();
        let on_change: OnChange = Arc::new(move |p| record.lock().push(p.to_path_buf()));
        let cache = HandleCache::new(&opts(Duration::from_secs(3600)), Some(on_change));
        cache.get(&a).await.unwrap();
        cache.get(&b).await.unwrap();
        assert!(seen.lock().is_empty());
        replace(&a, b"abcdef");
        cache.get_fresh(&a).await.unwrap();
        cache.invalidate(&b).await;
        assert_eq!(*seen.lock(), vec![a.clone(), b.clone()]);
        // An unchanged file does not run on_change.
        cache.get_fresh(&a).await.unwrap();
        cache.get_fresh(&b).await.unwrap();
        assert_eq!(*seen.lock(), vec![a, b]);
    }

    #[tokio::test]
    async fn stat_error_drops_handle() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"abc").unwrap();
        let seen = Arc::new(Mutex::new(0));
        let record = seen.clone();
        let on_change: OnChange = Arc::new(move |_| *record.lock() += 1);
        let cache = HandleCache::new(&opts(Duration::from_secs(3600)), Some(on_change));
        cache.get(&p).await.unwrap();
        cache.fail_stat.store(true, Ordering::Relaxed);
        let err = cache.get_fresh(&p).await.unwrap_err();
        assert!(
            matches!(err, object_store::Error::Generic { store: STORE, .. }),
            "{err}"
        );
        assert!(!cache.cache.contains_key(&p));
        assert_eq!(*seen.lock(), 1);
        cache.get(&p).await.unwrap();
        assert_eq!(cache.opens(), 2);
    }

    #[tokio::test]
    async fn every_open_gives_new_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bin");
        let b = dir.path().join("b.bin");
        std::fs::write(&a, b"abc").unwrap();
        std::fs::write(&b, b"abc").unwrap();
        let cache = HandleCache::new(&opts(Duration::from_secs(3600)), None);
        let first = cache.get(&a).await.unwrap();
        assert_ne!(cache.get(&b).await.unwrap().epoch, first.epoch);
        // An unchanged revalidation keeps the handle and the epoch.
        assert_eq!(cache.get_fresh(&a).await.unwrap().epoch, first.epoch);
        cache.invalidate(&a).await;
        let second = cache.get(&a).await.unwrap();
        assert!(second.epoch > first.epoch);
        // A removal without `invalidate`, as by eviction, also ends the epoch.
        cache.cache.invalidate(&a).await;
        assert!(cache.get(&a).await.unwrap().epoch > second.epoch);
    }

    #[tokio::test]
    async fn invalidate_if_current_keeps_newer_handle() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"abc").unwrap();
        let cache = HandleCache::new(&opts(Duration::from_secs(3600)), None);
        let old = cache.get(&p).await.unwrap();
        cache.invalidate(&p).await;
        let new = cache.get(&p).await.unwrap();
        cache.invalidate_if_current(&p, &old).await;
        assert!(Arc::ptr_eq(&cache.get(&p).await.unwrap(), &new));
        cache.invalidate_if_current(&p, &new).await;
        assert!(!Arc::ptr_eq(&cache.get(&p).await.unwrap(), &new));
        assert_eq!(cache.opens(), 3);
    }
}
