//! Shared state of one cached store.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use futures::future::{BoxFuture, Shared};
use object_store::path::Path;
use object_store::{Error, GetOptions, ObjectMeta, ObjectStore};
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::config::StoreCacheConfig;
use crate::index::{Index, is_under};

/// Marker in `GetOptions::extensions` that sends a call to the source.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheBypass;

/// Read the metadata of `location` from the source, not from a cache.
///
/// Use it before a write that must not replace an object the cache does not know.
pub async fn head_uncached(
    store: &dyn ObjectStore,
    location: &Path,
) -> object_store::Result<ObjectMeta> {
    let mut options = GetOptions::new().with_head(true);
    options.extensions.insert(CacheBypass);
    Ok(store.get_opts(location, options).await?.meta)
}

/// Result of the first listing of a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FillOutcome {
    Filled,
    TooLarge,
    Failed,
}

pub(crate) type SharedFill = Shared<BoxFuture<'static, FillOutcome>>;

#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub bypassed: AtomicU64,
    pub sync_errors: AtomicU64,
    pub last_sync_ms: AtomicU64,
    pub last_sync: Mutex<Option<DateTime<Utc>>>,
}

/// Counters and sizes of one cache.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CacheStatus {
    /// Objects in the cache.
    pub entries: usize,
    /// Prefixes that the cache holds completely.
    pub covered: Vec<String>,
    /// Calls answered from the cache.
    pub hits: u64,
    /// Calls that started a first listing of a prefix.
    pub misses: u64,
    /// Calls sent to the source.
    pub bypassed: u64,
    /// End time of the last sync.
    pub last_sync: Option<DateTime<Utc>>,
    /// Duration of the last sync in milliseconds.
    pub last_sync_ms: u64,
    /// Syncs of a prefix that failed.
    pub sync_errors: u64,
}

pub(crate) struct CacheState {
    pub inner: Arc<dyn ObjectStore>,
    pub config: StoreCacheConfig,
    pub index: RwLock<Index>,
    seq: AtomicU64,
    /// First listings in flight, by prefix.
    pub fills: Mutex<HashMap<String, SharedFill>>,
    /// Prefixes over the entry limit, with the time of the last attempt.
    pub too_large: Mutex<HashMap<String, Instant>>,
    pub sync_task: Mutex<Option<JoinHandle<()>>>,
    /// Held during a sync pass, so only one pass runs.
    pub sync_lock: tokio::sync::Mutex<()>,
    created: Instant,
    pub counters: Counters,
}

impl std::fmt::Debug for CacheState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheState")
            .field("inner", &self.inner)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Drop for CacheState {
    fn drop(&mut self) {
        if let Some(task) = self.sync_task.lock().take() {
            task.abort();
        }
    }
}

impl CacheState {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, config: StoreCacheConfig) -> Self {
        Self {
            inner,
            config,
            index: RwLock::default(),
            seq: AtomicU64::new(1),
            fills: Mutex::default(),
            too_large: Mutex::default(),
            sync_task: Mutex::default(),
            sync_lock: tokio::sync::Mutex::default(),
            created: Instant::now(),
            counters: Counters::default(),
        }
    }

    /// Take the next sequence number. Take it before the read from the source.
    pub(crate) fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Milliseconds since the cache start.
    pub(crate) fn now_ms(&self) -> u64 {
        u64::try_from(self.created.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    pub(crate) fn count(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// True when the path has a segment that always reads from the source.
    pub(crate) fn bypasses(&self, key: &str) -> bool {
        key.split('/')
            .any(|segment| self.config.bypass_segments.iter().any(|skip| skip == segment))
    }

    /// True when a cover with a recent sync holds `key`.
    pub(crate) fn is_fresh(&self, key: &str, inclusive: bool) -> bool {
        self.index
            .read()
            .is_fresh(key, inclusive, self.config.max_age(), self.now_ms())
    }

    /// True when the index keeps changes of `key`: below a cover or a listing in flight.
    pub(crate) fn records(&self, key: &str) -> bool {
        if self.index.read().has_cover(key, false) {
            return true;
        }
        self.fills.lock().keys().any(|prefix| is_under(key, prefix))
    }

    /// True when a listing of `prefix` went over the entry limit a short time ago.
    pub(crate) fn is_too_large(&self, prefix: &str) -> bool {
        // Only the same prefix counts: a folder below it can fit the limit.
        self.too_large
            .lock()
            .get(prefix)
            .is_some_and(|at| at.elapsed() < self.config.interval())
    }

    pub(crate) fn upsert(&self, key: &str, meta: Option<ObjectMeta>, seq: u64) {
        self.index.write().upsert(key, meta, seq);
    }

    /// Record that `location` is gone.
    pub(crate) fn tombstone(&self, location: &Path) {
        let key = location.as_ref();
        if self.records(key) {
            self.upsert(key, None, self.next_seq());
        }
    }

    /// Read `location` from the source and store the result.
    pub(crate) async fn refresh(&self, location: &Path) {
        let key = location.as_ref();
        if !self.records(key) {
            return;
        }
        let seq = self.next_seq();
        match head_uncached(self.inner.as_ref(), location).await {
            Ok(meta) => self.upsert(key, Some(meta), seq),
            Err(Error::NotFound { .. }) => self.upsert(key, None, seq),
            Err(error) => {
                tracing::warn!(path = key, %error, "store cache: metadata read failed, prefix dropped");
                self.index.write().uncover(key, false);
            }
        }
    }

    /// Correct the index with metadata that a read from the source returned.
    pub(crate) fn observe(&self, key: &str, meta: &ObjectMeta, seq: u64) {
        // `last_modified` is not compared: its precision differs between list and get.
        let same = self.index.read().get(key).is_some_and(|entry| {
            entry.meta.as_ref().is_some_and(|known| {
                known.size == meta.size
                    && (known.e_tag.is_none() || meta.e_tag.is_none() || known.e_tag == meta.e_tag)
            })
        });
        if !same {
            self.upsert(key, Some(meta.clone()), seq);
        }
    }

    pub(crate) fn status(&self) -> CacheStatus {
        let (entries, covered) = {
            let index = self.index.read();
            (index.len(), index.cover_prefixes())
        };
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        CacheStatus {
            entries,
            covered,
            hits: load(&self.counters.hits),
            misses: load(&self.counters.misses),
            bypassed: load(&self.counters.bypassed),
            last_sync: *self.counters.last_sync.lock(),
            last_sync_ms: load(&self.counters.last_sync_ms),
            sync_errors: load(&self.counters.sync_errors),
        }
    }
}

/// Object path for an index key. The root has no path.
pub(crate) fn to_path(key: &str) -> Option<Path> {
    if key.is_empty() {
        None
    } else {
        Path::parse(key).ok()
    }
}
