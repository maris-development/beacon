//! Shared state of one cached store.
//!
//! The state has no lock around its data. The index is a lock-free sorted
//! map, so a read does not wait for a write or for the background sync.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::{Error, GetOptions, ObjectMeta, ObjectStore};
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::config::StoreCacheConfig;
use crate::index::Index;

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

#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub bypassed: AtomicU64,
    pub sync_errors: AtomicU64,
    pub last_sync_ms: AtomicU64,
    /// End time of the last sync in Unix milliseconds. Zero means no sync.
    pub last_sync: AtomicI64,
}

/// Counters and sizes of one cache.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CacheStatus {
    /// Objects in the cache.
    pub entries: usize,
    /// True when the cache holds a full listing of the store.
    pub synced: bool,
    /// Calls answered from the cache.
    pub hits: u64,
    /// Calls that waited for a listing of the store.
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
    pub index: Index,
    seq: AtomicU64,
    /// End of the last good sync, in milliseconds since the cache start. Zero means no sync.
    synced_ms: AtomicU64,
    /// Time of the last read, in milliseconds since the cache start.
    used_ms: AtomicU64,
    pub sync_task: OnceLock<JoinHandle<()>>,
    /// Held during a listing of the source, so only one listing runs.
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
        if let Some(task) = self.sync_task.get() {
            task.abort();
        }
    }
}

impl CacheState {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, config: StoreCacheConfig) -> Self {
        Self {
            inner,
            config,
            index: Index::default(),
            seq: AtomicU64::new(1),
            synced_ms: AtomicU64::new(0),
            used_ms: AtomicU64::new(0),
            sync_task: OnceLock::new(),
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

    /// True when the index holds a listing with a recent sync. Records the read.
    pub(crate) fn is_fresh(&self) -> bool {
        let now = self.now_ms();
        self.used_ms.store(now, Ordering::Relaxed);
        let synced = self.synced_ms.load(Ordering::Relaxed);
        let max_age = u64::try_from(self.config.max_age().as_millis()).unwrap_or(u64::MAX);
        synced != 0 && self.index.is_synced() && now.saturating_sub(synced) <= max_age
    }

    /// Milliseconds since the last read.
    pub(crate) fn idle_ms(&self) -> u64 {
        self.now_ms().saturating_sub(self.used_ms.load(Ordering::Relaxed))
    }

    /// Record a good sync that ended now.
    pub(crate) fn mark_synced(&self) {
        self.synced_ms.store(self.now_ms().max(1), Ordering::Relaxed);
    }

    /// Make the next read list the source again.
    pub(crate) fn mark_stale(&self) {
        self.synced_ms.store(0, Ordering::Relaxed);
    }

    pub(crate) fn upsert(&self, key: &str, meta: Option<ObjectMeta>, seq: u64) {
        self.index.upsert(key, meta, seq);
    }

    /// Record that `location` is gone.
    pub(crate) fn tombstone(&self, location: &Path) {
        self.upsert(location.as_ref(), None, self.next_seq());
    }

    /// Read `location` from the source and store the result.
    pub(crate) async fn refresh(&self, location: &Path) {
        let key = location.as_ref();
        let seq = self.next_seq();
        match head_uncached(self.inner.as_ref(), location).await {
            Ok(meta) => self.upsert(key, Some(meta), seq),
            Err(Error::NotFound { .. }) => self.upsert(key, None, seq),
            Err(error) => {
                tracing::warn!(path = key, %error, "store cache: metadata read failed, cache marked stale");
                self.mark_stale();
            }
        }
    }

    /// Correct the index with metadata that a read from the source returned.
    pub(crate) fn observe(&self, key: &str, meta: &ObjectMeta, seq: u64) {
        // `last_modified` is not compared: its precision differs between list and get.
        let same = self.index.get(key).is_some_and(|known| {
            known.size == meta.size
                && (known.e_tag.is_none() || meta.e_tag.is_none() || known.e_tag == meta.e_tag)
        });
        if !same {
            self.upsert(key, Some(meta.clone()), seq);
        }
    }

    pub(crate) fn status(&self) -> CacheStatus {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let last_sync = match self.counters.last_sync.load(Ordering::Relaxed) {
            0 => None,
            millis => DateTime::from_timestamp_millis(millis),
        };
        CacheStatus {
            entries: self.index.len(),
            synced: self.index.is_synced(),
            hits: load(&self.counters.hits),
            misses: load(&self.counters.misses),
            bypassed: load(&self.counters.bypassed),
            last_sync,
            last_sync_ms: load(&self.counters.last_sync_ms),
            sync_errors: load(&self.counters.sync_errors),
        }
    }
}
