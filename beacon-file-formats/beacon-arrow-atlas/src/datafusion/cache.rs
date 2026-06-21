//! Cache of opened atlas stores keyed by marker path plus freshness
//! (`last_modified` + `size`). Callers that want an `Arc<Atlas>` go through
//! [`get_or_open_atlas`] so a single `atlas.json` is opened once for as long as
//! its on-disk metadata is unchanged.
//!
//! The cache is owned per-runtime ([`AtlasReaderCache`]) rather than being a
//! process-global static; passing `None` opens directly with no caching.

use std::sync::Arc;

use atlas::Atlas;
use beacon_object_storage::DatasetsStore;
use moka::future::Cache;
use object_store::ObjectMeta;
use object_store::path::Path as OsPath;

use crate::datafusion::reader;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    path: OsPath,
    last_modified: chrono::DateTime<chrono::Utc>,
    size: u64,
}

/// A reader cache for opened atlas stores, sized at construction time.
///
/// Cloning shares the underlying [`moka`] cache (reference-counted internally),
/// so a single instance is shared across the formats, sources and openers a
/// runtime hands a clone to. This is per-runtime state — there is no
/// process-global cache.
#[derive(Clone)]
pub struct AtlasReaderCache {
    cache: Cache<CacheKey, Arc<Atlas>>,
}

impl AtlasReaderCache {
    /// Build a cache holding up to `capacity` opened atlas stores.
    pub fn new(capacity: u64) -> Self {
        Self {
            cache: Cache::builder().max_capacity(capacity).build(),
        }
    }
}

// `Atlas` is not `Debug`, so the cache contents can't be derived. The cache is
// embedded in `Debug` structs (formats/sources), so provide an opaque impl.
impl std::fmt::Debug for AtlasReaderCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasReaderCache").finish_non_exhaustive()
    }
}

/// Return a cached [`Arc<Atlas>`] for `marker`, opening from disk on miss.
///
/// When `cache` is `None`, the store is opened directly with no caching.
/// Otherwise freshness is encoded in the cache key — a marker whose
/// `last_modified` or `size` differs from the cached entry produces a new key,
/// forcing a re-open. The previous entry lingers until evicted by the LRU
/// bound. Concurrent first-readers for the same key coalesce inside
/// [`moka::future::Cache::try_get_with`].
pub async fn get_or_open_atlas(
    cache: Option<&AtlasReaderCache>,
    store: Arc<DatasetsStore>,
    marker: &ObjectMeta,
) -> datafusion::error::Result<Arc<Atlas>> {
    let Some(cache) = cache else {
        return reader::open_atlas_store(store, &marker.location).await;
    };

    let key = CacheKey {
        path: marker.location.clone(),
        last_modified: marker.last_modified,
        size: marker.size,
    };
    let path = marker.location.clone();

    cache
        .cache
        .try_get_with(key, async move { reader::open_atlas_store(store, &path).await })
        .await
        .map_err(|e: Arc<datafusion::error::DataFusionError>| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to open atlas store via cache: {e}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::test_support::{ensure_fixture, fixture_marker_object_meta, test_store};
    use object_store::path::Path as OsPath;

    fn marker_with(path: OsPath, last_modified: chrono::DateTime<chrono::Utc>, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: path,
            last_modified,
            size,
            e_tag: None,
            version: None,
        }
    }

    #[tokio::test]
    async fn cache_returns_same_arc_for_identical_marker() {
        ensure_fixture().await;
        let store = test_store().await;
        let marker = fixture_marker_object_meta();
        let cache = AtlasReaderCache::new(32);

        let first = get_or_open_atlas(Some(&cache), store.clone(), &marker)
            .await
            .expect("first open");
        let second = get_or_open_atlas(Some(&cache), store, &marker)
            .await
            .expect("second open");

        assert!(
            Arc::ptr_eq(&first, &second),
            "identical marker must hit the cache",
        );
    }

    #[tokio::test]
    async fn cache_reopens_when_last_modified_changes() {
        ensure_fixture().await;
        let store = test_store().await;
        let base = fixture_marker_object_meta();
        let bumped = marker_with(
            base.location.clone(),
            base.last_modified + chrono::Duration::seconds(1),
            base.size,
        );
        let cache = AtlasReaderCache::new(32);

        let first = get_or_open_atlas(Some(&cache), store.clone(), &base)
            .await
            .expect("first open");
        let second = get_or_open_atlas(Some(&cache), store, &bumped)
            .await
            .expect("second open");

        assert!(
            !Arc::ptr_eq(&first, &second),
            "bumped last_modified must invalidate the cache",
        );
    }

    #[tokio::test]
    async fn cache_reopens_when_size_changes() {
        ensure_fixture().await;
        let store = test_store().await;
        let base = fixture_marker_object_meta();
        let bumped = marker_with(base.location.clone(), base.last_modified, base.size + 1);
        let cache = AtlasReaderCache::new(32);

        let first = get_or_open_atlas(Some(&cache), store.clone(), &base)
            .await
            .expect("first open");
        let second = get_or_open_atlas(Some(&cache), store, &bumped)
            .await
            .expect("second open");

        assert!(
            !Arc::ptr_eq(&first, &second),
            "bumped size must invalidate the cache",
        );
    }
}
