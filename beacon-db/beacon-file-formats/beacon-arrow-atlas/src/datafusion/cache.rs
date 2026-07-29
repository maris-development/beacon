//! Cache of opened atlas stores keyed by marker path plus freshness
//! (`last_modified` + `size`). Callers that want an `Arc<Atlas>` go through
//! [`get_or_open_atlas`] so a single store is opened once for as long as its
//! on-disk metadata is unchanged.
//!
//! The cache is owned per-runtime ([`AtlasReaderCache`]) rather than being a
//! process-global static; passing `None` opens directly with no caching.

use std::sync::Arc;

use atlas::Atlas;
use moka::future::Cache;
use object_store::{ObjectMeta, ObjectStore, path::Path as OsPath};

use crate::util::atlas_store_prefix;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    path: OsPath,
    last_modified: chrono::DateTime<chrono::Utc>,
    size: u64,
}

/// A reader cache for opened atlas stores, sized at construction time.
///
/// Cloning shares the underlying [`moka`] cache (reference-counted internally),
/// so one instance is shared across the formats, sources and openers a runtime
/// hands a clone to. Per-runtime state — there is no process-global cache.
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

// `Atlas` is not `Debug`; the cache is embedded in `Debug` structs
// (formats/sources), so provide an opaque impl.
impl std::fmt::Debug for AtlasReaderCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasReaderCache").finish_non_exhaustive()
    }
}

/// Open the atlas store whose marker lives at `marker.location`, over `store`.
///
/// Atlas opens natively over the [`object_store`] backend: the store prefix is
/// the marker's parent directory, and the metadata variant is auto-detected
/// from the files present.
async fn open_atlas_store(
    store: Arc<dyn ObjectStore>,
    marker_path: &OsPath,
) -> datafusion::error::Result<Arc<Atlas>> {
    let prefix = atlas_store_prefix(marker_path).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "Path {marker_path} is not an atlas metadata marker"
        ))
    })?;
    let atlas = Atlas::open(store, prefix.clone()).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open atlas store at prefix '{prefix}': {e}"
        ))
    })?;
    Ok(Arc::new(atlas))
}

/// Return a cached [`Arc<Atlas>`] for `marker`, opening from `store` on miss.
///
/// When `cache` is `None`, the store is opened directly with no caching.
/// Otherwise freshness is encoded in the cache key — a marker whose
/// `last_modified` or `size` differs from the cached entry produces a new key,
/// forcing a re-open. Concurrent first-readers for the same key coalesce inside
/// [`moka::future::Cache::try_get_with`].
pub async fn get_or_open_atlas(
    cache: Option<&AtlasReaderCache>,
    store: Arc<dyn ObjectStore>,
    marker: &ObjectMeta,
) -> datafusion::error::Result<Arc<Atlas>> {
    let Some(cache) = cache else {
        return open_atlas_store(store, &marker.location).await;
    };

    let key = CacheKey {
        path: marker.location.clone(),
        last_modified: marker.last_modified,
        size: marker.size,
    };
    let path = marker.location.clone();

    cache
        .cache
        .try_get_with(key, async move { open_atlas_store(store, &path).await })
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
    use crate::datafusion::test_support::{fixture_marker_object_meta, test_store};

    #[tokio::test]
    async fn cache_returns_same_arc_for_identical_marker() {
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
            "identical marker must hit the cache"
        );
    }

    #[tokio::test]
    async fn cache_reopens_when_last_modified_changes() {
        let store = test_store().await;
        let base = fixture_marker_object_meta();
        let mut bumped = base.clone();
        bumped.last_modified = base.last_modified + chrono::Duration::seconds(1);
        let cache = AtlasReaderCache::new(32);

        let first = get_or_open_atlas(Some(&cache), store.clone(), &base)
            .await
            .expect("first open");
        let second = get_or_open_atlas(Some(&cache), store, &bumped)
            .await
            .expect("second open");

        assert!(
            !Arc::ptr_eq(&first, &second),
            "bumped last_modified must invalidate the cache"
        );
    }
}
