//! Opening an Atlas collection, and caching the handle.
//!
//! The reader cache keys on the marker and the deletion mask, so a rewritten
//! collection or a fresh delete reopens, and an unchanged one does not.

use std::sync::Arc;

use atlas::Atlas;
use chrono::{DateTime, Utc};
use moka::future::Cache;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as OsPath};

use crate::discover::{ATLAS_MARKER, ATLAS_MASK, collection_prefix};

/// Open the collection whose container object is `marker`, over `store`.
///
/// One `HEAD`, one tail read, and one `GET` of the mask if it exists.
pub async fn open_collection(
    store: Arc<dyn ObjectStore>,
    marker: &OsPath,
) -> anyhow::Result<Arc<Atlas>> {
    let prefix = collection_prefix(marker).ok_or_else(|| {
        anyhow::anyhow!(
            "'{marker}' is not an atlas collection: the container is named '{ATLAS_MARKER}'"
        )
    })?;
    let atlas = Atlas::open(store, prefix.clone())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open the atlas collection at '{prefix}': {e}"))?;
    Ok(Arc::new(atlas))
}

/// What a cached handle describes, beyond the container itself.
/// The mask can change after a write and decides which datasets a handle reports.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MaskStamp {
    last_modified: DateTime<Utc>,
    size: u64,
    e_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    path: OsPath,
    last_modified: DateTime<Utc>,
    size: u64,
    /// `None` when the collection has no mask, which is the common case.
    mask: Option<MaskStamp>,
}

/// A cache of opened collections, sized at construction.
/// Cloning shares the underlying [`moka`] cache; this is per-runtime state, not global.
#[derive(Clone)]
pub struct AtlasReaderCache {
    cache: Cache<CacheKey, Arc<Atlas>>,
}

impl AtlasReaderCache {
    /// Build a cache holding up to `capacity` opened collections.
    pub fn new(capacity: u64) -> Self {
        Self {
            cache: Cache::builder().max_capacity(capacity).build(),
        }
    }
}

// `Atlas` is not `Debug`, and this sits inside `Debug` formats and sources.
impl std::fmt::Debug for AtlasReaderCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasReaderCache").finish_non_exhaustive()
    }
}

/// The identity of a collection's deletion mask, or `None` when it has none.
///
/// One `HEAD`. Any other error also reads as `None`, so a transient failure never fails a query.
async fn mask_stamp(store: &dyn ObjectStore, prefix: &OsPath) -> Option<MaskStamp> {
    let path = prefix.clone().join(ATLAS_MASK);
    match store.head(&path).await {
        Ok(meta) => Some(MaskStamp {
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
        }),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(e) => {
            tracing::debug!(path = %path, error = %e, "could not stat the atlas deletion mask");
            None
        }
    }
}

/// A cached handle for `marker`, opening it from `store` on a miss.
///
/// With `cache` as `None`, opens directly. Otherwise keys on the marker and mask, so a change reopens.
pub async fn get_or_open_atlas(
    cache: Option<&AtlasReaderCache>,
    store: Arc<dyn ObjectStore>,
    marker: &ObjectMeta,
) -> anyhow::Result<Arc<Atlas>> {
    let Some(cache) = cache else {
        return open_collection(store, &marker.location).await;
    };

    let prefix = collection_prefix(&marker.location).ok_or_else(|| {
        anyhow::anyhow!(
            "'{}' is not an atlas collection: the container is named '{ATLAS_MARKER}'",
            marker.location
        )
    })?;
    let key = CacheKey {
        path: marker.location.clone(),
        last_modified: marker.last_modified,
        size: marker.size,
        mask: mask_stamp(store.as_ref(), &prefix).await,
    };

    let path = marker.location.clone();
    cache
        .cache
        .try_get_with(key, async move { open_collection(store, &path).await })
        .await
        // The cache shares one error among waiters, so it can't be moved out; keep its chain as text.
        .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!("{e:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;

    #[tokio::test]
    async fn a_collection_opens_from_its_marker() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (store, marker) = test_support::store_and_marker(tmp.path());

        let atlas = open_collection(store, &marker.location).await.unwrap();
        assert_eq!(
            atlas.list_datasets(),
            vec!["winter", "summer"],
            "a collection lists in write order"
        );
    }

    /// Anything but the container is refused, and the error names what a
    /// collection is called.
    #[tokio::test]
    async fn a_path_that_is_not_the_container_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, _) = test_support::store_and_marker(tmp.path());
        let error = open_collection(store, &OsPath::from("store/index.json"))
            .await
            .expect_err("only the container names a collection")
            .to_string();
        assert!(error.contains("data.atlas"), "{error}");
    }

    #[tokio::test]
    async fn one_marker_opens_once() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (store, marker) = test_support::store_and_marker(tmp.path());
        let cache = AtlasReaderCache::new(8);

        let first = get_or_open_atlas(Some(&cache), store.clone(), &marker)
            .await
            .unwrap();
        let second = get_or_open_atlas(Some(&cache), store, &marker)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second), "the second open must hit");
    }

    #[tokio::test]
    async fn a_rewritten_container_reopens() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (store, marker) = test_support::store_and_marker(tmp.path());
        let cache = AtlasReaderCache::new(8);

        let first = get_or_open_atlas(Some(&cache), store.clone(), &marker)
            .await
            .unwrap();

        let mut moved = marker.clone();
        moved.last_modified = marker.last_modified + chrono::Duration::seconds(1);
        let second = get_or_open_atlas(Some(&cache), store, &moved)
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second), "a new mtime must miss");
    }

    /// A delete writes the mask and leaves the container alone, so the marker
    /// says nothing about it. Without the mask in the key, a handle opened
    /// before the delete keeps reporting the dataset it hid.
    #[tokio::test]
    async fn a_delete_reopens_the_collection() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (store, marker) = test_support::store_and_marker(tmp.path());
        let cache = AtlasReaderCache::new(8);

        let before = get_or_open_atlas(Some(&cache), store.clone(), &marker)
            .await
            .unwrap();
        assert_eq!(before.list_datasets().len(), 2);

        before.delete_dataset("winter").await.unwrap();

        let after = get_or_open_atlas(Some(&cache), store, &marker)
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&before, &after),
            "the mask changed, so the key did"
        );
        assert_eq!(after.list_datasets(), vec!["summer"]);
    }

    #[tokio::test]
    async fn without_a_cache_every_open_is_its_own() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (store, marker) = test_support::store_and_marker(tmp.path());

        let first = get_or_open_atlas(None, store.clone(), &marker)
            .await
            .unwrap();
        let second = get_or_open_atlas(None, store, &marker).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
    }
}
