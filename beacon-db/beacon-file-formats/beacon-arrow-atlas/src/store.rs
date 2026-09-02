//! Finding an Atlas collection in a listing, opening it, and caching the
//! handle.
//!
//! A collection is a store prefix holding one required object, `data.atlas`,
//! and one optional sidecar, `deleted.mask`. The container object is the
//! *marker*: it is what a listing matches, what a plan entry carries, and what
//! the reader cache keys on. Its parent directory is the prefix
//! [`atlas::Atlas::open`] takes.

use std::sync::Arc;

use atlas::Atlas;
use chrono::{DateTime, Utc};
use moka::future::Cache;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as OsPath};

/// The container object at the root of a collection.
pub const ATLAS_MARKER: &str = "data.atlas";

/// The deletion-mask sidecar beside it. Absent means nothing is deleted.
pub const ATLAS_MASK: &str = "deleted.mask";

/// `data.atlas` as it appears at the end of a nested path.
const MARKER_SUFFIX: &str = "/data.atlas";

/// Whether `path` names a collection's container object.
///
/// The name is fixed. [`atlas::Atlas::open`] resolves `data.atlas` under the
/// prefix it is given, so a collection renamed to `sensor.atlas` cannot be
/// opened at all and is not a marker.
pub fn is_marker_path(path: &OsPath) -> bool {
    let path = path.as_ref();
    path == ATLAS_MARKER || path.ends_with(MARKER_SUFFIX)
}

/// Whether `obj` is a collection's container object.
pub fn is_atlas_marker(obj: &ObjectMeta) -> bool {
    is_marker_path(&obj.location)
}

/// The prefix a collection is opened under: the marker's parent directory.
///
/// An empty path means the marker sits at the store root, which is what a
/// store rooted on the collection's own directory reports.
pub fn collection_prefix(marker: &OsPath) -> Option<OsPath> {
    let path = marker.as_ref();
    if path == ATLAS_MARKER {
        return Some(OsPath::default());
    }
    path.strip_suffix(MARKER_SUFFIX).map(OsPath::from)
}

/// The directory of a marker, as a string. `""` for one at the root.
fn marker_directory(marker: &OsPath) -> Option<String> {
    let path = marker.as_ref();
    if path == ATLAS_MARKER {
        return Some(String::new());
    }
    path.strip_suffix(MARKER_SUFFIX).map(str::to_string)
}

/// Reduce `objects` to the unique outermost collection markers.
///
/// Two markers at two depths of one tree keep only the ancestor: a collection
/// is one file and never contains another, so a deeper marker is a collection
/// that happens to sit inside another's directory and would be read twice.
pub fn top_level_atlas_markers(objects: &[ObjectMeta]) -> Vec<ObjectMeta> {
    // By directory, not by path. A path sort would put "a/b/data.atlas" before
    // "a/data.atlas", because 'b' sorts under 'd', and the nested collection
    // would then be the one kept.
    let mut markers: Vec<(String, &ObjectMeta)> = objects
        .iter()
        .filter_map(|object| {
            marker_directory(&object.location).map(|directory| (directory, object))
        })
        .collect();
    markers.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut kept: Vec<(String, ObjectMeta)> = Vec::new();
    'outer: for (directory, marker) in markers {
        for (held, _) in &kept {
            // A marker at the root sits above every path, but the collection
            // beside it is its own, so an empty directory excludes nothing.
            if !held.is_empty() && directory.starts_with(&format!("{held}/")) {
                continue 'outer;
            }
        }
        kept.push((directory, marker.clone()));
    }
    kept.into_iter().map(|(_, marker)| marker).collect()
}

/// Open the collection whose container object is `marker`, over `store`.
///
/// One `HEAD`, one tail read, and one `GET` of the deletion mask when it
/// exists. Nothing else, whatever the collection holds.
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
///
/// The container never changes after a write, so its size and modification time
/// pin its contents completely. The mask is the one part of a finished
/// collection that can change, and it decides which datasets a handle reports,
/// so it belongs in the key.
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
///
/// Cloning shares the underlying [`moka`] cache, so the formats, sources and
/// openers a runtime hands a clone to all draw from one store. This is
/// per-runtime state; there is no process-global cache.
///
/// Each entry owns a 256 MiB block cache and a 64 MiB I/O cache of its own, so
/// the capacity is a memory bound as much as a handle count. See
/// [`AtlasConfig::reader_cache_size`](crate::AtlasConfig::reader_cache_size).
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
/// One `HEAD`. An error other than "not found" also reads as `None`: the mask
/// only ever *hides* datasets, so the worst a stale handle can do is report a
/// dataset a concurrent delete just hid, and the alternative is failing a query
/// over a transient head request.
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
/// With `cache` set to `None` the collection is opened directly, with no
/// caching. Otherwise the key carries the marker's identity and the mask's, so
/// a rewritten collection or a fresh delete produces a new key and a re-open.
/// Concurrent first readers of one key coalesce inside
/// [`moka::future::Cache::try_get_with`].
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
        .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;

    fn object(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: OsPath::from(path),
            last_modified: DateTime::UNIX_EPOCH,
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    // ── markers ─────────────────────────────────────────────────────────

    #[test]
    fn the_container_object_is_the_marker() {
        assert!(is_atlas_marker(&object("data.atlas")));
        assert!(is_atlas_marker(&object("store/data.atlas")));
        assert!(is_atlas_marker(&object("a/b/c/data.atlas")));
    }

    #[test]
    fn nothing_else_is_a_marker() {
        // The mask sits beside the container and must never be read as one.
        // Neither must the registry of a pre-0.16 collection: this build reads
        // only the single-file format, so an old collection left on disk is
        // passed over rather than misread.
        for path in [
            "deleted.mask",
            "store/deleted.mask",
            "store/data.atlas.tmp",
            "store/mydata.atlas",
            "data.atlas/inner",
            "store/atlas.json",
        ] {
            assert!(!is_atlas_marker(&object(path)), "{path}");
        }
    }

    #[test]
    fn the_prefix_is_the_marker_directory() {
        assert_eq!(
            collection_prefix(&OsPath::from("a/b/data.atlas")),
            Some(OsPath::from("a/b"))
        );
        assert_eq!(
            collection_prefix(&OsPath::from("data.atlas")),
            Some(OsPath::default())
        );
        assert_eq!(collection_prefix(&OsPath::from("a/b/other.txt")), None);
    }

    #[test]
    fn top_level_markers_drop_a_nested_collection() {
        let objects = vec![
            object("a/data.atlas"),
            object("a/b/data.atlas"),
            object("c/data.atlas"),
            object("c/deleted.mask"),
        ];
        let kept: Vec<String> = top_level_atlas_markers(&objects)
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        assert_eq!(kept, vec!["a/data.atlas", "c/data.atlas"]);
    }

    #[test]
    fn a_sibling_directory_is_not_nested() {
        // "argo2" starts with "argo", but it is not under it.
        let objects = vec![object("argo/data.atlas"), object("argo2/data.atlas")];
        assert_eq!(top_level_atlas_markers(&objects).len(), 2);
    }

    // ── opening ─────────────────────────────────────────────────────────

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

    // ── the reader cache ────────────────────────────────────────────────

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
