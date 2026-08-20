//! Zarr path handling and group-discovery helpers shared by the DataFusion
//! integration.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

/// The storage a zarr group is opened over.
///
/// The reader is storage independent: everything after [`Group::async_open`] —
/// schema inference, the leaf-group walk, the `beacon-nd-array` scan and the
/// predicate pushdown — works the same whatever backs the store. This newtype
/// carries that storage through the DataFusion plumbing, which is otherwise
/// hard-wired to an [`ObjectStore`]. A plain zarr store wraps the session's
/// object store; an Icechunk repository wraps a repository session instead.
#[derive(Clone)]
pub struct ZarrStorage(Arc<dyn AsyncReadableListableStorageTraits>);

impl ZarrStorage {
    pub fn new(storage: Arc<dyn AsyncReadableListableStorageTraits>) -> Self {
        Self(storage)
    }

    /// The storage backed by an object store — the default for a listed zarr store.
    pub fn from_object_store(object_store: Arc<dyn ObjectStore>) -> Self {
        Self(Arc::new(AsyncObjectStore::new(object_store)))
    }

    pub fn inner(&self) -> Arc<dyn AsyncReadableListableStorageTraits> {
        self.0.clone()
    }
}

impl std::fmt::Debug for ZarrStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZarrStorage").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub enum ZarrPath {
    ObjectMeta(object_store::ObjectMeta),
    /// Directory path representing a Zarr group. Inside there should always be
    /// a `zarr.json` file.
    DirPath(object_store::path::Path),
}

impl ZarrPath {
    pub fn as_zarr_path(&self) -> String {
        let directory_path = match self {
            ZarrPath::ObjectMeta(meta) => {
                // Strip the trailing "zarr.json" to get the group path.
                let loc = meta.location.as_ref();
                loc.strip_suffix("zarr.json").unwrap_or(loc).to_string()
            }
            ZarrPath::DirPath(path) => path.as_ref().to_string(),
        };
        let p_str = directory_path.trim_end_matches('/');
        format!("/{p_str}")
    }

    pub fn as_zarr_json_path(&self) -> String {
        match self {
            ZarrPath::ObjectMeta(meta) => meta.location.as_ref().to_string(),
            ZarrPath::DirPath(path) => path.child("zarr.json").as_ref().to_string(),
        }
    }

    pub fn new_from_object_meta(meta: object_store::ObjectMeta) -> Result<Self, String> {
        if !is_zarr_v3_metadata(&meta) {
            return Err(format!(
                "ObjectMeta at location '{}' is not a Zarr v3 metadata file (zarr.json)",
                meta.location.as_ref()
            ));
        }
        Ok(ZarrPath::ObjectMeta(meta))
    }

    pub async fn new_from_dir_path(
        object_store: &dyn ObjectStore,
        path: object_store::path::Path,
    ) -> Result<Self, String> {
        if path.as_ref().ends_with("zarr.json") {
            return Err(format!(
                "Provided path '{}' should be a directory path, not a zarr.json file",
                path.as_ref()
            ));
        }

        let zarr_json_path = path.child("zarr.json");
        let exists = object_store.head(&zarr_json_path).await.is_ok();
        if !exists {
            return Err(format!(
                "No zarr.json file found under directory path '{}'",
                path.as_ref()
            ));
        }
        Ok(ZarrPath::DirPath(path))
    }
}

/// Get parent directory of a Path (S3-style).
/// Example: `a/b/c` -> `Some("a/b")`, `a` -> `None`.
pub fn path_parent(p: &object_store::path::Path) -> Option<object_store::path::Path> {
    let s = p.to_string();
    if let Some(pos) = s.rfind('/') {
        let parent_str = &s[..pos];
        Some(object_store::path::Path::from(parent_str))
    } else {
        None
    }
}

/// Check if this ObjectMeta represents a Zarr v3 metadata file (`zarr.json`).
///
/// A bare `zarr.json` — no parent directory — counts too: a repository-backed
/// store such as Icechunk exposes its root group there. Listing-based discovery
/// is unaffected, because [`is_zarr_store_root`] names a store by the directory
/// holding the marker and a root-level metadata file has none.
pub fn is_zarr_v3_metadata(meta: &object_store::ObjectMeta) -> bool {
    let loc = meta.location.to_string().to_lowercase();
    loc == "zarr.json" || loc.ends_with("/zarr.json")
}

/// The store key of a group's metadata file, given the group's zarr node path.
///
/// `/` (the root group) maps to `zarr.json`; `/a/b` maps to `a/b/zarr.json`.
pub fn group_metadata_key(node_path: &str) -> String {
    let trimmed = node_path.trim_matches('/');
    if trimmed.is_empty() {
        "zarr.json".to_string()
    } else {
        format!("{trimmed}/zarr.json")
    }
}

/// The metadata keys of the leaf groups under `group` — one scan partition each,
/// so nested sub-groups are read independently.
///
/// A group with no child groups is its own single leaf. Returns `None` when the
/// children could not be listed, mirroring [`find_partitioned_files`].
pub async fn leaf_group_keys(
    group: &Group<dyn AsyncReadableListableStorageTraits>,
) -> Option<Vec<String>> {
    let leaves = find_partitioned_files(group).await?;
    if leaves.is_empty() {
        return Some(vec![group_metadata_key(group.path().as_str())]);
    }
    Some(
        leaves
            .iter()
            .map(|leaf| group_metadata_key(leaf.path().as_str()))
            .collect(),
    )
}

/// Whether `meta` is the root marker of a Zarr store, judged from its own path.
///
/// A store is a `*.zarr` directory holding `zarr.json` directly beside its
/// arrays:
///
/// ```text
/// gridded-example.zarr/zarr.json              the store
/// gridded-example.zarr/lat/zarr.json          an array inside it
/// ```
///
/// Zarr v3 gives every group *and every array* a `zarr.json`, so a marker alone
/// says nothing about which one it is. The directory it sits in does: only the
/// store carries the `.zarr` suffix.
///
/// This is decided per object, with no reference to any other. That is what lets
/// discovery classify a listing as it streams, instead of holding every object
/// to compare ancestors. It is the only rule: an explicit `read_zarr` path
/// resolves its store the same way, so a path is either a store everywhere or
/// nowhere.
///
/// A bare `zarr.json` at the store root has no directory to name it, so it is
/// not a discovered dataset. Icechunk reaches its root group directly and does
/// not come through discovery.
pub fn is_zarr_store_root(meta: &object_store::ObjectMeta) -> bool {
    if !is_zarr_v3_metadata(meta) {
        return false;
    }
    path_parent(&meta.location)
        .and_then(|parent| parent.filename().map(|name| name.to_lowercase()))
        .is_some_and(|name| name.ends_with(".zarr"))
}

/// Recursively collect the leaf Zarr groups under `top_level_group`.
///
/// A group is a leaf when it has no child groups; the top-level group itself is
/// included when it has none.
pub async fn recursive_groups(
    top_level_group: Arc<Group<dyn AsyncReadableListableStorageTraits>>,
    zarr_groups: &mut Vec<Arc<Group<dyn AsyncReadableListableStorageTraits>>>,
) -> anyhow::Result<()> {
    let child_groups = top_level_group
        .async_child_groups()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list child groups: {e}"))?;

    if child_groups.is_empty() {
        zarr_groups.push(top_level_group.clone());
    } else {
        for child_group in child_groups {
            Box::pin(recursive_groups(Arc::new(child_group), zarr_groups)).await?;
        }
    }
    Ok(())
}

/// Recursively collect leaf sub-groups to treat as scan partitions.
///
/// Returns `None` if the group's children could not be listed.
pub async fn find_partitioned_files(
    group: &Group<dyn AsyncReadableListableStorageTraits>,
) -> Option<Vec<Group<dyn AsyncReadableListableStorageTraits>>> {
    match group.async_child_groups().await {
        Ok(children) => {
            let mut result = Vec::new();
            for child in children {
                if let Some(mut sub_children) = Box::pin(find_partitioned_files(&child)).await {
                    result.append(&mut sub_children);
                } else {
                    result.push(child);
                }
            }
            Some(result)
        }
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectMeta, path::Path};

    fn meta(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn a_marker_in_a_zarr_directory_is_a_store() {
        assert!(is_zarr_store_root(&meta("gridded-example.zarr/zarr.json")));
        assert!(is_zarr_store_root(&meta("deep/nested/path/cube.zarr/zarr.json")));
        // The suffix is matched case-insensitively, as the marker name is.
        assert!(is_zarr_store_root(&meta("CUBE.ZARR/zarr.json")));
    }

    /// Every array in a v3 store also has a `zarr.json`. The directory between
    /// it and the store is what tells them apart.
    #[test]
    fn a_marker_below_the_store_directory_is_an_array() {
        assert!(!is_zarr_store_root(&meta("gridded-example.zarr/lat/zarr.json")));
        assert!(!is_zarr_store_root(&meta("gridded-example.zarr/a/b/zarr.json")));
    }

    /// A directory without the suffix is not a store, however the marker looks.
    #[test]
    fn a_marker_outside_a_zarr_directory_is_not_a_store() {
        assert!(!is_zarr_store_root(&meta("a/zarr.json")));
        assert!(!is_zarr_store_root(&meta("root/zarr.json")));
    }

    /// A root-level marker has no directory to name it. Icechunk reaches its
    /// root group directly rather than through discovery.
    #[test]
    fn a_bare_marker_is_not_a_discovered_store() {
        assert!(!is_zarr_store_root(&meta("zarr.json")));
    }

    /// Non-markers are rejected before the directory is even considered.
    #[test]
    fn a_non_marker_is_never_a_store() {
        assert!(!is_zarr_store_root(&meta("cube.zarr/not_zarr.json")));
        assert!(!is_zarr_store_root(&meta("cube.zarr/data.nc")));
    }

    #[test]
    fn path_parent_walks_up_one_level() {
        assert_eq!(
            path_parent(&Path::from("a/b/c")),
            Some(Path::from("a/b"))
        );
        // A single-segment path has no parent.
        assert_eq!(path_parent(&Path::from("a")), None);
    }

    #[test]
    fn is_zarr_v3_metadata_matches_only_trailing_zarr_json() {
        assert!(is_zarr_v3_metadata(&meta("group/zarr.json")));
        // Case-insensitive.
        assert!(is_zarr_v3_metadata(&meta("group/ZARR.JSON")));
        // A file literally named zarr.json but not as the final path segment,
        // or a differently named file, is not v3 metadata.
        assert!(!is_zarr_v3_metadata(&meta("group/not_zarr.json")));
        assert!(!is_zarr_v3_metadata(&meta("zarr.json/data")));
    }

    #[test]
    fn zarr_path_from_object_meta_normalises_group_path() {
        let zp = ZarrPath::new_from_object_meta(meta("group/sub/zarr.json")).unwrap();
        // The group path strips the trailing zarr.json, drops the trailing slash,
        // and is rooted with a leading slash.
        assert_eq!(zp.as_zarr_path(), "/group/sub");
        // The json path is preserved verbatim.
        assert_eq!(zp.as_zarr_json_path(), "group/sub/zarr.json");
    }

    #[test]
    fn zarr_path_from_object_meta_rejects_non_metadata() {
        let err = ZarrPath::new_from_object_meta(meta("group/data.bin")).unwrap_err();
        assert!(err.contains("not a Zarr v3 metadata file"));
    }

    #[test]
    fn zarr_path_from_dir_path_derives_json_child() {
        let zp = ZarrPath::DirPath(Path::from("group/sub"));
        assert_eq!(zp.as_zarr_path(), "/group/sub");
        assert_eq!(zp.as_zarr_json_path(), "group/sub/zarr.json");
    }

    #[tokio::test]
    async fn new_from_dir_path_rejects_a_zarr_json_path() {
        let store = object_store::memory::InMemory::new();
        let err = ZarrPath::new_from_dir_path(&store, Path::from("group/zarr.json"))
            .await
            .unwrap_err();
        assert!(err.contains("should be a directory path"));
    }

    #[tokio::test]
    async fn new_from_dir_path_requires_existing_zarr_json() {
        let store = object_store::memory::InMemory::new();
        // Nothing written under the directory ⇒ no zarr.json ⇒ error.
        let err = ZarrPath::new_from_dir_path(&store, Path::from("missing"))
            .await
            .unwrap_err();
        assert!(err.contains("No zarr.json file found"));

        // Once a zarr.json exists under the directory, construction succeeds.
        store
            .put(
                &Path::from("present/zarr.json"),
                object_store::PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();
        let zp = ZarrPath::new_from_dir_path(&store, Path::from("present"))
            .await
            .unwrap();
        assert_eq!(zp.as_zarr_json_path(), "present/zarr.json");
    }
}
