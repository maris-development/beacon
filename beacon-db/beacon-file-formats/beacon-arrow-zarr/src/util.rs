//! Zarr path handling and group-discovery helpers shared by the DataFusion
//! integration.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use object_store::{ObjectStore, ObjectStoreExt};
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

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
pub fn is_zarr_v3_metadata(meta: &object_store::ObjectMeta) -> bool {
    let loc = meta.location.to_string().to_lowercase();
    loc.ends_with("/zarr.json")
}

/// Return only the ObjectMeta entries corresponding to **top-level Zarr groups**.
pub fn top_level_zarr_meta_v3(metas: &[object_store::ObjectMeta]) -> Vec<object_store::ObjectMeta> {
    let mut dir_to_meta: HashMap<object_store::path::Path, &object_store::ObjectMeta> =
        HashMap::new();

    for meta in metas {
        if is_zarr_v3_metadata(meta)
            && let Some(parent) = path_parent(&meta.location)
        {
            dir_to_meta.insert(parent, meta);
        }
    }

    let mut candidates: Vec<object_store::path::Path> = dir_to_meta.keys().cloned().collect();
    candidates.sort();
    candidates.dedup();

    let all_dirs: HashSet<object_store::path::Path> = candidates.iter().cloned().collect();

    let mut top_level_dirs: Vec<object_store::path::Path> = Vec::new();
    'outer: for dir in &candidates {
        let mut current = path_parent(dir);
        while let Some(ancestor) = current {
            if ancestor == *dir {
                break;
            }
            if all_dirs.contains(&ancestor) {
                continue 'outer;
            }
            current = path_parent(&ancestor);
        }
        top_level_dirs.push(dir.clone());
    }

    top_level_dirs
        .into_iter()
        .filter_map(|d| dir_to_meta.get(&d).cloned().cloned())
        .collect()
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

    fn extract_paths(metas: &[ObjectMeta]) -> Vec<String> {
        metas.iter().map(|m| m.location.to_string()).collect()
    }

    #[test]
    fn test_single_top_level() {
        let metas = vec![meta("a/zarr.json")];
        let result = top_level_zarr_meta_v3(&metas);
        assert_eq!(extract_paths(&result), vec!["a/zarr.json"]);
    }

    #[test]
    fn test_nested_group_dropped() {
        let metas = vec![meta("a/zarr.json"), meta("a/b/zarr.json")];
        let result = top_level_zarr_meta_v3(&metas);
        assert_eq!(extract_paths(&result), vec!["a/zarr.json"]);
    }

    #[test]
    fn test_multiple_top_level() {
        let metas = vec![meta("a/zarr.json"), meta("b/zarr.json")];
        let result = top_level_zarr_meta_v3(&metas);
        let mut paths = extract_paths(&result);
        paths.sort();
        assert_eq!(paths, vec!["a/zarr.json", "b/zarr.json"]);
    }

    #[test]
    fn test_ignore_non_zarr_json() {
        let metas = vec![
            meta("a/zarr.json"),
            meta("b/not_zarr.json"),
            meta("c/zarr.txt"),
        ];
        let result = top_level_zarr_meta_v3(&metas);
        assert_eq!(extract_paths(&result), vec!["a/zarr.json"]);
    }

    #[test]
    fn deeply_nested_children_are_all_dropped() {
        // Only the shallowest ancestor group survives.
        let metas = vec![
            meta("root/zarr.json"),
            meta("root/a/zarr.json"),
            meta("root/a/b/zarr.json"),
            meta("other/zarr.json"),
        ];
        let mut result = extract_paths(&top_level_zarr_meta_v3(&metas));
        result.sort();
        assert_eq!(result, vec!["other/zarr.json", "root/zarr.json"]);
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
