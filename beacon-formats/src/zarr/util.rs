use std::collections::{HashMap, HashSet};

use object_store::ObjectStore;

#[derive(Clone, Debug)]
pub enum ZarrPath {
    ObjectMeta(object_store::ObjectMeta),
    // Directory path representing a Zarr group. Inside there should always be a "zarr.json" file.
    DirPath(object_store::path::Path),
}

impl ZarrPath {
    pub fn as_zarr_path(&self) -> String {
        match self {
            ZarrPath::ObjectMeta(meta) => {
                // Strip the trailing "zarr.json" to get the group path.
                let loc = meta.location.as_ref();
                loc.strip_suffix("zarr.json").unwrap_or(loc).to_string()
            }
            ZarrPath::DirPath(path) => {
                // Check if ends with '/' and return as is or add it.
                let p_str = path.as_ref();
                if p_str.ends_with('/') {
                    p_str.to_string()
                } else {
                    format!("{}/", p_str)
                }
            }
        }
    }

    pub fn new_from_object_meta(meta: object_store::ObjectMeta) -> Result<Self, String> {
        // Ensure this is a zarr.json file?
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
        // Ensure that path does not end with "zarr.json" and that in the folder underneath there is a zarr.json file.
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
/// Example: "a/b/c" -> Some("a/b")
///          "a"     -> None
pub fn path_parent(p: &object_store::path::Path) -> Option<object_store::path::Path> {
    let s = p.to_string();
    if let Some(pos) = s.rfind('/') {
        // Parent is everything before the last '/'
        let parent_str = &s[..pos];
        Some(object_store::path::Path::from(parent_str))
    } else {
        // No '/' in path => parent is root (optional: return None instead)
        None
    }
}

/// Check if this ObjectMeta represents a Zarr v3 metadata file ("zarr.json")
pub fn is_zarr_v3_metadata(meta: &object_store::ObjectMeta) -> bool {
    // Normalize for safety, S3 paths are UTF-8 so this is fine
    let loc = meta.location.to_string().to_lowercase();
    loc.ends_with("/zarr.json")
}

/// Return only the ObjectMeta entries corresponding to **top-level Zarr groups**.
pub fn top_level_zarr_meta_v3(metas: &[object_store::ObjectMeta]) -> Vec<object_store::ObjectMeta> {
    // 1. Collect all metas that are zarr.json + record their directories.
    let mut dir_to_meta: HashMap<object_store::path::Path, &object_store::ObjectMeta> =
        HashMap::new();

    for meta in metas {
        if is_zarr_v3_metadata(meta)
            && let Some(parent) = path_parent(&meta.location)
        {
            dir_to_meta.insert(parent, meta);
        }
    }

    // 2. Extract all candidate directories and sort.
    let mut candidates: Vec<object_store::path::Path> = dir_to_meta.keys().cloned().collect();
    candidates.sort();
    candidates.dedup();

    // 3. Build a set for quick ancestor checks.
    let all_dirs: HashSet<object_store::path::Path> = candidates.iter().cloned().collect();

    // 4. Keep only directories that have no ancestor also in all_dirs.
    let mut top_level_dirs: Vec<object_store::path::Path> = Vec::new();

    'outer: for dir in &candidates {
        let mut current = path_parent(dir);
        while let Some(ancestor) = current {
            if ancestor == *dir {
                break;
            }
            if all_dirs.contains(&ancestor) {
                // dir is nested under another zarr.json directory
                continue 'outer;
            }
            current = path_parent(&ancestor);
        }
        top_level_dirs.push(dir.clone());
    }

    // 5. Return the corresponding ObjectMetas (cloned)
    top_level_dirs
        .into_iter()
        .filter_map(|d| dir_to_meta.get(&d).cloned().cloned())
        .collect()
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
        let metas = vec![
            meta("a/zarr.json"),   // top-level
            meta("a/b/zarr.json"), // nested -> drop
        ];
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
    fn test_deep_nesting_dropped() {
        let metas = vec![
            meta("a/zarr.json"),
            meta("a/b/zarr.json"),
            meta("a/b/c/zarr.json"),
        ];
        let result = top_level_zarr_meta_v3(&metas);
        assert_eq!(extract_paths(&result), vec!["a/zarr.json"]);
    }

    #[test]
    fn test_sibling_groups() {
        let metas = vec![
            meta("root1/zarr.json"),
            meta("root2/zarr.json"),
            meta("root2/child/zarr.json"), // nested -> drop
        ];
        let result = top_level_zarr_meta_v3(&metas);
        let mut paths = extract_paths(&result);
        paths.sort();
        assert_eq!(paths, vec!["root1/zarr.json", "root2/zarr.json"]);
    }

    #[test]
    fn test_root_level_key() {
        let metas = vec![
            meta("zarr.json"),   // top-level at root
            meta("a/zarr.json"), // nested under root -> drop
        ];
        let result = top_level_zarr_meta_v3(&metas);
        assert_eq!(extract_paths(&result), vec!["a/zarr.json"]);
    }

    #[test]
    fn test_ignore_non_zarr_json() {
        let metas = vec![
            meta("a/zarr.json"), // valid
            meta("b/not_zarr.json"),
            meta("c/zarr.txt"),
            meta("d/data.bin"),
        ];
        let result = top_level_zarr_meta_v3(&metas);
        assert_eq!(extract_paths(&result), vec!["a/zarr.json"]);
    }

    #[test]
    fn test_empty_input() {
        let metas: Vec<ObjectMeta> = vec![];
        let result = top_level_zarr_meta_v3(&metas);
        assert!(result.is_empty());
    }
}
