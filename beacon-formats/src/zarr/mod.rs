use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{
            FileGroup, FileGroupPartitioner, FileScanConfig, FileScanConfigBuilder, FileSource,
        },
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};
use zarrs_object_store::AsyncObjectStore;

use crate::zarr::source::{ZarrSource, fetch_schema};

mod source;
mod stream_share;

pub struct ZarrFormatFactory;

impl Debug for ZarrFormatFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZarrFormatFactory").finish()
    }
}

impl ZarrFormatFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ZarrFormatFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl GetExt for ZarrFormatFactory {
    fn get_ext(&self) -> String {
        "zarr".to_string()
    }
}

impl FileFormatFactory for ZarrFormatFactory {
    fn create(
        &self,
        state: &dyn datafusion::catalog::Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn FileFormat>> {
        // Here you can use `state` and `format_options` to customize the creation of the FileFormat
        Ok(Arc::new(ZarrFormat))
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        Arc::new(ZarrFormat)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct ZarrFormat;

#[async_trait::async_trait]
impl FileFormat for ZarrFormat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        "zarr.json".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("zarr.json".to_string())
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let objects = top_level_zarr_meta_v3(objects);
        tracing::debug!("Top-level Zarr v3 groups: {:?}", objects);
        let mut schema = None;
        for object in objects {
            let object_schema = fetch_schema(store.clone(), &object.clone()).await?;
            schema = Some(object_schema);
        }
        Ok(schema.unwrap())
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Recreate file groups here. Only maintain top-level groups.
        let mut object_metas: Vec<ObjectMeta> = Vec::new();
        for group in &conf.file_groups {
            for file in group.files() {
                object_metas.push(file.object_meta.clone());
            }
        }

        // Recreate file groups with only top-level Zarr groups.
        let top_level_metas = top_level_zarr_meta_v3(&object_metas);
        let file_groups: Vec<FileGroup> = top_level_metas
            .into_iter()
            .map(|meta| {
                FileGroup::new(vec![
                    PartitionedFile::from(meta.clone()).with_range(0, meta.size as i64),
                ])
            })
            .collect();

        let source = ZarrSource::default();
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(ZarrSource::default())
    }
}

/// Get parent directory of a Path (S3-style).
/// Example: "a/b/c" -> Some("a/b")
///          "a"     -> None
fn path_parent(p: &object_store::path::Path) -> Option<object_store::path::Path> {
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
fn is_zarr_v3_metadata(meta: &object_store::ObjectMeta) -> bool {
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
    use datafusion::datasource::listing::ListingTableUrl;
    use futures::StreamExt;

    use super::*;

    #[test]
    fn test_name() {
        let test_path = object_store::path::Path::parse("bucket/key.zarr").unwrap();

        println!("Path: {:?}", test_path);
    }

    #[tokio::test]
    async fn test_listing_table() {
        let object_store =
            object_store::local::LocalFileSystem::new_with_prefix("./test_files").unwrap();

        let url = ListingTableUrl::try_new(
            "file:///".try_into().unwrap(),
            Some(glob::Pattern::new("gridded-example.zarr").unwrap()),
        )
        .unwrap();
        println!("URL: {:?}", url);

        let session = datafusion::execution::context::SessionContext::new();
        let state = session.state();
        let mut files = url
            .list_all_files(&state, &object_store, "zarr.json")
            .await
            .unwrap();

        while let Some(file) = files.next().await {
            println!("File: {:?}", file);
        }

        let url =
            ListingTableUrl::try_new("file:///gridded-example.zarr/".try_into().unwrap(), None)
                .unwrap();
        println!("URL: {:?}", url);

        let session = datafusion::execution::context::SessionContext::new();
        let state = session.state();
        let mut files = url
            .list_all_files(&state, &object_store, "zarr.json")
            .await
            .unwrap();

        while let Some(file) = files.next().await {
            println!("File: {:?}", file);
        }
    }

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
