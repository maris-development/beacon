use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_object_storage::DatasetsStore;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{
            FileGroup, FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
        },
    },
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::{
    options::AtlasOptions,
    reader::open_atlas_store,
    source::{AtlasFileInfo, AtlasSource},
};

pub mod options;
pub mod reader;
pub mod source;

/// The extension used for atlas store marker files.
const ATLAS_MARKER: &str = "atlas.json";

#[derive(Debug, Clone)]
pub struct AtlasFormatFactory {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: AtlasOptions,
}

impl AtlasFormatFactory {
    pub fn new(datasets_object_store: Arc<DatasetsStore>, options: AtlasOptions) -> Self {
        Self {
            datasets_object_store,
            options,
        }
    }
}

impl FileFormatFactory for AtlasFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AtlasFormat::new(
            self.datasets_object_store.clone(),
            self.options.clone(),
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(AtlasFormat::new(
            self.datasets_object_store.clone(),
            self.options.clone(),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for AtlasFormatFactory {
    fn get_ext(&self) -> String {
        ATLAS_MARKER.to_string()
    }
}

impl FileFormatFactoryExt for AtlasFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let markers = top_level_atlas_markers(objects);
        let ext = self.get_ext();
        let mut out = Vec::new();

        for marker in markers {
            // Read atlas.json directly with sync I/O. This is the cheap
            // shape we want for the catalog scan — we only need the
            // dataset names, not the full atlas open path (which is async
            // and would deadlock when discover_datasets is called from
            // inside a tokio runtime).
            let local_path = match self
                .datasets_object_store
                .translate_netcdf_url_path(&marker.location)
            {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        "Failed to translate atlas marker path {}: {e}",
                        marker.location
                    );
                    continue;
                }
            };

            if local_path.starts_with("http://") || local_path.starts_with("https://") {
                tracing::warn!(
                    "Skipping atlas marker at {} — remote object stores are not yet supported",
                    marker.location
                );
                continue;
            }

            let dataset_names = match read_atlas_dataset_names(&local_path) {
                Ok(names) => names,
                Err(e) => {
                    tracing::warn!("Failed to read atlas marker at {local_path}: {e}");
                    continue;
                }
            };

            let store_path = marker
                .location
                .as_ref()
                .strip_suffix(ATLAS_MARKER)
                .unwrap_or(marker.location.as_ref())
                .trim_end_matches('/');

            for dataset_name in dataset_names {
                let file_path = if store_path.is_empty() {
                    format!("{ATLAS_MARKER}/{dataset_name}")
                } else {
                    format!("{store_path}/{ATLAS_MARKER}/{dataset_name}")
                };
                out.push(DatasetMetadata::new(file_path, ext.clone()));
            }
        }

        Ok(out)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }
}

/// Parse an `atlas.json` file from disk and return the names of the
/// datasets it registers.
///
/// Atlas's on-disk layout has a stable shape:
/// `{ "version": <u32>, "codec": ..., "datasets": { "<name>": {...}, ... } }`.
/// We deliberately only read the `datasets` object's keys to stay
/// compatible across minor schema additions.
fn read_atlas_dataset_names(local_path: &str) -> anyhow::Result<Vec<String>> {
    let bytes = std::fs::read(local_path)
        .map_err(|e| anyhow::anyhow!("Failed to read atlas.json at {local_path}: {e}"))?;
    #[derive(serde::Deserialize)]
    struct Doc {
        #[serde(default)]
        datasets: std::collections::BTreeMap<String, serde_json::Value>,
    }
    let doc: Doc = serde_json::from_slice(&bytes)
        .map_err(|e| anyhow::anyhow!("Failed to parse atlas.json at {local_path}: {e}"))?;
    Ok(doc.datasets.into_keys().collect())
}

fn is_atlas_marker(obj: &ObjectMeta) -> bool {
    let loc = obj.location.to_string();
    loc == ATLAS_MARKER || loc.ends_with(&format!("/{ATLAS_MARKER}"))
}

/// Filter `objects` down to the unique top-level `atlas.json` files.
///
/// If two `atlas.json` files appear at different levels of a nested
/// directory tree we keep only the outermost one (the ancestor) — atlas
/// stores never contain other atlas stores, so any deeper marker is
/// spurious. This mirrors zarr's `top_level_zarr_meta_v3`.
fn top_level_atlas_markers(objects: &[ObjectMeta]) -> Vec<ObjectMeta> {
    let mut markers: Vec<&ObjectMeta> = objects.iter().filter(|o| is_atlas_marker(o)).collect();
    markers.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

    let mut kept: Vec<ObjectMeta> = Vec::new();
    'outer: for meta in &markers {
        let dir = match marker_parent(&meta.location) {
            Some(p) => p,
            None => String::new(),
        };
        for already in &kept {
            let already_dir = marker_parent(&already.location).unwrap_or_default();
            if !already_dir.is_empty() && dir.starts_with(&format!("{already_dir}/")) {
                continue 'outer;
            }
        }
        kept.push((*meta).clone());
    }
    kept
}

fn marker_parent(p: &object_store::path::Path) -> Option<String> {
    let s = p.as_ref();
    s.strip_suffix(ATLAS_MARKER)
        .map(|s| s.trim_end_matches('/').to_string())
}

#[derive(Debug, Clone)]
pub struct AtlasFormat {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: AtlasOptions,
}

impl AtlasFormat {
    pub fn new(datasets_object_store: Arc<DatasetsStore>, options: AtlasOptions) -> Self {
        Self {
            datasets_object_store,
            options,
        }
    }
}

#[async_trait::async_trait]
impl FileFormat for AtlasFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        ATLAS_MARKER.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(ATLAS_MARKER.to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let markers = top_level_atlas_markers(objects);
        if markers.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        let mut schemas = Vec::new();
        for marker in markers {
            let atlas =
                open_atlas_store(self.datasets_object_store.clone(), &marker.location).await?;
            let read_dimensions = self.options.read_dimensions.as_deref();
            for dataset_name in atlas.list_datasets() {
                let view = atlas.open_dataset(&dataset_name).await.map_err(|e| {
                    exec_datafusion_err!(
                        "Failed to open atlas dataset '{}' at {}: {}",
                        dataset_name,
                        marker.location,
                        e
                    )
                })?;
                let schema = crate::compat::atlas_view_arrow_schema(&view, read_dimensions)
                    .map_err(|e| {
                        exec_datafusion_err!(
                            "Failed to derive Arrow schema for atlas dataset '{}': {}",
                            dataset_name,
                            e
                        )
                    })?;
                schemas.push(Arc::new(schema));
            }
        }

        if schemas.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        let schema = super_type_schema(&schemas).map_err(|e| {
            exec_datafusion_err!(
                "Failed to compute super type schema for atlas datasets: {}",
                e
            )
        })?;
        Ok(Arc::new(schema))
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
        let source = AtlasSource::new(
            self.datasets_object_store.clone(),
            self.options.read_dimensions.clone(),
        );
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "Writing atlas datasets is not supported".to_string(),
        ))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(AtlasSource::new(
            self.datasets_object_store.clone(),
            self.options.read_dimensions.clone(),
        ))
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Shared helpers for integration tests across the datafusion module.

    use super::ATLAS_MARKER;
    use crate::reader::test_support::build_two_dataset_store;
    use beacon_object_storage::{DatasetsStore, get_datasets_object_store};
    use object_store::{ObjectMeta, path::Path as OsPath};
    use std::sync::Arc;

    /// Fixture directory under [`beacon_config::DATASETS_DIR_PATH`].
    pub const FIXTURE_DIR: &str = "beacon-arrow-atlas-tests/two_datasets.atlas";

    /// Ensure the `two_datasets.atlas` fixture exists under the global
    /// datasets directory. Idempotent and race-free across concurrent
    /// `#[tokio::test]` invocations.
    pub async fn ensure_fixture() -> std::path::PathBuf {
        static FIXTURE: tokio::sync::OnceCell<std::path::PathBuf> =
            tokio::sync::OnceCell::const_new();
        FIXTURE
            .get_or_init(|| async {
                let dst = beacon_config::DATASETS_DIR_PATH.join(FIXTURE_DIR);
                let marker = dst.join(ATLAS_MARKER);
                if !marker.exists() {
                    if dst.exists() {
                        std::fs::remove_dir_all(&dst).expect("cleanup partial fixture");
                    }
                    std::fs::create_dir_all(&dst).expect("create fixture dir");
                    build_two_dataset_store(&dst).await;
                }
                dst
            })
            .await
            .clone()
    }

    pub async fn test_store() -> Arc<DatasetsStore> {
        ensure_fixture().await;
        get_datasets_object_store().await
    }

    /// `ObjectMeta` for the fixture's `atlas.json` marker.
    pub fn fixture_marker_object_meta() -> ObjectMeta {
        ObjectMeta {
            location: OsPath::from(format!("{FIXTURE_DIR}/{ATLAS_MARKER}")),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{ensure_fixture, fixture_marker_object_meta, test_store};
    use super::*;
    use datafusion::prelude::SessionContext;
    use object_store::local::LocalFileSystem;

    fn dummy_object_store() -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new())
    }

    // ── is_atlas_marker / top_level_atlas_markers ──────────────────────

    #[test]
    fn is_atlas_marker_matches_only_marker_file() {
        let yes = ObjectMeta {
            location: object_store::path::Path::from("foo/atlas.json"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };
        let no = ObjectMeta {
            location: object_store::path::Path::from("foo/data.af"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };
        assert!(is_atlas_marker(&yes));
        assert!(!is_atlas_marker(&no));
    }

    #[test]
    fn top_level_markers_drops_nested_stores() {
        let objs = vec![
            ObjectMeta {
                location: object_store::path::Path::from("a/atlas.json"),
                last_modified: chrono::Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            },
            ObjectMeta {
                location: object_store::path::Path::from("a/b/atlas.json"),
                last_modified: chrono::Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            },
            ObjectMeta {
                location: object_store::path::Path::from("c/atlas.json"),
                last_modified: chrono::Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            },
        ];
        let kept = top_level_atlas_markers(&objs);
        let kept_paths: Vec<String> = kept.iter().map(|m| m.location.to_string()).collect();
        assert!(kept_paths.contains(&"a/atlas.json".to_string()));
        assert!(kept_paths.contains(&"c/atlas.json".to_string()));
        assert!(!kept_paths.iter().any(|p| p == "a/b/atlas.json"));
    }

    // ── AtlasFormatFactory ─────────────────────────────────────────────

    #[tokio::test]
    async fn factory_get_ext_returns_atlas_json() {
        let store = test_store().await;
        let factory = AtlasFormatFactory::new(store, AtlasOptions::default());
        assert_eq!(factory.get_ext(), "atlas.json");
        assert_eq!(factory.file_format_name(), "atlas.json");
    }

    #[tokio::test]
    async fn discover_datasets_emits_one_entry_per_atlas_dataset() {
        let store = test_store().await;
        let factory = AtlasFormatFactory::new(store, AtlasOptions::default());

        let objects = vec![fixture_marker_object_meta()];
        let datasets = factory.discover_datasets(&objects).expect("discover");

        assert_eq!(datasets.len(), 2, "expected 2 datasets, got {datasets:?}");
        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert!(paths.iter().any(|p| p.ends_with("/atlas.json/winter")));
        assert!(paths.iter().any(|p| p.ends_with("/atlas.json/summer")));
        for ds in &datasets {
            assert_eq!(ds.format, "atlas.json");
        }
    }

    #[tokio::test]
    async fn discover_datasets_ignores_non_marker_objects() {
        let store = test_store().await;
        let factory = AtlasFormatFactory::new(store, AtlasOptions::default());
        let objects = vec![ObjectMeta {
            location: object_store::path::Path::from("some/other.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }];
        let datasets = factory.discover_datasets(&objects).expect("discover");
        assert!(datasets.is_empty());
    }

    // ── AtlasFormat ────────────────────────────────────────────────────

    #[tokio::test]
    async fn infer_schema_unions_columns_across_datasets() {
        let store = test_store().await;
        let format = AtlasFormat::new(store, AtlasOptions::default());
        let ctx = SessionContext::new();

        let schema = format
            .infer_schema(
                &ctx.state(),
                &dummy_object_store(),
                &[fixture_marker_object_meta()],
            )
            .await
            .expect("infer");

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // Winter has cycle, season, temperature, year; summer has season, temperature.
        // The super-typed schema should include all of them.
        assert!(
            names.contains(&"temperature"),
            "missing temperature in {names:?}"
        );
        assert!(names.contains(&"cycle"), "missing cycle in {names:?}");
        assert!(names.contains(&"season"), "missing season in {names:?}");
        assert!(names.contains(&"year"), "missing year in {names:?}");
    }

    #[tokio::test]
    async fn infer_schema_empty_objects_returns_empty_schema() {
        let store = test_store().await;
        let format = AtlasFormat::new(store, AtlasOptions::default());
        let ctx = SessionContext::new();

        let schema = format
            .infer_schema(&ctx.state(), &dummy_object_store(), &[])
            .await
            .expect("infer");
        assert!(schema.fields().is_empty());
    }

    #[tokio::test]
    async fn file_source_returns_atlas_type() {
        let store = test_store().await;
        ensure_fixture().await;
        let format = AtlasFormat::new(store, AtlasOptions::default());
        assert_eq!(format.file_source().file_type(), "atlas");
    }

    #[tokio::test]
    async fn writer_path_not_implemented() {
        use datafusion::physical_plan::empty::EmptyExec;
        let store = test_store().await;
        let format = AtlasFormat::new(store, AtlasOptions::default());
        let ctx = SessionContext::new();
        let empty_schema: arrow::datatypes::SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(empty_schema.clone()));

        let conf = FileSinkConfig {
            original_url: String::new(),
            object_store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(
            ),
            table_paths: vec![],
            file_group: FileGroup::new(vec![]),
            output_schema: empty_schema,
            table_partition_cols: vec![],
            insert_op: datafusion::logical_expr::dml::InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "atlas.json".to_string(),
        };
        let err = format
            .create_writer_physical_plan(input, &ctx.state(), conf, None)
            .await
            .expect_err("writing should be unsupported");
        let msg = format!("{err}");
        assert!(msg.to_lowercase().contains("not"), "{msg}");
    }
}
