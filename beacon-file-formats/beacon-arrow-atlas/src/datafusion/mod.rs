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
    source::{AtlasSource, AtlasStreamDatasets},
};

pub mod cache;
pub mod options;
pub mod pushdown;
pub mod reader;
pub mod source;

pub use cache::AtlasReaderCache;

/// Canonical marker filename used as the format identifier with
/// DataFusion. Kept stable across atlas versions so the registered
/// `FileFormat` lookup key doesn't churn even though the on-disk
/// filename may be any of [`ATLAS_MARKER_NAMES`].
const ATLAS_MARKER: &str = "atlas.json";

/// All marker filenames atlas 0.8.0 may emit at the root of a store.
/// Order matches atlas's own `META_VARIANTS` — uncompressed first within
/// each format, JSON before MsgPack overall — so when multiple are
/// present the priority tie-break mirrors atlas.
const ATLAS_MARKER_NAMES: [&str; 6] = [
    "atlas.json",
    "atlas.json.zst",
    "atlas.json.lz4",
    "atlas.msgpack",
    "atlas.msgpack.zst",
    "atlas.msgpack.lz4",
];

#[derive(Copy, Clone)]
enum MetaFormat {
    Json,
    MsgPack,
}

#[derive(Copy, Clone)]
enum Codec {
    None,
    Zstd,
    Lz4,
}

/// Map a marker filename to its `(format, compression)` encoding.
fn atlas_marker_encoding(filename: &str) -> Option<(MetaFormat, Codec)> {
    match filename {
        "atlas.json" => Some((MetaFormat::Json, Codec::None)),
        "atlas.json.zst" => Some((MetaFormat::Json, Codec::Zstd)),
        "atlas.json.lz4" => Some((MetaFormat::Json, Codec::Lz4)),
        "atlas.msgpack" => Some((MetaFormat::MsgPack, Codec::None)),
        "atlas.msgpack.zst" => Some((MetaFormat::MsgPack, Codec::Zstd)),
        "atlas.msgpack.lz4" => Some((MetaFormat::MsgPack, Codec::Lz4)),
        _ => None,
    }
}

/// If `p` ends in one of the known atlas marker filenames, return that
/// filename. Used both to recognize markers and to recover the actual
/// on-disk filename for path manipulation.
fn atlas_marker_filename(p: &object_store::path::Path) -> Option<&'static str> {
    let s = p.as_ref();
    ATLAS_MARKER_NAMES
        .iter()
        .copied()
        .find(|name| s == *name || s.ends_with(&format!("/{name}")))
}

/// Runtime configuration for the atlas format.
///
/// Plain data with sensible defaults; the caller populates it (no environment
/// parsing here). The reader-cache capacity is a shared runtime resource, while
/// `use_reader_cache` is a default that a table can override via
/// `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct AtlasConfig {
    /// Whether reads consult the shared reader cache by default.
    pub use_reader_cache: bool,
    /// Capacity (number of opened atlas stores) of the shared reader cache.
    pub reader_cache_size: u64,
}

impl Default for AtlasConfig {
    fn default() -> Self {
        Self {
            use_reader_cache: true,
            reader_cache_size: 32,
        }
    }
}

/// Parse a boolean value supplied through a `CREATE EXTERNAL TABLE` option.
fn parse_bool_option(key: &str, value: &str) -> datafusion::error::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(exec_datafusion_err!(
            "invalid boolean for atlas option '{key}': '{other}'"
        )),
    }
}

#[derive(Debug, Clone)]
pub struct AtlasFormatFactory {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: AtlasOptions,
    pub config: AtlasConfig,
    /// Shared reader cache for this runtime, sized from `config`.
    cache: AtlasReaderCache,
}

impl AtlasFormatFactory {
    pub fn new(
        datasets_object_store: Arc<DatasetsStore>,
        options: AtlasOptions,
        config: AtlasConfig,
    ) -> Self {
        let cache = AtlasReaderCache::new(config.reader_cache_size);
        Self {
            datasets_object_store,
            options,
            config,
            cache,
        }
    }

    /// Build an [`AtlasFormat`] with the given per-table effective settings,
    /// wiring in the shared reader cache when caching is enabled.
    fn build_format(&self, options: AtlasOptions, use_reader_cache: bool) -> AtlasFormat {
        let cache = use_reader_cache.then(|| self.cache.clone());
        AtlasFormat::new(self.datasets_object_store.clone(), options).with_cache(cache)
    }
}

impl FileFormatFactory for AtlasFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        // Per-table overrides from `CREATE EXTERNAL TABLE ... OPTIONS (...)`,
        // defaulting to the runtime config.
        let mut options = self.options.clone();
        let mut use_reader_cache = self.config.use_reader_cache;

        if let Some(value) = format_options.get("read_dimensions") {
            options.read_dimensions = Some(
                value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            );
        }
        if let Some(value) = format_options.get("use_reader_cache") {
            use_reader_cache = parse_bool_option("use_reader_cache", value)?;
        }

        Ok(Arc::new(self.build_format(options, use_reader_cache)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.build_format(self.options.clone(), self.config.use_reader_cache))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for AtlasFormatFactory {
    fn get_ext(&self) -> String {
        "atlas".to_string()
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

            let marker_name = atlas_marker_filename(&marker.location)
                .expect("top_level_atlas_markers only yields recognized markers");
            let store_path = marker
                .location
                .as_ref()
                .strip_suffix(marker_name)
                .unwrap_or(marker.location.as_ref())
                .trim_end_matches('/');

            for dataset_name in dataset_names {
                let file_path = if store_path.is_empty() {
                    format!("{marker_name}/{dataset_name}")
                } else {
                    format!("{store_path}/{marker_name}/{dataset_name}")
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

/// Parse an atlas store metadata file from disk and return the names of
/// the datasets it registers.
///
/// Format and compression are derived from the filename — atlas 0.8.0
/// encodes both in the on-disk name (`atlas.{json,msgpack}{,.zst,.lz4}`).
/// We deliberately only read the `datasets` object's keys to stay
/// compatible across minor schema additions.
fn read_atlas_dataset_names(local_path: &str) -> anyhow::Result<Vec<String>> {
    let bytes = std::fs::read(local_path)
        .map_err(|e| anyhow::anyhow!("Failed to read atlas marker at {local_path}: {e}"))?;

    let filename = std::path::Path::new(local_path)
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("Atlas marker path has no filename: {local_path}"))?;

    let (format, codec) = atlas_marker_encoding(filename)
        .ok_or_else(|| anyhow::anyhow!("Unknown atlas marker filename: {filename}"))?;

    let raw = match codec {
        Codec::None => bytes,
        Codec::Zstd => zstd::stream::decode_all(bytes.as_slice())
            .map_err(|e| anyhow::anyhow!("Failed to zstd-decode {local_path}: {e}"))?,
        Codec::Lz4 => lz4_flex::decompress_size_prepended(&bytes)
            .map_err(|e| anyhow::anyhow!("Failed to lz4-decode {local_path}: {e}"))?,
    };

    // `IgnoredAny` deserializes from any value shape, so this struct
    // works against both JSON and MsgPack without per-format variants.
    #[derive(serde::Deserialize)]
    struct Doc {
        #[serde(default)]
        datasets: std::collections::BTreeMap<String, serde::de::IgnoredAny>,
    }

    let doc: Doc = match format {
        MetaFormat::Json => serde_json::from_slice(&raw)
            .map_err(|e| anyhow::anyhow!("Failed to JSON-parse {local_path}: {e}"))?,
        MetaFormat::MsgPack => rmp_serde::from_slice(&raw)
            .map_err(|e| anyhow::anyhow!("Failed to MsgPack-parse {local_path}: {e}"))?,
    };
    Ok(doc.datasets.into_keys().collect())
}

fn is_atlas_marker(obj: &ObjectMeta) -> bool {
    atlas_marker_filename(&obj.location).is_some()
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
    let name = atlas_marker_filename(p)?;
    Some(s.strip_suffix(name)?.trim_end_matches('/').to_string())
}

#[derive(Debug, Clone)]
pub struct AtlasFormat {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: AtlasOptions,
    /// Reader cache to consult, or `None` to bypass caching for this format.
    cache: Option<AtlasReaderCache>,
}

impl AtlasFormat {
    pub fn new(datasets_object_store: Arc<DatasetsStore>, options: AtlasOptions) -> Self {
        Self {
            datasets_object_store,
            options,
            cache: None,
        }
    }

    /// Wire in a reader cache (`Some`) or disable caching (`None`).
    pub fn with_cache(mut self, cache: Option<AtlasReaderCache>) -> Self {
        self.cache = cache;
        self
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
            let atlas = cache::get_or_open_atlas(
                self.cache.as_ref(),
                self.datasets_object_store.clone(),
                &marker,
            )
            .await?;
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
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            conf.file_schema().clone(),
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = AtlasSource::new(
            self.datasets_object_store.clone(),
            self.options.read_dimensions.clone(),
            table_schema,
        )
        .with_cache(self.cache.clone())
        .with_projection(projection);
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

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(
            AtlasSource::new(
                self.datasets_object_store.clone(),
                self.options.read_dimensions.clone(),
                table_schema,
            )
            .with_cache(self.cache.clone()),
        )
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Shared helpers for integration tests across the datafusion module.

    use super::ATLAS_MARKER;
    use crate::reader::test_support::build_two_dataset_store;
    use beacon_object_storage::DatasetsStore;
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
        beacon_object_storage::get_datasets_object_store().await
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
    use object_store::path::Path as OsPath;

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
    async fn factory_get_ext_returns_atlas() {
        let store = test_store().await;
        let factory = AtlasFormatFactory::new(store, AtlasOptions::default(), AtlasConfig::default());
        // `get_ext` is the format identity used to register and resolve
        // external tables (`STORED AS ATLAS`), not the marker filename
        // (`atlas.json`).
        assert_eq!(factory.get_ext(), "atlas");
        assert_eq!(factory.file_format_name(), "atlas");
    }

    #[tokio::test]
    async fn discover_datasets_emits_one_entry_per_atlas_dataset() {
        let store = test_store().await;
        let factory = AtlasFormatFactory::new(store, AtlasOptions::default(), AtlasConfig::default());

        let objects = vec![fixture_marker_object_meta()];
        let datasets = factory.discover_datasets(&objects).expect("discover");

        assert_eq!(datasets.len(), 2, "expected 2 datasets, got {datasets:?}");
        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert!(paths.iter().any(|p| p.ends_with("/atlas.json/winter")));
        assert!(paths.iter().any(|p| p.ends_with("/atlas.json/summer")));
        for ds in &datasets {
            // `format` is the factory identity (`get_ext`), used to resolve the
            // reader — the marker filename `atlas.json` only appears in the path.
            assert_eq!(ds.format, "atlas");
        }
    }

    #[tokio::test]
    async fn discover_datasets_ignores_non_marker_objects() {
        let store = test_store().await;
        let factory = AtlasFormatFactory::new(store, AtlasOptions::default(), AtlasConfig::default());
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
        assert_eq!(
            format
                .file_source(
                    datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
                        arrow::datatypes::Schema::empty()
                    ))
                )
                .file_type(),
            "atlas"
        );
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
            file_output_mode:
                datafusion::datasource::physical_plan::FileOutputMode::SingleFile,
        };
        let err = format
            .create_writer_physical_plan(input, &ctx.state(), conf, None)
            .await
            .expect_err("writing should be unsupported");
        let msg = format!("{err}");
        assert!(msg.to_lowercase().contains("not"), "{msg}");
    }

    // ── Atlas 0.8.0 marker variants ────────────────────────────────────

    fn marker_obj(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: object_store::path::Path::from(path),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn is_atlas_marker_matches_all_six_variants() {
        for name in &ATLAS_MARKER_NAMES {
            assert!(
                is_atlas_marker(&marker_obj(name)),
                "bare {name} should be recognized",
            );
            assert!(
                is_atlas_marker(&marker_obj(&format!("store/{name}"))),
                "store/{name} should be recognized",
            );
            assert!(
                is_atlas_marker(&marker_obj(&format!("a/b/c/{name}"))),
                "a/b/c/{name} should be recognized",
            );
        }

        // Lookalikes must not match.
        for negative in [
            "foo/data.af",
            "store/atlas.json.tmp",
            "store/atlas.jsona",
            "store/atlas.msgpack.bak",
            "atlas.json/inner",
        ] {
            assert!(
                !is_atlas_marker(&marker_obj(negative)),
                "{negative} should NOT be recognized",
            );
        }
    }

    #[test]
    fn marker_parent_strips_each_variant() {
        for name in &ATLAS_MARKER_NAMES {
            assert_eq!(
                marker_parent(&object_store::path::Path::from(format!("store/{name}"))),
                Some("store".to_string()),
                "marker_parent should strip {name}",
            );
            assert_eq!(
                marker_parent(&object_store::path::Path::from(*name)),
                Some(String::new()),
                "marker_parent of bare {name} is empty",
            );
        }
        assert_eq!(
            marker_parent(&object_store::path::Path::from("foo.txt")),
            None,
            "marker_parent of a non-marker is None",
        );
    }

    // ── read_atlas_dataset_names: one test per variant ─────────────────

    /// Atlas's on-disk meta shape, mirrored just well enough to encode
    /// fixtures. We don't import atlas's own `StoreMeta` because it's
    /// `pub(crate)`.
    #[derive(serde::Serialize)]
    struct FixtureMeta {
        version: u32,
        codec: &'static str,
        datasets: std::collections::BTreeMap<String, FixtureDataset>,
    }

    #[derive(serde::Serialize, Default)]
    struct FixtureDataset {
        arrays: std::collections::BTreeMap<String, serde_json::Value>,
        attributes: std::collections::BTreeMap<String, serde_json::Value>,
    }

    fn sample_meta() -> FixtureMeta {
        let mut datasets = std::collections::BTreeMap::new();
        datasets.insert("a".to_string(), FixtureDataset::default());
        datasets.insert("b".to_string(), FixtureDataset::default());
        FixtureMeta {
            version: 1,
            codec: "Zstd",
            datasets,
        }
    }

    fn encode_meta(meta: &FixtureMeta, format: MetaFormat) -> Vec<u8> {
        match format {
            MetaFormat::Json => serde_json::to_vec(meta).expect("encode json"),
            MetaFormat::MsgPack => rmp_serde::to_vec_named(meta).expect("encode msgpack"),
        }
    }

    fn compress_meta(bytes: Vec<u8>, codec: Codec) -> Vec<u8> {
        match codec {
            Codec::None => bytes,
            Codec::Zstd => zstd::stream::encode_all(bytes.as_slice(), 0).expect("zstd"),
            Codec::Lz4 => lz4_flex::compress_prepend_size(&bytes),
        }
    }

    fn write_fixture_marker(filename: &str, format: MetaFormat, codec: Codec) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let bytes = compress_meta(encode_meta(&sample_meta(), format), codec);
        std::fs::write(dir.path().join(filename), bytes).expect("write fixture");
        dir
    }

    fn assert_dataset_names(dir: &std::path::Path, filename: &str) {
        let path = dir.join(filename);
        let names =
            read_atlas_dataset_names(path.to_str().expect("utf-8 path")).expect("read names");
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn read_atlas_dataset_names_json_uncompressed() {
        let dir = write_fixture_marker("atlas.json", MetaFormat::Json, Codec::None);
        assert_dataset_names(dir.path(), "atlas.json");
    }

    #[test]
    fn read_atlas_dataset_names_json_zstd() {
        let dir = write_fixture_marker("atlas.json.zst", MetaFormat::Json, Codec::Zstd);
        assert_dataset_names(dir.path(), "atlas.json.zst");
    }

    #[test]
    fn read_atlas_dataset_names_json_lz4() {
        let dir = write_fixture_marker("atlas.json.lz4", MetaFormat::Json, Codec::Lz4);
        assert_dataset_names(dir.path(), "atlas.json.lz4");
    }

    #[test]
    fn read_atlas_dataset_names_msgpack_uncompressed() {
        let dir = write_fixture_marker("atlas.msgpack", MetaFormat::MsgPack, Codec::None);
        assert_dataset_names(dir.path(), "atlas.msgpack");
    }

    #[test]
    fn read_atlas_dataset_names_msgpack_zstd() {
        let dir = write_fixture_marker("atlas.msgpack.zst", MetaFormat::MsgPack, Codec::Zstd);
        assert_dataset_names(dir.path(), "atlas.msgpack.zst");
    }

    #[test]
    fn read_atlas_dataset_names_msgpack_lz4() {
        let dir = write_fixture_marker("atlas.msgpack.lz4", MetaFormat::MsgPack, Codec::Lz4);
        assert_dataset_names(dir.path(), "atlas.msgpack.lz4");
    }

    #[test]
    fn read_atlas_dataset_names_rejects_unknown_filename() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("not_an_atlas_marker");
        std::fs::write(&path, b"{}").expect("write");
        let err =
            read_atlas_dataset_names(path.to_str().expect("utf-8")).expect_err("should reject");
        assert!(
            format!("{err}").contains("Unknown atlas marker filename"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn top_level_markers_dedupes_across_different_formats() {
        // Outer store uses atlas.json; nested store uses atlas.msgpack.zst.
        // The nested one must be dropped even though the filenames differ.
        let objs = vec![
            marker_obj("a/atlas.json"),
            marker_obj("a/b/atlas.msgpack.zst"),
            marker_obj("c/atlas.msgpack"),
        ];
        let kept: Vec<String> = top_level_atlas_markers(&objs)
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        assert!(kept.contains(&"a/atlas.json".to_string()));
        assert!(kept.contains(&"c/atlas.msgpack".to_string()));
        assert!(!kept.iter().any(|p| p == "a/b/atlas.msgpack.zst"));
    }

    // ── End-to-end: discover_datasets against a non-default marker ─────

    const MSGPACK_ZST_FIXTURE_DIR: &str = "beacon-arrow-atlas-tests/two_datasets.msgpack.zst.atlas";

    /// Build the msgpack.zst fixture once per test run.
    async fn ensure_msgpack_zst_fixture() -> std::path::PathBuf {
        use crate::reader::test_support::build_two_dataset_store_with_config;
        use atlas::{Codec as AtlasCodec, MetaFormat as AtlasMetaFormat, StoreConfig};

        static FIXTURE: tokio::sync::OnceCell<std::path::PathBuf> =
            tokio::sync::OnceCell::const_new();
        FIXTURE
            .get_or_init(|| async {
                let dst = beacon_config::DATASETS_DIR_PATH.join(MSGPACK_ZST_FIXTURE_DIR);
                let marker = dst.join("atlas.msgpack.zst");
                if !marker.exists() {
                    if dst.exists() {
                        std::fs::remove_dir_all(&dst).expect("cleanup partial fixture");
                    }
                    std::fs::create_dir_all(&dst).expect("create fixture dir");
                    let config = StoreConfig {
                        meta_format: AtlasMetaFormat::MsgPack,
                        meta_compression: AtlasCodec::Zstd,
                        ..Default::default()
                    };
                    build_two_dataset_store_with_config(&dst, config).await;
                }
                dst
            })
            .await
            .clone()
    }

    #[tokio::test]
    async fn discover_datasets_finds_msgpack_zst_store() {
        ensure_msgpack_zst_fixture().await;
        let store = test_store().await;
        let factory = AtlasFormatFactory::new(store, AtlasOptions::default(), AtlasConfig::default());

        let marker = ObjectMeta {
            location: OsPath::from(format!("{MSGPACK_ZST_FIXTURE_DIR}/atlas.msgpack.zst")),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };
        let datasets = factory.discover_datasets(&[marker]).expect("discover");

        assert_eq!(datasets.len(), 2, "expected 2 datasets, got {datasets:?}");
        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        // Synthetic paths must preserve the actual on-disk marker name.
        assert!(
            paths
                .iter()
                .any(|p| p.ends_with("/atlas.msgpack.zst/winter")),
            "missing winter in {paths:?}",
        );
        assert!(
            paths
                .iter()
                .any(|p| p.ends_with("/atlas.msgpack.zst/summer")),
            "missing summer in {paths:?}",
        );
    }

    // ── End-to-end via SessionContext (projection + predicate pushdown) ──

    /// Register the `two_datasets.atlas` fixture as a DataFusion table backed by
    /// [`AtlasFormat`] over the `datasets://` object store.
    async fn register_example(ctx: &SessionContext, store: Arc<DatasetsStore>) {
        use beacon_common::super_table::SuperListingTable;
        use datafusion::datasource::file_format::FileFormat;
        use datafusion::datasource::listing::ListingTableUrl;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        ctx.register_object_store(store_url.as_ref(), store.clone());

        let format: Arc<dyn FileFormat> =
            Arc::new(AtlasFormat::new(store, AtlasOptions::default()));
        let url = ListingTableUrl::parse(
            "datasets:///beacon-arrow-atlas-tests/two_datasets.atlas/atlas.json",
        )
        .unwrap();
        let table = SuperListingTable::new(&ctx.state(), format, vec![url])
            .await
            .unwrap();
        ctx.register_table("atlas_t", Arc::new(table)).unwrap();
    }

    #[tokio::test]
    async fn projection_pushdown_through_datafusion() {
        ensure_fixture().await;
        let store = test_store().await;
        let ctx = SessionContext::new();
        register_example(&ctx, store).await;

        let df = ctx.sql("SELECT temperature FROM atlas_t").await.unwrap();
        let names: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["temperature".to_string()]);

        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 7, "winter (4) + summer (3) temperature rows");
    }

    #[tokio::test]
    async fn predicate_pushdown_prunes_through_datafusion() {
        ensure_fixture().await;
        let store = test_store().await;
        let ctx = SessionContext::new();
        register_example(&ctx, store).await;

        // The maximum temperature in the fixture is 22.
        let rows: usize = ctx
            .sql("SELECT temperature FROM atlas_t WHERE temperature > 1000000")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 0, "no temperature exceeds 1e6");
    }

    #[tokio::test]
    async fn predicate_pushdown_selects_subset_through_datafusion() {
        use arrow::array::Float32Array;

        ensure_fixture().await;
        let store = test_store().await;
        let ctx = SessionContext::new();
        register_example(&ctx, store).await;

        // winter = [1,2,3,4], summer = [20,21,22]; `> 10` keeps only summer.
        let batches = ctx
            .sql("SELECT temperature FROM atlas_t WHERE temperature > 10")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut temps: Vec<f32> = vec![];
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            for i in 0..col.len() {
                assert!(col.value(i) > 10.0, "every returned temperature must satisfy the predicate");
                temps.push(col.value(i));
            }
        }
        temps.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(temps, vec![20.0f32, 21.0, 22.0], "only summer's temperatures remain");
    }
}
