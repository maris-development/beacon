//! DataFusion integration for atlas stores.
//!
//! Mirrors the zarr crate: an [`AtlasFormatFactory`] discovers atlas metadata
//! markers, [`AtlasFormat`] infers the (super-typed) Arrow schema across a
//! store's datasets and plans the scan, and [`AtlasSource`] opens each store
//! natively over the query's object store and streams every dataset through the
//! shared `beacon-nd-array` engine.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
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
    cache::AtlasReaderCache, options::AtlasOptions, source::AtlasDatasetSlice, source::AtlasSource,
};
use crate::util::{ATLAS_MARKER, top_level_atlas_markers};

pub mod cache;
pub mod metrics;
pub mod options;
pub mod pruning;
pub mod source;
pub mod table_function;

pub use cache::{AtlasReaderCache as ReaderCache, get_or_open_atlas};
pub use options::AtlasOptions as Options;
pub use source::AtlasSource as Source;
pub use table_function::ReadAtlasFunc;

/// Runtime configuration for the atlas format.
///
/// Plain data with sensible defaults; the caller populates it. The reader-cache
/// capacity is a shared runtime resource, while `use_reader_cache` is a default
/// a table can override via `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct AtlasConfig {
    /// Whether reads consult the shared reader cache by default.
    pub use_reader_cache: bool,
    /// Capacity (number of opened atlas stores) of the shared reader cache.
    pub reader_cache_size: u64,
    /// Whether a predicate scan prunes datasets that can't match, using the
    /// collection's statistics, before reading them. A pure optimization — off
    /// only trades throughput for skipping the pruning-index build. Overridable
    /// per table via `CREATE EXTERNAL TABLE ... OPTIONS (use_pruning '…')`.
    pub use_pruning: bool,
}

impl Default for AtlasConfig {
    fn default() -> Self {
        Self {
            use_reader_cache: true,
            reader_cache_size: 32,
            use_pruning: true,
        }
    }
}

/// Split a store's dataset names into up to `partitions` round-robin buckets.
///
/// Round-robin (`i % parts`) rather than contiguous chunks keeps the buckets
/// balanced when nearby ordinals have similar sizes (a common ingest pattern).
/// Empty buckets are dropped, so a store with fewer datasets than `partitions`
/// simply yields fewer scan partitions.
fn partition_dataset_names(names: Vec<String>, partitions: usize) -> Vec<Vec<String>> {
    if names.is_empty() {
        return Vec::new();
    }
    let parts = partitions.max(1).min(names.len());
    let mut buckets: Vec<Vec<String>> = vec![Vec::new(); parts];
    for (i, name) in names.into_iter().enumerate() {
        buckets[i % parts].push(name);
    }
    buckets
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

// ─── Factory ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AtlasFormatFactory {
    pub options: AtlasOptions,
    pub config: AtlasConfig,
    /// Shared reader cache for this runtime, sized from `config`.
    cache: AtlasReaderCache,
}

impl AtlasFormatFactory {
    pub fn new(options: AtlasOptions, config: AtlasConfig) -> Self {
        let cache = AtlasReaderCache::new(config.reader_cache_size);
        Self {
            options,
            config,
            cache,
        }
    }

    /// Build an [`AtlasFormat`] with the given per-table effective settings,
    /// wiring in the shared reader cache when caching is enabled.
    fn build_format(
        &self,
        options: AtlasOptions,
        use_reader_cache: bool,
        use_pruning: bool,
    ) -> AtlasFormat {
        let cache = use_reader_cache.then(|| self.cache.clone());
        AtlasFormat::new(options)
            .with_cache(cache)
            .with_pruning(use_pruning)
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
        let mut use_pruning = self.config.use_pruning;

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
        if let Some(value) = format_options.get("use_pruning") {
            use_pruning = parse_bool_option("use_pruning", value)?;
        }

        Ok(Arc::new(self.build_format(options, use_reader_cache, use_pruning)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.build_format(
            self.options.clone(),
            self.config.use_reader_cache,
            self.config.use_pruning,
        ))
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
        // One dataset entry per top-level store marker (mirroring zarr, which
        // emits one entry per top-level `zarr.json`). The store's individual
        // datasets are enumerated at scan time by the opener.
        let ext = self.get_ext();
        Ok(top_level_atlas_markers(objects)
            .into_iter()
            .map(|marker| DatasetMetadata::new(marker.location.to_string(), ext.clone()))
            .collect())
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }
}

// ─── Format ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct AtlasFormat {
    pub options: AtlasOptions,
    /// Reader cache to consult, or `None` to bypass caching for this format.
    cache: Option<AtlasReaderCache>,
    /// Whether a predicate scan prunes non-matching datasets before reading.
    use_pruning: bool,
}

impl AtlasFormat {
    pub fn new(options: AtlasOptions) -> Self {
        Self {
            options,
            cache: None,
            use_pruning: false,
        }
    }

    /// Wire in a reader cache (`Some`) or disable caching (`None`).
    pub fn with_cache(mut self, cache: Option<AtlasReaderCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Enable or disable dataset pruning for predicate scans.
    pub fn with_pruning(mut self, use_pruning: bool) -> Self {
        self.use_pruning = use_pruning;
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
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let infer_start = std::time::Instant::now();
        let markers = top_level_atlas_markers(objects);
        if markers.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        // Scale note: the schema is derived from each store's collection-wide
        // `merged_schema()` — a pre-widened, in-memory summary that costs O(1)
        // disk reads (just the metadata already loaded on open), independent of
        // the dataset count. Never iterate `list_datasets()` here: at 1M+
        // datasets that turns planning into a full-collection scan.
        let read_dimensions = self.options.read_dimensions.clone();
        let mut schemas = Vec::new();
        for marker in &markers {
            let atlas = get_or_open_atlas(self.cache.as_ref(), store.clone(), marker).await?;
            let merged = atlas.merged_schema();
            let schema =
                crate::compat::atlas_merged_schema_to_arrow(&merged, read_dimensions.as_deref());
            schemas.push(Arc::new(schema));
        }

        // Union across stores (a single store is the common case — one schema).
        let schema = super_type_schema(&schemas).map_err(|e| {
            exec_datafusion_err!("Failed to compute super type schema for atlas datasets: {}", e)
        })?;
        tracing::debug!(
            elapsed_ms = infer_start.elapsed().as_millis() as u64,
            stores = markers.len(),
            fields = schema.fields().len(),
            "atlas infer_schema",
        );
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
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Spread each store's datasets across up to `target_partitions` file
        // groups so DataFusion scans them on separate cores — the single
        // partition a plain listing produces would otherwise pin a 1M-dataset
        // store to one thread. Opening the store here is one metadata read,
        // cached and reused by the openers; `list_datasets` is in-memory.
        let plan_start = std::time::Instant::now();
        let object_store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;
        let target_partitions = state.config().target_partitions().max(1);

        let mut markers: Vec<ObjectMeta> = Vec::new();
        for group in &conf.file_groups {
            for file in group.files() {
                markers.push(file.object_meta.clone());
            }
        }
        let markers = top_level_atlas_markers(&markers);

        let mut total_datasets = 0usize;
        let mut file_groups: Vec<FileGroup> = Vec::new();
        for marker in &markers {
            let atlas = get_or_open_atlas(self.cache.as_ref(), object_store.clone(), marker).await?;
            let names = atlas.list_datasets();
            total_datasets += names.len();
            for slice in partition_dataset_names(names, target_partitions) {
                // `From<ObjectMeta>` keeps the marker's freshness (last_modified
                // + size) so the opener's cache key matches this plan-time open.
                let mut file = PartitionedFile::from(marker.clone());
                file.extensions = Some(Arc::new(AtlasDatasetSlice { names: slice }));
                file_groups.push(FileGroup::new(vec![file]));
            }
        }
        tracing::debug!(
            elapsed_ms = plan_start.elapsed().as_millis() as u64,
            stores = markers.len(),
            datasets = total_datasets,
            partitions = file_groups.len(),
            "atlas create_physical_plan partitioning",
        );

        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            conf.file_schema().clone(),
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = AtlasSource::new(self.options.read_dimensions.clone(), table_schema)
            .with_cache(self.cache.clone())
            .with_pruning(self.use_pruning)
            .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
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
            AtlasSource::new(self.options.read_dimensions.clone(), table_schema)
                .with_cache(self.cache.clone())
                .with_pruning(self.use_pruning),
        )
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Shared helpers for integration tests across the datafusion module.

    use crate::reader::test_support::build_two_dataset_store;
    use crate::util::ATLAS_MARKER;
    use object_store::{ObjectMeta, ObjectStore, local::LocalFileSystem, path::Path as OsPath};
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Fixture directory (under the crate-local test root) for the store.
    pub const FIXTURE_DIR: &str = "two_datasets.atlas";

    /// The local root fixtures are built under — a crate-local dir in the OS
    /// temp dir. These tests need *a* root, not the application's.
    fn datasets_root() -> PathBuf {
        std::env::temp_dir().join("beacon-arrow-atlas-datasets")
    }

    /// Ensure the fixture store exists under the test root. Idempotent and
    /// race-free across concurrent `#[tokio::test]` invocations.
    async fn ensure_fixture() -> PathBuf {
        static FIXTURE: tokio::sync::OnceCell<PathBuf> = tokio::sync::OnceCell::const_new();
        FIXTURE
            .get_or_init(|| async {
                let dst = datasets_root().join(FIXTURE_DIR);
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

    /// An object store rooted at the test datasets root, plus the ensured
    /// fixture. The marker location is `{FIXTURE_DIR}/atlas.json` relative to
    /// this store.
    pub async fn test_store() -> Arc<dyn ObjectStore> {
        ensure_fixture().await;
        Arc::new(LocalFileSystem::new_with_prefix(datasets_root()).unwrap())
    }

    /// `ObjectMeta` for the fixture's marker, relative to [`test_store`].
    pub fn fixture_marker_object_meta() -> ObjectMeta {
        ObjectMeta {
            location: OsPath::from(format!("{FIXTURE_DIR}/{ATLAS_MARKER}")),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{fixture_marker_object_meta, test_store};
    use super::*;
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as OsPath;

    // ── discovery ───────────────────────────────────────────────────────

    #[test]
    fn factory_get_ext_is_atlas() {
        let factory = AtlasFormatFactory::new(Default::default(), Default::default());
        assert_eq!(factory.get_ext(), "atlas");
        assert_eq!(factory.file_format_name(), "atlas");
    }

    #[test]
    fn discover_datasets_emits_one_entry_per_store() {
        let factory = AtlasFormatFactory::new(Default::default(), Default::default());
        let objects = vec![
            ObjectMeta {
                location: OsPath::from("store_a/atlas.json"),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
                version: None,
            },
            // A nested store marker must NOT become its own dataset.
            ObjectMeta {
                location: OsPath::from("store_a/inner/atlas.json"),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
                version: None,
            },
            ObjectMeta {
                location: OsPath::from("store_b/atlas.msgpack"),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
                version: None,
            },
        ];
        let mut datasets = factory.discover_datasets(&objects).expect("discover");
        datasets.sort_by(|a, b| a.file_path.cmp(&b.file_path));
        assert_eq!(datasets.len(), 2, "{datasets:?}");
        assert_eq!(datasets[0].file_path, "store_a/atlas.json");
        assert_eq!(datasets[1].file_path, "store_b/atlas.msgpack");
        assert!(datasets.iter().all(|d| d.format == "atlas"));
    }

    #[test]
    fn discover_datasets_ignores_non_markers() {
        let factory = AtlasFormatFactory::new(Default::default(), Default::default());
        let objects = vec![ObjectMeta {
            location: OsPath::from("some/other.nc"),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }];
        assert!(factory.discover_datasets(&objects).unwrap().is_empty());
    }

    // ── schema inference over the object store ──────────────────────────

    #[tokio::test]
    async fn infer_schema_unions_columns_across_datasets() {
        let store = test_store().await;
        let format = AtlasFormat::default();
        let ctx = SessionContext::new();

        let schema = format
            .infer_schema(&ctx.state(), &store, &[fixture_marker_object_meta()])
            .await
            .expect("infer");

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        for expected in ["temperature", "cycle", "season", "year"] {
            assert!(names.contains(&expected), "missing {expected} in {names:?}");
        }
    }

    #[tokio::test]
    async fn infer_schema_empty_objects_returns_empty_schema() {
        let store = test_store().await;
        let format = AtlasFormat::default();
        let ctx = SessionContext::new();
        let schema = format
            .infer_schema(&ctx.state(), &store, &[])
            .await
            .expect("infer");
        assert!(schema.fields().is_empty());
    }

    #[tokio::test]
    async fn file_source_returns_atlas_type() {
        let format = AtlasFormat::default();
        let source = format.file_source(
            datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
                arrow::datatypes::Schema::empty(),
            )),
        );
        assert_eq!(source.file_type(), "atlas");
    }

    // ── end-to-end through DataFusion + ListingTable ────────────────────

    /// Register the fixture store as a table backed by [`AtlasFormat`] over a
    /// `file://` object store (a `LocalFileSystem` DataFusion supplies).
    async fn register_example(ctx: &SessionContext) {
        // Ensure the fixture exists on disk, then point a ListingTable at it.
        let _ = test_store().await;
        let store_dir = std::env::temp_dir()
            .join("beacon-arrow-atlas-datasets")
            .join(super::test_support::FIXTURE_DIR);
        let store_dir = store_dir.to_string_lossy().replace('\\', "/");
        let table_path = ListingTableUrl::parse(format!("file:///{store_dir}/")).unwrap();

        let format: Arc<dyn FileFormat> = Arc::new(AtlasFormat::default());
        let listing_options = ListingOptions::new(format).with_file_extension("atlas.json");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        let table = ListingTable::try_new(config).unwrap();
        ctx.register_table("atlas_t", Arc::new(table)).unwrap();
    }

    #[tokio::test]
    async fn reads_all_datasets_through_datafusion() {
        let _ = LocalFileSystem::new(); // ensure the local store type is linked
        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let batches = ctx
            .sql("SELECT temperature FROM atlas_t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // winter (4) + summer (3) temperature values.
        assert_eq!(rows, 7);
    }

    #[tokio::test]
    async fn projection_prunes_columns_through_datafusion() {
        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let df = ctx.sql("SELECT temperature FROM atlas_t").await.unwrap();
        let names: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["temperature".to_string()]);
    }

    #[tokio::test]
    async fn predicate_pushdown_prunes_rows_through_datafusion() {
        let ctx = SessionContext::new();
        register_example(&ctx).await;

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
    async fn count_star_counts_every_dataset_row() {
        let ctx = SessionContext::new();
        register_example(&ctx).await;

        use arrow::array::Int64Array;
        let batches = ctx
            .sql("SELECT COUNT(*) AS n FROM atlas_t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        // winter contributes 4 rows, summer 3.
        assert_eq!(n, 7, "COUNT(*) must count rows from every dataset");
    }

    // ── cross-dataset dtype widening: cast + null-fill ──────────────────

    /// Register the widening fixture (`a.value: Int16`, `b.value: Float32`,
    /// `a.flag: Int32` only) as a table over its own `file://` store.
    async fn register_widening(ctx: &SessionContext) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        crate::reader::test_support::build_widening_store(tmp.path()).await;
        let dir = tmp.path().to_string_lossy().replace('\\', "/");
        let table_path = ListingTableUrl::parse(format!("file:///{dir}/")).unwrap();
        let format: Arc<dyn FileFormat> = Arc::new(AtlasFormat::default());
        let listing_options = ListingOptions::new(format).with_file_extension("atlas.json");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        ctx.register_table("w", Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
        tmp
    }

    #[tokio::test]
    async fn widened_array_dtype_is_cast_from_each_dataset() {
        use arrow::array::Float32Array;
        use arrow::datatypes::DataType;

        let ctx = SessionContext::new();
        let _tmp = register_widening(&ctx).await;

        // Int16 ∪ Float32 widens to Float32 (int16 is exactly representable),
        // so the table's `value` column is Float32 and each dataset is cast up.
        let df = ctx.sql("SELECT value FROM w ORDER BY value").await.unwrap();
        assert_eq!(
            df.schema().field_with_unqualified_name("value").unwrap().data_type(),
            &DataType::Float32,
            "merged value column must be the widened super-type"
        );

        let batches = df.collect().await.unwrap();
        let mut vals: Vec<f32> = Vec::new();
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<Float32Array>().unwrap();
            for i in 0..col.len() {
                vals.push(col.value(i));
            }
        }
        // a.value = [1,2] (Int16 → f32), b.value = [3.5,4.5] (Float32).
        assert_eq!(vals, vec![1.0, 2.0, 3.5, 4.5], "each dataset cast up to the super-type");
    }

    #[tokio::test]
    async fn missing_column_is_null_filled_per_dataset() {
        let ctx = SessionContext::new();
        let _tmp = register_widening(&ctx).await;

        // `flag` exists only in dataset `a` (2 rows); dataset `b`'s 2 rows must
        // null-fill it.
        let batches = ctx
            .sql("SELECT flag FROM w")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let (rows, nulls): (usize, usize) = batches
            .iter()
            .map(|b| (b.num_rows(), b.column(0).null_count()))
            .fold((0, 0), |(r, n), (br, bn)| (r + br, n + bn));
        assert_eq!(rows, 4, "both datasets contribute rows");
        assert_eq!(nulls, 2, "dataset b's rows null-fill the missing flag column");
    }

    /// A collection whose datasets give the same array *non-numeric* conflicting
    /// dtypes still reads: atlas widens `String` ∪ `Int64` to `String`, and the
    /// integer dataset is cast into `Utf8` rather than the scan failing. Guards
    /// the assumption that every merged dtype is castable-into from each
    /// dataset's native type.
    #[tokio::test]
    async fn incompatible_dtype_union_reads_both_datasets_as_strings() {
        use arrow::array::{Array, StringArray};
        use arrow::datatypes::DataType;

        let ctx = SessionContext::new();
        let tmp = tempfile::tempdir().unwrap();
        crate::reader::test_support::build_incompatible_store(tmp.path()).await;
        let dir = tmp.path().to_string_lossy().replace('\\', "/");
        let table_path = ListingTableUrl::parse(format!("file:///{dir}/")).unwrap();
        let format: Arc<dyn FileFormat> = Arc::new(AtlasFormat::default());
        let listing_options = ListingOptions::new(format).with_file_extension("atlas.json");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        ctx.register_table("m", Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();

        let df = ctx.sql("SELECT value FROM m ORDER BY value").await.unwrap();
        assert_eq!(
            df.schema().field_with_unqualified_name("value").unwrap().data_type(),
            &DataType::Utf8,
            "String wins the union, so the column is Utf8"
        );

        let batches = df.collect().await.unwrap();
        let mut vals: Vec<String> = Vec::new();
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..col.len() {
                vals.push(col.value(i).to_string());
            }
        }
        // a.value = ["x","y"] (native String); b.value = [1,2] (Int64, stringified).
        assert_eq!(vals, vec!["1", "2", "x", "y"], "both datasets contribute, integers cast to text");
    }

    // ── partition splitting ─────────────────────────────────────────────

    #[test]
    fn partition_dataset_names_round_robins_and_drops_empties() {
        let names: Vec<String> = (0..5).map(|i| format!("d{i}")).collect();
        let buckets = partition_dataset_names(names, 3);
        assert_eq!(buckets.len(), 3);
        // round-robin: [d0,d3], [d1,d4], [d2]
        assert_eq!(buckets[0], vec!["d0", "d3"]);
        assert_eq!(buckets[1], vec!["d1", "d4"]);
        assert_eq!(buckets[2], vec!["d2"]);

        // Fewer datasets than partitions → one bucket per dataset, no empties.
        let two = partition_dataset_names(vec!["x".into(), "y".into()], 8);
        assert_eq!(two.len(), 2);

        assert!(partition_dataset_names(Vec::new(), 4).is_empty());
    }

    // ── dataset pruning toggle ──────────────────────────────────────────

    /// Register the ranged fixture (`d{i}.temperature ∈ [10i, 10i+3]`, `n`
    /// datasets) with pruning `use_pruning` on or off.
    async fn register_ranged(ctx: &SessionContext, n: usize, use_pruning: bool) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        crate::reader::test_support::build_ranged_store(tmp.path(), n).await;
        let dir = tmp.path().to_string_lossy().replace('\\', "/");
        let table_path = ListingTableUrl::parse(format!("file:///{dir}/")).unwrap();
        let format: Arc<dyn FileFormat> =
            Arc::new(AtlasFormat::default().with_pruning(use_pruning));
        let listing_options = ListingOptions::new(format).with_file_extension("atlas.json");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        ctx.register_table("ranged", Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
        tmp
    }

    async fn ranged_row_count(use_pruning: bool, predicate: &str) -> usize {
        let ctx = SessionContext::new();
        let _tmp = register_ranged(&ctx, 10, use_pruning).await;
        ctx.sql(&format!("SELECT temperature FROM ranged WHERE {predicate}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    #[tokio::test]
    async fn pruning_matches_unpruned_results() {
        // `> 45` keeps d5..d9 → 5 datasets × 4 rows = 20 rows, whether or not
        // pruning is on. Pruning only changes which datasets get opened.
        assert_eq!(ranged_row_count(true, "temperature > 45").await, 20);
        assert_eq!(ranged_row_count(false, "temperature > 45").await, 20);

        // An impossible predicate → 0 rows both ways.
        assert_eq!(ranged_row_count(true, "temperature > 100000").await, 0);
        assert_eq!(ranged_row_count(false, "temperature > 100000").await, 0);
    }

    #[tokio::test]
    async fn pruning_on_mixed_dtype_column_end_to_end() {
        // End-to-end proof that pruning casts a mixed-dtype column to the merged
        // table type before filtering: `value` is Int16 in `a`, Float32 in `b`
        // (merged Float32). `value > 3` prunes `a` and keeps `b` — and the
        // result is byte-identical with pruning on and off.
        use arrow::array::Float32Array;

        async fn values(use_pruning: bool) -> Vec<f32> {
            let tmp = tempfile::tempdir().unwrap();
            crate::reader::test_support::build_widening_store(tmp.path()).await;
            let dir = tmp.path().to_string_lossy().replace('\\', "/");
            let table_path = ListingTableUrl::parse(format!("file:///{dir}/")).unwrap();
            let format: Arc<dyn FileFormat> =
                Arc::new(AtlasFormat::default().with_pruning(use_pruning));
            let listing_options = ListingOptions::new(format).with_file_extension("atlas.json");
            let ctx = SessionContext::new();
            let config = ListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .infer_schema(&ctx.state())
                .await
                .unwrap();
            ctx.register_table("w", Arc::new(ListingTable::try_new(config).unwrap()))
                .unwrap();

            let batches = ctx
                .sql("SELECT value FROM w WHERE value > 3 ORDER BY value")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0).as_any().downcast_ref::<Float32Array>().unwrap();
                for i in 0..col.len() {
                    out.push(col.value(i));
                }
            }
            out
        }

        // Only b's values exceed 3; a's [1,2] (Int16, cast to f32) are pruned out.
        assert_eq!(values(true).await, vec![3.5, 4.5]);
        assert_eq!(values(false).await, vec![3.5, 4.5], "toggle must not change results");
    }

    #[tokio::test]
    async fn pruning_across_many_partitions_is_correct() {
        // Many partitions all share one memoized prune result per store; the
        // union must still be exactly the matching rows, none dropped or doubled.
        use datafusion::prelude::SessionConfig;
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
        let _tmp = register_ranged(&ctx, 10, true).await;
        let rows: usize = ctx
            .sql("SELECT temperature FROM ranged WHERE temperature > 45")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 20, "d5..d9 × 4 rows, across 8 partitions");
    }

    #[tokio::test]
    async fn scan_metrics_report_pruned_and_scanned_counts() {
        use datafusion::physical_plan::metrics::MetricsSet;
        use datafusion::physical_plan::{ExecutionPlan, collect};

        let ctx = SessionContext::new();
        let _tmp = register_ranged(&ctx, 10, true).await;
        let plan = ctx
            .sql("SELECT temperature FROM ranged WHERE temperature > 45")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        collect(plan.clone(), ctx.task_ctx()).await.unwrap();

        // Find the scan node exposing the atlas metrics (aggregated over partitions).
        fn find(plan: &Arc<dyn ExecutionPlan>) -> Option<MetricsSet> {
            if let Some(m) = plan.metrics()
                && m.sum_by_name("atlas_datasets_scanned").is_some()
            {
                return Some(m);
            }
            plan.children().into_iter().find_map(find)
        }
        let m = find(&plan).expect("atlas scan metrics present");
        let sum = |name: &str| m.sum_by_name(name).map(|v| v.as_usize());

        // `> 45` keeps d5..d9 (5) and prunes d0..d4 (5), summed across partitions.
        assert_eq!(sum("atlas_datasets_scanned"), Some(5));
        assert_eq!(sum("atlas_datasets_pruned"), Some(5));
        // Timers are registered.
        assert!(m.sum_by_name("atlas_open_time").is_some());
        assert!(m.sum_by_name("atlas_prune_time").is_some());
        assert!(m.sum_by_name("atlas_dataset_build_time").is_some());
    }

    #[tokio::test]
    async fn pruning_off_by_option_still_correct() {
        // The same table, pruning disabled via the format flag, returns every
        // matching row — the toggle must never change results.
        let ctx = SessionContext::new();
        let _tmp = register_ranged(&ctx, 10, false).await;
        use arrow::array::Float32Array;
        let batches = ctx
            .sql("SELECT temperature FROM ranged WHERE temperature > 45 ORDER BY temperature")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut vals = Vec::new();
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<Float32Array>().unwrap();
            for i in 0..col.len() {
                vals.push(col.value(i));
            }
        }
        assert!(vals.iter().all(|v| *v > 45.0));
        assert_eq!(vals.len(), 20);
    }

    #[tokio::test]
    async fn partitioned_scan_reads_every_dataset_row() {
        // With target_partitions > 1 the store's datasets are split across
        // partitions; the union of all partitions must still be every row.
        use datafusion::prelude::SessionConfig;
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        register_example(&ctx).await;

        let rows: usize = ctx
            .sql("SELECT temperature FROM atlas_t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 7, "partitioned scan must not drop or duplicate rows");
    }
}
