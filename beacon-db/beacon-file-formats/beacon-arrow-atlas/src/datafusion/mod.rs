//! The DataFusion integration: discovering Atlas collections, typing them, and
//! planning a scan over their datasets.
//!
//! [`AtlasFormatFactory`] recognizes a collection in a listing and builds an
//! [`AtlasFormat`] per table. The format infers the collection's schema, then
//! plans a scan whose entries are *datasets* rather than files — see
//! [`source`] for what the openers then do with them.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use beacon_datafusion_ext::format_ext::{
    DatasetMetadata, FileFormatFactoryExt, SchemaOptions, SchemaUnit, units_over_stores,
};
use beacon_datafusion_ext::format_options::format_option;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::{LabeledSchema, session_widening};
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::{
            FileGroup, FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
        },
        table_schema::TableSchema,
    },
    error::{DataFusionError, Result},
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::config::AtlasConfig;
use crate::reader::collection_schema;
use crate::store::{
    ATLAS_MARKER, AtlasReaderCache, get_or_open_atlas, is_atlas_marker, top_level_atlas_markers,
};

pub mod metrics;
pub mod options;
pub mod pruning;
pub mod source;
pub mod statistics;
pub mod table_function;

pub use options::AtlasOptions;
pub use source::{AtlasEntry, AtlasSource};
pub use table_function::ReadAtlasFunc;

/// The name this format answers to: `STORED AS ATLAS`, `read_atlas`.
pub const ATLAS_FORMAT: &str = "atlas";

/// Parse a boolean supplied through `CREATE EXTERNAL TABLE ... OPTIONS`.
fn parse_bool_option(key: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(exec_datafusion_err!(
            "invalid boolean for atlas option '{key}': '{other}'"
        )),
    }
}

// ─── Factory ─────────────────────────────────────────────────────────────────

/// Builds an [`AtlasFormat`] per table, over one runtime's settings and one
/// shared reader cache.
#[derive(Debug, Clone)]
pub struct AtlasFormatFactory {
    pub options: AtlasOptions,
    pub config: AtlasConfig,
    /// The runtime's reader cache, sized from `config` and shared by every
    /// format, source and opener this factory builds.
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

    /// A format with this table's effective settings, wired to the shared cache
    /// when caching is on.
    fn build(
        &self,
        options: AtlasOptions,
        use_reader_cache: bool,
        use_pruning: bool,
    ) -> AtlasFormat {
        AtlasFormat::new(options)
            .with_cache(use_reader_cache.then(|| self.cache.clone()))
            .with_pruning(use_pruning)
    }

    /// Whether this table wants its columns measured at all.
    ///
    /// Only the file analyzer measures a collection, through
    /// [`FileFormatFactoryExt::create_for_analysis`]. This is the switch that
    /// turns even that off, per table or per runtime.
    fn statistics_wanted(&self, format_options: &HashMap<String, String>) -> Result<bool> {
        match format_option(format_options, "enable_statistics") {
            Some(value) => parse_bool_option("enable_statistics", value),
            None => Ok(self.config.enable_statistics),
        }
    }
}

impl FileFormatFactory for AtlasFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let mut options = self.options.clone();
        let mut use_reader_cache = self.config.use_reader_cache;
        let mut use_pruning = self.config.use_pruning;

        if let Some(value) = format_option(format_options, "read_dimensions") {
            options.read_dimensions = Some(
                value
                    .split(',')
                    .map(|dimension| dimension.trim().to_string())
                    .filter(|dimension| !dimension.is_empty())
                    .collect(),
            );
        }
        if let Some(value) = format_option(format_options, "use_reader_cache") {
            use_reader_cache = parse_bool_option("use_reader_cache", value)?;
        }
        if let Some(value) = format_option(format_options, "use_pruning") {
            use_pruning = parse_bool_option("use_pruning", value)?;
        }
        // Parsed here only so a bad value is an error at `CREATE EXTERNAL
        // TABLE` rather than at the first analysis pass. A query measures
        // nothing whatever it says: see `create_for_analysis`.
        self.statistics_wanted(format_options)?;

        Ok(Arc::new(self.build(options, use_reader_cache, use_pruning)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.build(
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
        ATLAS_FORMAT.to_string()
    }
}

impl FileFormatFactoryExt for AtlasFormatFactory {
    /// One dataset entry per collection, named by its container object.
    ///
    /// A collection's datasets are enumerated at plan time, not here: a listing
    /// of a data lake would otherwise open every collection it found.
    fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
        let format = self.get_ext();
        Ok(top_level_atlas_markers(objects)
            .into_iter()
            .map(|marker| DatasetMetadata::new(marker.location.to_string(), format.clone()))
            .collect())
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    /// One schema per collection, not per object.
    ///
    /// `infer_schema` reads the container and derives the schema from the
    /// footer inside it, so the entry is keyed on the container and depends on
    /// everything beside it — the deletion mask included, which changes which
    /// datasets the schema covers.
    fn schema_units(&self, objects: &[ObjectMeta]) -> Vec<SchemaUnit> {
        units_over_stores(objects, &top_level_atlas_markers(objects))
    }

    /// Atlas opts into the schema cache for a collection read whole.
    ///
    /// TODO(#367): cache a dimension-projected read too. `read_dimensions`
    /// decides which arrays survive, so one collection has one schema per
    /// dimension set and the key would have to carry the set in order. Left out
    /// of this pass to keep the four nd formats saying the same thing.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        let format = format.as_any().downcast_ref::<AtlasFormat>()?;
        if format.options.read_dimensions.is_some() {
            return None;
        }
        Some(SchemaOptions::new(ATLAS_FORMAT).finish())
    }

    /// The same format, with the column measurement switched on.
    ///
    /// `infer_stats` folds a collection's footer, which is cheap but not free
    /// over a listing of thousands. Only the file analyzer asks for it, and a
    /// scan prunes from what that recorded. See
    /// [`FileFormatFactoryExt::create_for_analysis`].
    fn create_for_analysis(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
        url: &ListingTableUrl,
        listing: &ListingFactory,
    ) -> Result<Arc<dyn FileFormat>> {
        let wanted = self.statistics_wanted(format_options)?;
        let format = self.create_with_native_root(state, format_options, url, listing)?;
        let atlas = format
            .as_any()
            .downcast_ref::<AtlasFormat>()
            .ok_or_else(|| {
                exec_datafusion_err!("the atlas factory did not produce an AtlasFormat")
            })?
            .clone();
        Ok(Arc::new(atlas.with_enable_statistics(wanted)))
    }
}

// ─── Format ──────────────────────────────────────────────────────────────────

/// Reads one table's worth of Atlas collections.
#[derive(Debug, Clone)]
pub struct AtlasFormat {
    pub options: AtlasOptions,
    /// The reader cache to consult, or `None` to bypass caching.
    cache: Option<AtlasReaderCache>,
    /// Whether a predicate scan drops the datasets it can rule out.
    use_pruning: bool,
    /// Whether [`FileFormat::infer_stats`] measures a collection's columns.
    enable_statistics: bool,
}

impl Default for AtlasFormat {
    fn default() -> Self {
        Self::new(AtlasOptions::default())
    }
}

impl AtlasFormat {
    pub fn new(options: AtlasOptions) -> Self {
        let defaults = AtlasConfig::default();
        Self {
            options,
            cache: None,
            // A query prunes by default: it only ever saves reads.
            use_pruning: defaults.use_pruning,
            // A query measures nothing. Only the analyzer asks, through
            // `create_for_analysis`.
            enable_statistics: false,
        }
    }

    /// Wire in a reader cache (`Some`), or bypass caching (`None`).
    pub fn with_cache(mut self, cache: Option<AtlasReaderCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Drop the datasets a predicate rules out, or read them all.
    pub fn with_pruning(mut self, use_pruning: bool) -> Self {
        self.use_pruning = use_pruning;
        self
    }

    /// Measure a collection's columns in [`FileFormat::infer_stats`], or report
    /// them unknown.
    pub fn with_enable_statistics(mut self, enable_statistics: bool) -> Self {
        self.enable_statistics = enable_statistics;
        self
    }
}

/// Wrap a scan in the nd spine: `NdBroadcastExec` over `NdSourceExec` over the
/// scan.
///
/// The scan carries its columns `beacon.nd`-encoded, one chunk per row, so
/// `NdSourceExec` decodes them and `NdBroadcastExec` broadcasts them back onto
/// the logical table schema above.
pub fn nd_scan_plan(conf: FileScanConfig) -> Result<Arc<dyn ExecutionPlan>> {
    let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
    let nd_source = Arc::new(beacon_datafusion_ext::nd::exec::NdSourceExec::try_new(
        scan,
    )?);
    Ok(Arc::new(
        beacon_datafusion_ext::nd::exec::NdBroadcastExec::try_new(nd_source)?,
    ))
}

#[async_trait::async_trait]
impl FileFormat for AtlasFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    /// The container's own name, which is what a listing matches on.
    fn get_ext(&self) -> String {
        ATLAS_MARKER.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok(ATLAS_MARKER.to_string())
    }

    /// The schema of every collection in the listing, merged.
    ///
    /// Each collection costs one open — a footer read — and the datasets behind
    /// it cost no I/O at all. Never enumerate a collection's datasets any other
    /// way here: at a million datasets that would turn planning into a scan.
    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let started = std::time::Instant::now();
        let markers = top_level_atlas_markers(objects);
        if markers.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }

        // One rule for both merges: the datasets inside a collection, and the
        // collections of this table.
        let widening = session_widening(state);
        let read_dimensions = self.options.read_dimensions.clone();

        let mut schemas = Vec::with_capacity(markers.len());
        for marker in &markers {
            let atlas = get_or_open_atlas(self.cache.as_ref(), Arc::clone(store), marker)
                .await
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            let label = marker.location.as_ref();
            let schema = collection_schema(&atlas, read_dimensions.as_deref(), label, &widening)
                .await
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            // The container names the schema, so a refused column names both
            // collections.
            schemas.push(LabeledSchema::new(schema, label));
        }

        let schema = widening.merge_schemas(&schemas).map_err(|e| {
            exec_datafusion_err!("Failed to merge the schemas of the atlas collections: {e}")
        })?;
        tracing::debug!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            collections = markers.len(),
            fields = schema.fields().len(),
            "atlas infer_schema",
        );
        Ok(schema)
    }

    /// The column ranges of one collection, folded out of its footer.
    ///
    /// Reporting unknown rather than erroring is deliberate throughout: absent
    /// statistics are always a legal answer, and they only mean Beacon reads
    /// what it might have skipped. A listing also hands this method every
    /// object it matched, and only a container has a collection behind it.
    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        if !self.enable_statistics || !is_atlas_marker(object) {
            return Ok(Statistics::new_unknown(&table_schema));
        }

        match get_or_open_atlas(self.cache.as_ref(), Arc::clone(store), object).await {
            Ok(atlas) => Ok(statistics::collection_statistics(&atlas, &table_schema)),
            Err(e) => {
                tracing::debug!(object = %object.location, "not measuring this collection: {e}");
                Ok(Statistics::new_unknown(&table_schema))
            }
        }
    }

    /// Plan one entry per dataset, then wrap the scan in the nd spine.
    ///
    /// Opening each collection here is one metadata read, and the openers reuse
    /// the same handle through the reader cache. `list_datasets` is in memory.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        beacon_nd_array::arrow::morsel::reject_partition_columns("Atlas", &conf)?;

        let started = std::time::Instant::now();
        let object_store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;

        let listed: Vec<ObjectMeta> = conf
            .file_groups
            .iter()
            .flat_map(|group| group.files())
            .map(|file| file.object_meta.clone())
            .collect();
        let markers = top_level_atlas_markers(&listed);

        let mut datasets = 0usize;
        let mut file_groups: Vec<FileGroup> = Vec::with_capacity(markers.len());
        for marker in &markers {
            let atlas = get_or_open_atlas(self.cache.as_ref(), Arc::clone(&object_store), marker)
                .await
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            let names = atlas.list_datasets();
            datasets += names.len();

            let entries: Vec<PartitionedFile> = names
                .into_iter()
                .enumerate()
                .map(|(position, dataset)| {
                    // `From<ObjectMeta>` keeps the container's freshness, so
                    // the opener's cache key matches this plan-time open.
                    let mut entry = PartitionedFile::from(marker.clone());
                    entry.extensions = Some(Arc::new(AtlasEntry { dataset, position }));
                    entry
                })
                .collect();
            file_groups.push(FileGroup::new(entries));
        }
        tracing::debug!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            collections = markers.len(),
            datasets,
            "atlas create_physical_plan",
        );

        // The scan carries nd columns, so the source's schema is the encoded
        // form of the logical table schema.
        let encoded = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            conf.file_schema(),
        ));
        let table_schema = TableSchema::new(encoded, conf.table_partition_cols().clone());
        // Preserve a projection already pushed into the incoming source;
        // rebuilding it below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();

        let source = AtlasSource::new(self.options.read_dimensions.clone(), table_schema)
            .with_cache(self.cache.clone())
            .with_pruning(self.use_pruning)
            .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_source(Arc::new(source))
            .build();

        nd_scan_plan(conf)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(
            AtlasSource::new(self.options.read_dimensions.clone(), table_schema)
                .with_cache(self.cache.clone())
                .with_pruning(self.use_pruning),
        )
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "an atlas collection is written once, by `atlas create`, and Beacon does not write one"
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use arrow::datatypes::DataType;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::Path;

    /// Register the collection in `dir` as `name`, through a listing table.
    async fn register(ctx: &SessionContext, dir: &Path, name: &str) {
        let format: Arc<dyn FileFormat> = Arc::new(AtlasFormat::default());
        register_with(ctx, dir, name, format).await;
    }

    async fn register_with(
        ctx: &SessionContext,
        dir: &Path,
        name: &str,
        format: Arc<dyn FileFormat>,
    ) {
        let directory = dir.to_string_lossy().replace('\\', "/");
        let url = ListingTableUrl::parse(format!("file://{directory}/")).unwrap();
        let listing = ListingOptions::new(format).with_file_extension(ATLAS_MARKER);
        let config = ListingTableConfig::new(url)
            .with_listing_options(listing)
            .infer_schema(&ctx.state())
            .await
            .expect("the collection types");
        ctx.register_table(name, Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
    }

    fn context(partitions: usize) -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions))
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> usize {
        ctx.sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum()
    }

    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        use arrow::array::Int64Array;
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("a count is an i64")
            .value(0)
    }

    // ── discovery ───────────────────────────────────────────────────────

    #[test]
    fn the_factory_answers_to_atlas() {
        let factory = AtlasFormatFactory::new(Default::default(), Default::default());
        assert_eq!(factory.get_ext(), "atlas");
        assert_eq!(factory.file_format_name(), "atlas");
        assert_eq!(AtlasFormat::default().get_ext(), "data.atlas");
    }

    #[test]
    fn one_dataset_entry_per_collection() {
        fn object(path: &str) -> ObjectMeta {
            ObjectMeta {
                location: object_store::path::Path::from(path),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
                version: None,
            }
        }

        let factory = AtlasFormatFactory::new(Default::default(), Default::default());
        let discovered = factory
            .discover_datasets(&[
                object("a/data.atlas"),
                object("a/deleted.mask"),
                object("b/data.atlas"),
                object("b/notes.txt"),
            ])
            .unwrap();

        let paths: Vec<&str> = discovered.iter().map(|d| d.file_path.as_str()).collect();
        assert_eq!(paths, vec!["a/data.atlas", "b/data.atlas"]);
        assert!(discovered.iter().all(|d| d.format == "atlas"));
    }

    // ── reading, end to end ─────────────────────────────────────────────

    #[tokio::test]
    async fn every_dataset_of_a_collection_is_read() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let ctx = context(1);
        register(&ctx, tmp.path(), "obs").await;

        // winter contributes 4 rows and summer 3.
        assert_eq!(rows(&ctx, "SELECT temperature FROM obs").await, 7);
    }

    #[tokio::test]
    async fn count_star_counts_every_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let ctx = context(1);
        register(&ctx, tmp.path(), "obs").await;

        assert_eq!(count(&ctx, "SELECT COUNT(*) FROM obs").await, 7);
    }

    /// The plan is the nd spine over the scan, in that nesting order.
    #[tokio::test]
    async fn the_plan_is_the_nd_spine_over_the_scan() {
        use datafusion::physical_plan::displayable;

        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let ctx = context(1);
        register(&ctx, tmp.path(), "obs").await;

        let plan = ctx
            .sql("SELECT temperature FROM obs")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();

        let broadcast = rendered.find("NdBroadcastExec");
        let source = rendered.find("NdSourceExec");
        let scan = rendered.find("DataSourceExec");
        assert!(
            broadcast.is_some() && source.is_some() && scan.is_some(),
            "the spine must be present:\n{rendered}"
        );
        assert!(
            broadcast < source && source < scan,
            "expected NdBroadcastExec over NdSourceExec over DataSourceExec:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn a_projection_reaches_the_result() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let ctx = context(1);
        register(&ctx, tmp.path(), "obs").await;

        let df = ctx.sql("SELECT temperature FROM obs").await.unwrap();
        let columns: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(columns, vec!["temperature".to_string()]);
    }

    /// An attribute rides along as a constant column on every row its dataset
    /// contributes.
    #[tokio::test]
    async fn an_attribute_is_constant_across_its_datasets_rows() {
        use arrow::array::{Array, StringArray};

        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let ctx = context(1);
        register(&ctx, tmp.path(), "obs").await;

        let batches = ctx
            .sql(r#"SELECT ".season" AS season FROM obs WHERE temperature < 10"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut seen = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("a text column");
            for row in 0..column.len() {
                seen.push(column.value(row).to_string());
            }
        }
        // Only winter's four rows are below 10 degrees.
        assert_eq!(seen, vec!["winter".to_string(); 4]);
    }

    // ── datasets that disagree ──────────────────────────────────────────

    #[tokio::test]
    async fn a_widened_column_is_cast_from_each_dataset() {
        use arrow::array::Float64Array;

        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;
        let ctx = context(1);
        register(&ctx, tmp.path(), "w").await;

        let df = ctx.sql("SELECT value FROM w ORDER BY value").await.unwrap();
        assert_eq!(
            df.schema()
                .field_with_unqualified_name("value")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );

        let batches = df.collect().await.unwrap();
        let mut values = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            values.extend(column.iter().flatten());
        }
        // a.value = [1, 2] as Int16, b.value = [3.5, 4.5] as Float32.
        assert_eq!(values, vec![1.0, 2.0, 3.5, 4.5]);
    }

    /// A dataset that lacks a projected column contributes its rows with that
    /// column null, rather than dropping them.
    #[tokio::test]
    async fn a_column_one_dataset_lacks_is_null_filled() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;
        let ctx = context(1);
        register(&ctx, tmp.path(), "w").await;

        let batches = ctx
            .sql("SELECT value, flag FROM w")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let (rows, nulls) = batches.iter().fold((0, 0), |(rows, nulls), batch| {
            (
                rows + batch.num_rows(),
                nulls + batch.column(1).null_count(),
            )
        });
        assert_eq!(rows, 4, "both datasets contribute their rows");
        assert_eq!(nulls, 2, "dataset b declares no flag");
    }

    // ── the deletion mask ───────────────────────────────────────────────

    /// A deleted dataset is gone from the result, and its rows with it.
    #[tokio::test]
    async fn a_deleted_dataset_is_not_read() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        test_support::open(tmp.path())
            .await
            .delete_dataset("winter")
            .await
            .expect("delete winter");

        let ctx = context(1);
        register(&ctx, tmp.path(), "obs").await;
        // Summer's three rows alone.
        assert_eq!(rows(&ctx, "SELECT temperature FROM obs").await, 3);
    }

    // ── dividing the scan ───────────────────────────────────────────────

    /// Every row is read exactly once, however many partitions share the
    /// collection. A dataset popped twice is a row returned twice, and one
    /// popped by nobody is a row lost; neither raises an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partitioned_scan_reads_every_row_once() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 12).await;

        for partitions in [1_usize, 2, 4, 8] {
            let ctx = context(partitions);
            register(&ctx, tmp.path(), "ranged").await;
            assert_eq!(
                rows(&ctx, "SELECT temperature FROM ranged").await,
                48,
                "partitions={partitions}: 12 datasets of 4 rows"
            );
            assert_eq!(
                count(&ctx, "SELECT COUNT(*) FROM ranged").await,
                48,
                "partitions={partitions}: and the count agrees"
            );
        }
    }

    /// The scan is planned across every partition, through the queue.
    #[tokio::test]
    async fn a_collection_is_planned_across_every_partition() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 12).await;
        let ctx = context(4);
        register(&ctx, tmp.path(), "ranged").await;

        let plan = ctx
            .sql("SELECT temperature FROM ranged")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        // The count comes off the scan, not the plan root: DataFusion adds a
        // round robin above a single-partition scan either way.
        let mut scan = Arc::clone(&plan);
        while let Some(child) = scan.children().first() {
            scan = Arc::clone(child);
        }
        assert_eq!(
            scan.output_partitioning().partition_count(),
            4,
            "the datasets divide over the partitions:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
        );
    }

    // ── predicates ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_predicate_keeps_only_the_rows_that_match() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let ctx = context(2);
        register(&ctx, tmp.path(), "ranged").await;

        // d5..d9 hold [50..53] … [90..93]: 20 rows above 45.
        assert_eq!(
            rows(
                &ctx,
                "SELECT temperature FROM ranged WHERE temperature > 45"
            )
            .await,
            20
        );
        assert_eq!(
            rows(
                &ctx,
                "SELECT temperature FROM ranged WHERE temperature > 100000"
            )
            .await,
            0,
            "a predicate nothing meets returns nothing"
        );
    }

    // ── pruning, end to end ─────────────────────────────────────────────

    /// Register the collection twice, once pruning and once not.
    async fn register_pruning(ctx: &SessionContext, dir: &Path, name: &str, use_pruning: bool) {
        let format: Arc<dyn FileFormat> =
            Arc::new(AtlasFormat::default().with_pruning(use_pruning));
        register_with(ctx, dir, name, format).await;
    }

    async fn values(ctx: &SessionContext, sql: &str) -> Vec<f32> {
        use arrow::array::Float32Array;
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("a float column");
            out.extend(column.iter().flatten());
        }
        out
    }

    /// The switch changes what is read, never what is returned.
    ///
    /// This is the property pruning has to hold above all others: it drops
    /// datasets that cannot contain a matching row, and nothing else.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pruning_does_not_change_the_answer() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;

        for predicate in [
            "temperature > 45",
            "temperature < 25",
            "temperature > 1000",
            "temperature >= 0",
            "temperature > 45 AND temperature < 75",
        ] {
            let sql =
                format!("SELECT temperature FROM ranged WHERE {predicate} ORDER BY temperature");

            let on = context(4);
            register_pruning(&on, tmp.path(), "ranged", true).await;
            let off = context(4);
            register_pruning(&off, tmp.path(), "ranged", false).await;

            assert_eq!(
                values(&on, &sql).await,
                values(&off, &sql).await,
                "pruning changed the answer for `{predicate}`"
            );
        }
    }

    /// Find the scan's metrics by the names only this format registers.
    fn atlas_metrics(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        if let Some(metrics) = plan.metrics()
            && metrics.sum_by_name("atlas_datasets_scanned").is_some()
        {
            return Some(metrics);
        }
        plan.children().into_iter().find_map(atlas_metrics)
    }

    /// The scan reports what it read and what it skipped, and the two add up to
    /// the collection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_metrics_report_what_was_read_and_what_was_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let ctx = context(4);
        register_pruning(&ctx, tmp.path(), "ranged", true).await;

        let plan = ctx
            .sql("SELECT temperature FROM ranged WHERE temperature > 45")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .unwrap();

        let metrics = atlas_metrics(&plan).expect("the atlas scan reports metrics");
        let sum = |name: &str| metrics.sum_by_name(name).map(|value| value.as_usize());

        // d5..d9 hold values above 45; d0..d4 cannot.
        assert_eq!(sum("atlas_datasets_scanned"), Some(5));
        assert_eq!(sum("atlas_datasets_pruned"), Some(5));
        assert_eq!(
            sum("atlas_index_rows"),
            Some(10),
            "the index covered them all"
        );
        assert!(metrics.sum_by_name("atlas_prune_time").is_some());
    }

    /// One index per collection, however many partitions share it.
    ///
    /// Every partition's opener holds the same memo, so the first to reach the
    /// collection builds the index and the rest await it. Without that, a
    /// twenty-four-partition scan would build it twenty-four times.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn one_index_is_built_per_collection() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 24).await;
        let ctx = context(8);
        register_pruning(&ctx, tmp.path(), "ranged", true).await;

        let plan = ctx
            .sql("SELECT temperature FROM ranged WHERE temperature > 100")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .unwrap();

        let metrics = atlas_metrics(&plan).expect("the atlas scan reports metrics");
        assert_eq!(
            metrics
                .sum_by_name("atlas_index_builds")
                .map(|value| value.as_usize()),
            Some(1),
            "eight partitions must share one index"
        );
    }

    /// A scan with no predicate builds no index at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_scan_without_a_predicate_builds_no_index() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        let ctx = context(4);
        register_pruning(&ctx, tmp.path(), "ranged", true).await;

        let plan = ctx
            .sql("SELECT temperature FROM ranged")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .unwrap();

        let metrics = atlas_metrics(&plan).expect("the atlas scan reports metrics");
        assert_eq!(
            metrics
                .sum_by_name("atlas_index_builds")
                .map(|value| value.as_usize()),
            Some(0)
        );
        assert_eq!(
            metrics
                .sum_by_name("atlas_datasets_pruned")
                .map(|value| value.as_usize()),
            Some(0)
        );
    }

    // ── measuring a collection ──────────────────────────────────────────

    /// A query never measures a collection, whatever the option says. Only the
    /// analyzer asks, through `create_for_analysis`.
    #[test]
    fn a_query_never_measures_a_collection() {
        let factory = AtlasFormatFactory::new(Default::default(), Default::default());
        let ctx = SessionContext::new();
        let on = HashMap::from([("enable_statistics".to_string(), "true".to_string())]);

        for options in [HashMap::new(), on] {
            let format = factory.create(&ctx.state(), &options).unwrap();
            assert!(
                !format
                    .as_any()
                    .downcast_ref::<AtlasFormat>()
                    .unwrap()
                    .enable_statistics,
                "a format built for a query measures nothing"
            );
        }
    }

    /// The analyzer layers the per-table option over the runtime default.
    #[test]
    fn analysis_layers_the_statistics_option_over_the_runtime() {
        let measured = |config: AtlasConfig, options: HashMap<String, String>| {
            let ctx = SessionContext::new();
            let listing = Arc::new(ListingFactory::dynamic());
            let url = ListingTableUrl::parse("file:///tmp/").unwrap();
            AtlasFormatFactory::new(Default::default(), config)
                .create_for_analysis(&ctx.state(), &options, &url, &listing)
                .unwrap()
                .as_any()
                .downcast_ref::<AtlasFormat>()
                .unwrap()
                .enable_statistics
        };
        let off = HashMap::from([("enable_statistics".to_string(), "false".to_string())]);
        let on = HashMap::from([("enable_statistics".to_string(), "yes".to_string())]);

        assert!(measured(AtlasConfig::default(), HashMap::new()));
        assert!(!measured(AtlasConfig::default(), off));

        let disabled = AtlasConfig {
            enable_statistics: false,
            ..Default::default()
        };
        assert!(!measured(disabled.clone(), HashMap::new()));
        assert!(measured(disabled, on), "one table can turn them back on");
    }

    // ── dimensions ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn read_dimensions_narrow_the_table() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::chunked_grid(tmp.path()).await;
        let ctx = context(1);
        let format: Arc<dyn FileFormat> = Arc::new(AtlasFormat::new(AtlasOptions {
            read_dimensions: Some(vec!["lat".to_string()]),
        }));
        register_with(&ctx, tmp.path(), "grid", format).await;

        let columns: Vec<String> = ctx
            .table_provider("grid")
            .await
            .unwrap()
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert!(
            !columns.contains(&"temperature".to_string()),
            "a 2-D array does not fit a 1-D grid: {columns:?}"
        );
    }

    // ── the table this crate is registered as ───────────────────────────

    /// The same collection through `FastObjectTable`, which is what
    /// `read_atlas` builds.
    ///
    /// A collection is one file, and the reader takes it as the marker it is;
    /// every other test here goes through `ListingTable`, which would not
    /// notice if that stopped being true.
    #[tokio::test]
    async fn a_collection_reads_through_the_fast_object_table() {
        use beacon_datafusion_ext::fast_object::FastObjectTable;
        use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
        use datafusion::execution::SessionStateBuilder;

        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_target_partitions(4)
                    .with_extension(ArrowTypeWidening::default_extension()),
            )
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        let directory = tmp.path().to_string_lossy().replace('\\', "/");
        let url = ListingTableUrl::parse(format!("file://{directory}/")).unwrap();
        let table =
            FastObjectTable::try_new(&ctx.state(), Arc::new(AtlasFormat::default()), vec![url])
                .await
                .expect("a collection registers as a table");
        ctx.register_table("obs", Arc::new(table)).unwrap();

        assert_eq!(rows(&ctx, "SELECT temperature FROM obs").await, 7);
    }

    // ── refusals ────────────────────────────────────────────────────────

    /// A dataset lives inside a container, not at a path, so no `PARTITIONED
    /// BY` value can be read off it. Saying so beats returning the column
    /// silently empty.
    #[tokio::test]
    async fn a_partitioned_table_is_refused_by_name() {
        use datafusion::datasource::physical_plan::FileScanConfigBuilder;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let table_schema = TableSchema::new(
            Arc::new(arrow::datatypes::Schema::empty()),
            vec![Arc::new(arrow::datatypes::Field::new(
                "year",
                DataType::Utf8,
                false,
            ))],
        );
        let source = AtlasSource::new(None, table_schema);
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source) as Arc<dyn FileSource>,
        )
        .build();

        let ctx = SessionContext::new();
        let error = AtlasFormat::default()
            .create_physical_plan(&ctx.state(), conf)
            .await
            .expect_err("a partitioned atlas table is refused")
            .to_string();
        assert!(error.contains("Atlas"), "{error}");
        assert!(error.contains("year"), "{error}");
    }

    #[test]
    fn an_unparseable_option_is_an_error() {
        let error = parse_bool_option("use_reader_cache", "maybe")
            .unwrap_err()
            .to_string();
        assert!(error.contains("use_reader_cache"), "{error}");
        assert!(error.contains("maybe"), "{error}");
    }

    /// A read that names dimensions stays out of the schema cache, because the
    /// key does not carry the dimension set. See the `TODO(#367)` above.
    #[test]
    fn a_dimension_projected_read_is_not_schema_cached() {
        let factory = AtlasFormatFactory::new(Default::default(), Default::default());
        assert!(
            factory
                .schema_options_fingerprint(&AtlasFormat::default())
                .is_some()
        );
        assert!(
            factory
                .schema_options_fingerprint(&AtlasFormat::new(AtlasOptions {
                    read_dimensions: Some(vec!["time".to_string()]),
                }))
                .is_none()
        );
    }
}
