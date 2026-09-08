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
use crate::store::{ATLAS_MARKER, AtlasReaderCache, get_or_open_atlas, top_level_atlas_markers};

pub mod metrics;
pub mod options;
pub mod pruning;
pub mod source;
pub mod table_function;

pub use options::AtlasOptions;
pub use source::AtlasSource;
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
    pub(crate) fn build(
        &self,
        options: AtlasOptions,
        use_reader_cache: bool,
        use_pruning: bool,
    ) -> AtlasFormat {
        AtlasFormat {
            options,
            cache: use_reader_cache.then(|| self.cache.clone()),
            use_pruning,
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

    /// The plain format. Atlas measures no column.
    ///
    /// The analyzer asks a format to measure a collection, and this one reports
    /// unknown for every column. See [`AtlasFormat::infer_stats`].
    fn create_for_analysis(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
        _url: &ListingTableUrl,
        _listing: &ListingFactory,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AtlasFormat::default()))
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
        }
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

    /// Unknown for every column.
    ///
    /// Atlas measures nothing for the analyzer. A recorded range prunes whole
    /// collections before a scan opens them, so a range that is too narrow
    /// deletes matching rows from an answer. The scan prunes from the footer
    /// itself instead, per dataset, where the numbers are exact and cost no
    /// array read. See [`pruning`].
    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    /// Plan one entry per collection, then wrap the scan in the nd spine.
    ///
    /// Nothing is opened here. The markers the listing found are deduped to the
    /// outermost collections and dealt round-robin over the target partitions,
    /// and each partition's opener lists, prunes and reads the collections it
    /// holds. Parallelism is therefore bounded by the collection count.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        beacon_nd_array::arrow::morsel::reject_partition_columns("Atlas", &conf)?;

        let listed: Vec<ObjectMeta> = conf
            .file_groups
            .iter()
            .flat_map(|group| group.files())
            .map(|file| file.object_meta.clone())
            .collect();
        let markers = top_level_atlas_markers(&listed);

        // One collection is one unit of work, and a container is never split,
        // so the deal here is the whole distribution.
        let partitions = state
            .config()
            .target_partitions()
            .clamp(1, markers.len().max(1));
        let mut dealt: Vec<Vec<PartitionedFile>> = vec![Vec::new(); partitions];
        for (index, marker) in markers.iter().enumerate() {
            dealt[index % partitions].push(PartitionedFile::from(marker.clone()));
        }
        let file_groups: Vec<FileGroup> = dealt
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(FileGroup::new)
            .collect();
        tracing::debug!(
            collections = markers.len(),
            partitions = file_groups.len(),
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
        use arrow::array::{Array, Float64Array};

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

    /// The unit of work is the collection, so one collection is one partition
    /// however many datasets it holds.
    #[tokio::test]
    async fn one_collection_is_one_partition() {
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
            1,
            "twelve datasets of one collection stay together:
{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
        );
    }

    /// Several collections deal round-robin over the partitions, and never
    /// past the collection count.
    #[tokio::test]
    async fn collections_deal_across_the_partitions() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let tmp = tempfile::tempdir().unwrap();
        for name in ["a", "b", "c"] {
            test_support::ranged(&tmp.path().join(name), 2).await;
        }
        let root = tmp
            .path()
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");

        for (target, expected) in [(8, 3), (2, 2), (1, 1)] {
            let ctx = ddl_context(target);
            ctx.sql(&format!(
                "CREATE EXTERNAL TABLE t STORED AS ATLAS LOCATION '{root}/**/data.atlas'"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

            let plan = ctx
                .sql("SELECT temperature FROM t")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let mut scan = Arc::clone(&plan);
            while let Some(child) = scan.children().first() {
                scan = Arc::clone(child);
            }
            assert_eq!(
                scan.output_partitioning().partition_count(),
                expected,
                "three collections over {target} target partitions"
            );
        }
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

    /// A format with pruning on or off, built the way a table is.
    ///
    /// `use_pruning` reaches a format through the factory alone, as
    /// `CREATE EXTERNAL TABLE ... OPTIONS ('use_pruning' '...')` does.
    fn format_with_pruning(use_pruning: bool) -> Arc<dyn FileFormat> {
        Arc::new(
            AtlasFormatFactory::new(Default::default(), Default::default()).build(
                AtlasOptions::default(),
                false,
                use_pruning,
            ),
        )
    }

    /// Register the collection twice, once pruning and once not.
    async fn register_pruning(ctx: &SessionContext, dir: &Path, name: &str, use_pruning: bool) {
        register_with(ctx, dir, name, format_with_pruning(use_pruning)).await;
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

    /// Atlas measures no column, for any caller.
    ///
    /// A recorded range prunes whole collections before a scan opens them, so a
    /// range that is too narrow deletes matching rows from an answer. The scan
    /// prunes from the footer instead, per dataset, where the numbers are exact
    /// and cost no array read.
    #[tokio::test]
    async fn a_collection_reports_no_column_range() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 4).await;
        let (store, marker) = test_support::store_and_marker(tmp.path());
        let ctx = SessionContext::new();

        let format = AtlasFormat::default();
        let schema = format
            .infer_schema(&ctx.state(), &store, std::slice::from_ref(&marker))
            .await
            .unwrap();
        let statistics = format
            .infer_stats(&ctx.state(), &store, Arc::clone(&schema), &marker)
            .await
            .unwrap();

        assert_eq!(statistics.column_statistics.len(), schema.fields().len());
        for (column, field) in statistics.column_statistics.iter().zip(schema.fields()) {
            assert!(
                column.min_value.is_exact().is_none(),
                "{} reports a minimum",
                field.name()
            );
            assert!(
                column.max_value.is_exact().is_none(),
                "{} reports a maximum",
                field.name()
            );
        }
        assert!(statistics.num_rows.is_exact().is_none());
    }

    /// The analyzer gets the same format a query does, because neither
    /// measures anything.
    #[test]
    fn analysis_asks_for_no_measurement() {
        let ctx = SessionContext::new();
        let listing = Arc::new(ListingFactory::dynamic());
        let url = ListingTableUrl::parse("file:///tmp/").unwrap();
        let factory = AtlasFormatFactory::new(Default::default(), Default::default());

        let analysis = factory
            .create_for_analysis(&ctx.state(), &HashMap::new(), &url, &listing)
            .unwrap();
        assert!(analysis.as_any().downcast_ref::<AtlasFormat>().is_some());
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
    // ── a wide collection on two dimensions, through SQL ─────────────────

    /// The shapes every test in this section reads: three datasets whose
    /// profile and level counts all differ.
    const WIDE: &[(usize, usize)] = &[(3, 4), (5, 4), (2, 6)];

    /// Rows a full-grid read returns: `profiles * levels`, summed per dataset.
    const WIDE_GRID_ROWS: usize = 44;

    /// Rows a read narrowed to `profile` returns: the profiles themselves.
    const WIDE_PROFILE_ROWS: usize = 10;

    /// Columns the merged collection carries: eight arrays, four attributes
    /// each, and two dataset attributes.
    const WIDE_COLUMNS: usize = 42;

    /// Columns that survive a narrowing to `profile`. The four grid arrays go;
    /// their attributes stay.
    const WIDE_PROFILE_COLUMNS: usize = 38;

    /// A wide collection in a temporary directory, and a table over it.
    ///
    /// The caller holds the returned directory. It deletes the collection when
    /// it drops.
    async fn wide_table(ctx: &SessionContext, format: Arc<dyn FileFormat>) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        test_support::wide_profiles(tmp.path(), WIDE).await;
        register_with(ctx, tmp.path(), "wide", format).await;
        tmp
    }

    /// One scalar, as its rendered text. Enough to pin a value without a
    /// downcast per Arrow type.
    async fn scalar(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
            .lines()
            .nth(3)
            .expect("a one-row result")
            .trim()
            .trim_matches('|')
            .trim()
            .to_string()
    }

    /// The row count is the grid every dataset contributes, and not the
    /// profile count.
    #[tokio::test]
    async fn a_wide_collection_reads_every_row() {
        let ctx = context(4);
        let _tmp = wide_table(&ctx, Arc::new(AtlasFormat::default())).await;

        assert_eq!(
            count(&ctx, "SELECT COUNT(*) FROM wide").await as usize,
            WIDE_GRID_ROWS
        );
    }

    #[tokio::test]
    async fn a_wide_table_carries_every_column() {
        let ctx = context(1);
        let _tmp = wide_table(&ctx, Arc::new(AtlasFormat::default())).await;

        let df = ctx.sql("SELECT * FROM wide").await.unwrap();
        assert_eq!(df.schema().fields().len(), WIDE_COLUMNS);
    }

    /// A per-profile array beside a per-level one, in one result.
    #[tokio::test]
    async fn a_wide_row_carries_both_of_its_grids() {
        let ctx = context(1);
        let _tmp = wide_table(&ctx, Arc::new(AtlasFormat::default())).await;

        let batches = ctx
            .sql(
                "SELECT platform, latitude, pressure, temperature, salinity \
                 FROM wide WHERE platform = 'set1' AND pressure = 2.0 \
                 ORDER BY latitude LIMIT 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rendered = arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        // set1 holds temperature = 100 + level, and level 2 is pressure 2.
        assert!(rendered.contains("102.0"), "{rendered}");
        assert!(rendered.contains("32.0"), "{rendered}");
    }

    /// An attribute rides along as a constant column.
    #[tokio::test]
    async fn an_attribute_of_a_wide_collection_reads_as_a_column() {
        let ctx = context(1);
        let _tmp = wide_table(&ctx, Arc::new(AtlasFormat::default())).await;

        assert_eq!(
            scalar(&ctx, "SELECT DISTINCT \".title\" FROM wide").await,
            "wide profiles"
        );
        assert_eq!(
            scalar(&ctx, "SELECT DISTINCT \"temperature.long_name\" FROM wide").await,
            "the temperature"
        );
    }

    /// A predicate over a real column, with a known answer. Each dataset owns a
    /// disjoint `temperature` range, so this is the pruning arithmetic too.
    #[tokio::test]
    async fn a_predicate_over_a_wide_collection_selects_the_rows_that_match() {
        let ctx = context(4);
        let _tmp = wide_table(&ctx, Arc::new(AtlasFormat::default())).await;

        // Only set2 reaches past 150: its 2 * 6 cells hold 200 to 205.
        assert_eq!(
            count(&ctx, "SELECT COUNT(*) FROM wide WHERE temperature > 150.0").await,
            12
        );
        assert_eq!(
            count(&ctx, "SELECT COUNT(DISTINCT platform) FROM wide").await,
            3
        );
    }

    /// Pruning reads the footer's per-dataset ranges and drops what cannot
    /// match. It must not change an answer, whichever way the switch is set.
    #[tokio::test]
    async fn pruning_does_not_change_the_answers_over_a_wide_collection() {
        let queries = [
            "SELECT COUNT(*) FROM wide WHERE temperature > 150.0",
            "SELECT COUNT(*) FROM wide WHERE latitude > 12.0",
            "SELECT COUNT(*) FROM wide WHERE platform = 'set1'",
            "SELECT COUNT(*) FROM wide WHERE salinity < 32.0",
        ];

        let pruned = context(4);
        let _a = wide_table(&pruned, Arc::new(AtlasFormat::default())).await;
        let whole = context(4);
        let _b = wide_table(&whole, format_with_pruning(false)).await;

        for sql in queries {
            assert_eq!(
                count(&pruned, sql).await,
                count(&whole, sql).await,
                "pruning changed the answer to: {sql}"
            );
        }
    }

    /// A projected scan over a pruned collection.
    ///
    /// The predicate is indexed against the table schema, and the pruning
    /// engine reads it against the projected one. `temperature` sits at column
    /// 32 of 42, so a five-column projection puts that index out of range. This
    /// is the case where the two must be brought into step.
    #[tokio::test]
    async fn a_projected_scan_prunes_without_losing_its_columns() {
        let ctx = context(4);
        let _tmp = wide_table(&ctx, Arc::new(AtlasFormat::default())).await;

        let rows = rows(
            &ctx,
            "SELECT platform, latitude, longitude, pressure, temperature \
             FROM wide WHERE temperature IS NOT NULL \
             ORDER BY platform, latitude, pressure LIMIT 5",
        )
        .await;
        assert_eq!(rows, 5);
    }

    /// A narrowing to `profile` reads the profiles and not their levels.
    ///
    /// `COUNT(*)` projects nothing, so the scan picks the widest array of the
    /// dataset to count. That array lives on both dimensions, and this
    /// narrowing drops it. So the driver has to respect the dimensions too.
    #[tokio::test]
    async fn narrowing_a_wide_table_to_one_dimension_reads_one_row_per_profile() {
        let ctx = context(4);
        let _tmp = wide_table(
            &ctx,
            Arc::new(AtlasFormat::new(AtlasOptions {
                read_dimensions: Some(vec!["profile".to_string()]),
            })),
        )
        .await;

        assert_eq!(
            count(&ctx, "SELECT COUNT(*) FROM wide").await as usize,
            WIDE_PROFILE_ROWS
        );
        // The same number through a column, rather than the count driver.
        assert_eq!(
            count(&ctx, "SELECT COUNT(latitude) FROM wide").await as usize,
            WIDE_PROFILE_ROWS
        );
        assert_eq!(
            ctx.sql("SELECT * FROM wide")
                .await
                .unwrap()
                .schema()
                .fields()
                .len(),
            WIDE_PROFILE_COLUMNS
        );
    }

    /// A narrowed read whose projection names only attributes builds no
    /// dimensioned array. The narrowing has nothing to drop and must still
    /// succeed.
    #[tokio::test]
    async fn a_narrowed_read_of_attributes_alone_succeeds() {
        let ctx = context(1);
        let _tmp = wide_table(
            &ctx,
            Arc::new(AtlasFormat::new(AtlasOptions {
                read_dimensions: Some(vec!["profile".to_string()]),
            })),
        )
        .await;

        assert_eq!(
            scalar(&ctx, "SELECT DISTINCT \"temperature.units\" FROM wide").await,
            "1"
        );
        assert_eq!(
            scalar(&ctx, "SELECT DISTINCT \".institution\" FROM wide").await,
            "test"
        );
    }

    /// Every dataset is one unit of work, and a partitioned scan divides them
    /// without reading one twice.
    #[tokio::test]
    async fn a_partitioned_scan_of_a_wide_collection_reads_every_row_once() {
        for partitions in [1, 2, 8] {
            let ctx = context(partitions);
            let _tmp = wide_table(&ctx, Arc::new(AtlasFormat::default())).await;
            assert_eq!(
                count(&ctx, "SELECT COUNT(*) FROM wide").await as usize,
                WIDE_GRID_ROWS,
                "over {partitions} partitions"
            );
        }
    }

    // ── STORED AS ATLAS ──────────────────────────────────────────────────

    /// A session that answers `STORED AS ATLAS`, as the runtime builds one.
    ///
    /// Two lookups sit behind that clause, under two spellings of one name.
    /// DataFusion resolves the `STORED AS` word in `table_factories`, upper
    /// cased, and Beacon registers [`ListingTableFactoryExt`] there. That
    /// factory then resolves the *file format* by the same word lower cased,
    /// which is where [`ATLAS_FORMAT`] answers.
    fn ddl_context(partitions: usize) -> SessionContext {
        use beacon_datafusion_ext::listing_table_factory_ext::ListingTableFactoryExt;
        use datafusion::execution::session_state::SessionStateBuilder;

        let mut config = SessionConfig::new()
            .with_target_partitions(partitions)
            .with_extension(Arc::new(ListingFactory::dynamic()))
            .with_extension(Arc::new(ListingTableFactoryExt));
        // What the runtime sets. DataFusion's default matches a glob against
        // the file name alone, and a collection is always one directory down,
        // so `**/data.atlas` would list nothing and the table would be empty.
        config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = false;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_table_factory(
                ATLAS_FORMAT.to_uppercase(),
                Arc::new(ListingTableFactoryExt),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.state_ref()
            .write()
            .register_file_format(
                Arc::new(AtlasFormatFactory::new(
                    Default::default(),
                    Default::default(),
                )),
                true,
            )
            .expect("the atlas format registers under its own name");
        ctx
    }

    /// A directory as SQL takes it: forward slashes, whatever the platform.
    fn location(dir: &Path) -> String {
        dir.to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/")
    }

    /// Run one DDL statement, and name the statement if it is refused.
    async fn run_ddl(ctx: &SessionContext, sql: &str) {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|e| panic!("refused `{sql}`: {e}"))
            .collect()
            .await
            .unwrap();
    }

    /// The clause a user writes, over the directory that holds the container.
    #[tokio::test]
    async fn stored_as_atlas_reads_a_collection_directory() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::wide_profiles(tmp.path(), WIDE).await;
        let ctx = ddl_context(4);

        run_ddl(
            &ctx,
            &format!(
                "CREATE EXTERNAL TABLE t STORED AS ATLAS LOCATION '{}/'",
                location(tmp.path())
            ),
        )
        .await;

        assert_eq!(
            count(&ctx, "SELECT COUNT(*) FROM t").await as usize,
            WIDE_GRID_ROWS
        );
        assert_eq!(
            ctx.sql("SELECT * FROM t")
                .await
                .unwrap()
                .schema()
                .fields()
                .len(),
            WIDE_COLUMNS
        );
    }

    /// The container object names the collection just as well as its directory
    /// does. That is what a `LOCATION` copied out of a listing looks like.
    #[tokio::test]
    async fn stored_as_atlas_reads_the_container_object() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::wide_profiles(tmp.path(), WIDE).await;
        let ctx = ddl_context(4);

        run_ddl(
            &ctx,
            &format!(
                "CREATE EXTERNAL TABLE t STORED AS ATLAS LOCATION '{}/{ATLAS_MARKER}'",
                location(tmp.path())
            ),
        )
        .await;

        assert_eq!(
            count(&ctx, "SELECT COUNT(*) FROM t").await as usize,
            WIDE_GRID_ROWS
        );
    }

    /// SQL folds the `STORED AS` word, so either spelling reaches the format.
    #[tokio::test]
    async fn stored_as_atlas_is_case_insensitive() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::wide_profiles(tmp.path(), WIDE).await;

        for spelling in ["ATLAS", "atlas", "Atlas"] {
            let ctx = ddl_context(1);
            run_ddl(
                &ctx,
                &format!(
                    "CREATE EXTERNAL TABLE t STORED AS {spelling} LOCATION '{}/'",
                    location(tmp.path())
                ),
            )
            .await;

            assert_eq!(
                count(&ctx, "SELECT COUNT(*) FROM t").await as usize,
                WIDE_GRID_ROWS,
                "STORED AS {spelling}"
            );
        }
    }

    /// `OPTIONS` reaches the format, so a table can name its dimensions
    /// without the `read_atlas` function.
    #[tokio::test]
    async fn stored_as_atlas_takes_its_options() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::wide_profiles(tmp.path(), WIDE).await;
        let ctx = ddl_context(4);

        run_ddl(
            &ctx,
            &format!(
                "CREATE EXTERNAL TABLE t STORED AS ATLAS \
                 OPTIONS ('read_dimensions' 'profile') LOCATION '{}/'",
                location(tmp.path())
            ),
        )
        .await;

        assert_eq!(
            count(&ctx, "SELECT COUNT(*) FROM t").await as usize,
            WIDE_PROFILE_ROWS
        );
    }

    /// An `OPTIONS` value is a string, so a list of dimensions is one comma
    /// separated string. `read_atlas` takes a real SQL list and joins it into
    /// the same option, so both spellings reach one place.
    ///
    /// Both dimensions of this collection reads it whole, which is what the
    /// default already does. The point is the parse, not the answer.
    #[tokio::test]
    async fn stored_as_atlas_takes_a_comma_separated_dimension_list() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::wide_profiles(tmp.path(), WIDE).await;

        // A space after a comma, and a trailing comma, are both tolerated.
        for list in ["profile,level", "profile, level", "level,profile,"] {
            let ctx = ddl_context(4);
            run_ddl(
                &ctx,
                &format!(
                    "CREATE EXTERNAL TABLE t STORED AS ATLAS \
                     OPTIONS ('read_dimensions' '{list}') LOCATION '{}/'",
                    location(tmp.path())
                ),
            )
            .await;

            assert_eq!(
                count(&ctx, "SELECT COUNT(*) FROM t").await as usize,
                WIDE_GRID_ROWS,
                "'{list}' did not name the whole grid"
            );
            assert_eq!(
                ctx.sql("SELECT * FROM t")
                    .await
                    .unwrap()
                    .schema()
                    .fields()
                    .len(),
                WIDE_COLUMNS,
                "'{list}'"
            );
        }
    }

    /// A glob puts several collections in one table, and the rows are their
    /// union.
    ///
    /// This is the form the docs give for a data lake:
    /// `LOCATION 'collections/**/data.atlas'`. It rests on
    /// `listing_table_ignore_subdirectory` being off, because a collection's
    /// container always sits one directory below the glob's prefix.
    #[tokio::test]
    async fn stored_as_atlas_globs_several_collections_into_one_table() {
        // Two collections under one root, so the union is a number neither one
        // could produce alone.
        let tmp = tempfile::tempdir().unwrap();
        for name in ["one", "two"] {
            test_support::wide_profiles(&tmp.path().join(name), WIDE).await;
        }
        let root = location(tmp.path());

        for glob in ["**/data.atlas", "*/data.atlas"] {
            let ctx = ddl_context(4);
            run_ddl(
                &ctx,
                &format!("CREATE EXTERNAL TABLE t STORED AS ATLAS LOCATION '{root}/{glob}'"),
            )
            .await;

            assert_eq!(
                count(&ctx, "SELECT COUNT(*) FROM t").await as usize,
                2 * WIDE_GRID_ROWS,
                "'{glob}' did not read both collections"
            );
            // One schema over both, merged under the session's widening rule.
            assert_eq!(
                ctx.sql("SELECT * FROM t")
                    .await
                    .unwrap()
                    .schema()
                    .fields()
                    .len(),
                WIDE_COLUMNS,
                "'{glob}'"
            );
        }
    }

    /// A bad option is an error at `CREATE EXTERNAL TABLE`, not at the first
    /// query against the table.
    #[tokio::test]
    async fn stored_as_atlas_refuses_a_bad_option_at_ddl() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::wide_profiles(tmp.path(), WIDE).await;
        let ctx = ddl_context(1);

        let error = ctx
            .sql(&format!(
                "CREATE EXTERNAL TABLE t STORED AS ATLAS \
                 OPTIONS ('use_pruning' 'maybe') LOCATION '{}/'",
                location(tmp.path())
            ))
            .await
            .expect_err("'maybe' is no boolean")
            .to_string();

        assert!(error.contains("use_pruning"), "{error}");
        assert!(error.contains("maybe"), "{error}");
    }
    /// A session whose merge keeps the first type of a conflicting column.
    ///
    /// The default refuses such a column outright, so a test of the cast has to
    /// ask for `KeepFirst`.
    fn keep_first_context(partitions: usize) -> SessionContext {
        use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, DefaultArrowTypeWidening};

        SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(partitions)
                .with_extension(Arc::new(ArrowTypeWidening::new(Arc::new(
                    DefaultArrowTypeWidening::keeping_first_type(),
                )))),
        )
    }

    /// A value the merged type cannot hold reads as null, and does not fail the
    /// query.
    ///
    /// `KeepFirst` keeps `Float64` and marks the column. The mark has to
    /// survive the nd encoding, because the scan's target schema is the encoded
    /// one. Without it `scan_adapt` casts strictly and `'0.-90'` fails the whole
    /// scan.
    ///
    /// `'3.5'` casts cleanly beside it, so this separates "one value is null"
    /// from "the column gave up".
    #[tokio::test]
    async fn a_value_the_merged_type_cannot_hold_reads_as_null() {
        use arrow::array::{Array, Float64Array};

        let tmp = tempfile::tempdir().unwrap();
        test_support::conflicting_numbers(tmp.path()).await;
        let ctx = keep_first_context(1);
        register(&ctx, tmp.path(), "t").await;

        let batches = ctx
            .sql("SELECT value FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .expect("an unparseable value is null, not an error");

        let mut seen: Vec<Option<f64>> = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("the merge kept the first type");
            for row in 0..column.len() {
                seen.push((!column.is_null(row)).then(|| column.value(row)));
            }
        }
        seen.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(seen, vec![None, Some(1.5), Some(2.5), Some(3.5)]);
    }

    /// The mark itself, on the schema the scan carries.
    ///
    /// The logical schema is marked, and the encoded schema has to stay marked.
    /// This is the step that used to drop it.
    #[test]
    fn the_nd_encoding_keeps_the_type_conflict_mark() {
        use beacon_datafusion_ext::nd::encoded_schema;
        use beacon_datafusion_ext::type_widening::{
            TYPE_CONFLICT_FIRST_TYPE, TYPE_CONFLICT_KEY, is_type_conflict,
        };

        let marked = arrow::datatypes::Field::new("value", DataType::Float64, true).with_metadata(
            std::collections::HashMap::from([(
                TYPE_CONFLICT_KEY.to_string(),
                TYPE_CONFLICT_FIRST_TYPE.to_string(),
            )]),
        );
        let plain = arrow::datatypes::Field::new("other", DataType::Float64, true);
        let logical = Schema::new(vec![marked, plain]);

        let encoded = encoded_schema(&logical);
        assert!(
            is_type_conflict(encoded.field_with_name("value").unwrap()),
            "the encoded field lost the mark"
        );
        assert!(
            !is_type_conflict(encoded.field_with_name("other").unwrap()),
            "an unmarked column must not gain the mark"
        );
        // The extension tag still identifies the column as nd-encoded.
        assert!(beacon_datafusion_ext::nd::is_nd_encoded(
            encoded.field_with_name("value").unwrap()
        ));
    }
    /// What the *default* widening does with Float64 beside Utf8.
    #[tokio::test]
    async fn probe_default_widening() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::conflicting_numbers(tmp.path()).await;

        // Default session: no widening extension registered at all.
        let ctx = context(1);
        let dir = tmp
            .path()
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        let url = ListingTableUrl::parse(format!("file://{dir}/")).unwrap();
        let format: Arc<dyn FileFormat> = Arc::new(AtlasFormat::default());
        let listing = ListingOptions::new(format).with_file_extension(ATLAS_MARKER);
        match ListingTableConfig::new(url)
            .with_listing_options(listing)
            .infer_schema(&ctx.state())
            .await
        {
            Ok(config) => {
                let schema = config.file_schema.clone().unwrap();
                let field = schema.field_with_name("value").unwrap();
                println!("PROBE default infer OK type={:?}", field.data_type());
                println!(
                    "PROBE default marked={}",
                    beacon_datafusion_ext::type_widening::is_type_conflict(field)
                );
                ctx.register_table("t", Arc::new(ListingTable::try_new(config).unwrap()))
                    .unwrap();
                match ctx
                    .sql("SELECT value FROM t")
                    .await
                    .unwrap()
                    .collect()
                    .await
                {
                    Ok(b) => println!(
                        "PROBE default query OK\n{}",
                        arrow::util::pretty::pretty_format_batches(&b).unwrap()
                    ),
                    Err(e) => println!("PROBE default query ERR {e}"),
                }
            }
            Err(e) => println!("PROBE default infer ERR {e}"),
        }
    }

    /// The same value, with no mark on the column at all.
    ///
    /// An nd cast lands on the `values` list inside the `beacon.nd` struct, and
    /// it reads leniently whether or not the merge marked the column. A table
    /// whose schema says `Float64` therefore survives a dataset that stores the
    /// array as text, however the schema came to say so.
    #[tokio::test]
    async fn an_unparseable_nd_value_reads_as_null_without_a_mark() {
        use arrow::array::{Array, Float64Array};

        let tmp = tempfile::tempdir().unwrap();
        test_support::value_typed(&tmp.path().join("one"), false).await;
        test_support::value_typed(&tmp.path().join("two"), true).await;

        // Two paths, so the table schema comes from the first collection alone
        // and carries no mark. That is the shape a scan cannot rely on one.
        let ctx = keep_first_context(4);
        let urls: Vec<ListingTableUrl> = ["one", "two"]
            .iter()
            .map(|name| {
                let dir = tmp
                    .path()
                    .join(name)
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                ListingTableUrl::parse(format!("file://{dir}/")).unwrap()
            })
            .collect();
        let format: Arc<dyn FileFormat> = Arc::new(AtlasFormat::default());
        let listing = ListingOptions::new(format).with_file_extension(ATLAS_MARKER);
        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing)
            .infer_schema(&ctx.state())
            .await
            .expect("the collections type");
        let schema = config.file_schema.clone().unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Float64);
        assert!(
            !beacon_datafusion_ext::type_widening::is_type_conflict(field),
            "this shape is the one with no mark; the test is pointless with one"
        );

        ctx.register_table("t", Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
        let batches = ctx
            .sql("SELECT value FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .expect("an unparseable value is null, not an error");

        let mut seen: Vec<Option<f64>> = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("the table type");
            for row in 0..column.len() {
                seen.push((!column.is_null(row)).then(|| column.value(row)));
            }
        }
        seen.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(seen, vec![None, Some(1.5), Some(2.5), Some(3.5)]);
    }
}
