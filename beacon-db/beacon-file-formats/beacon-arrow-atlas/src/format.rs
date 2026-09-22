//! The DataFusion file format: typing the collections of a table and
//! planning a scan over them.
//!
//! [`AtlasFormatFactory`] recognizes a collection and builds an
//! [`AtlasFormat`] per table; scan entries are collections, not files.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context as _;
use arrow::datatypes::{Schema, SchemaRef};
use beacon_datafusion_ext::cancel::query_cancellation;
use beacon_datafusion_ext::format_ext::{
    DatasetMetadata, FileFormatFactoryExt, SchemaOptions, SchemaUnit, units_over_stores,
};
use beacon_datafusion_ext::format_options::format_option;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, LabeledSchema, session_widening};
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::ListingTableUrl,
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
        table_schema::TableSchema,
    },
    error::{DataFusionError, Result},
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

use crate::discover::{ATLAS_MARKER, atlas_markers, deal_rotated};
use crate::error::external;
use crate::open::{AtlasReaderCache, get_or_open_atlas};
use crate::options::AtlasOptions;
use crate::schema;
pub use crate::source::AtlasSource;

/// The name this format answers to: `STORED AS ATLAS`, `read_atlas`.
pub const ATLAS_FORMAT: &str = "atlas";

/// Builds an [`AtlasFormat`] per table, over one runtime's settings and one
/// reader cache shared by every table and query of the runtime.
#[derive(Debug, Clone)]
pub struct AtlasFormatFactory {
    pub options: AtlasOptions,
    cache: AtlasReaderCache,
}

impl AtlasFormatFactory {
    pub fn new(options: AtlasOptions) -> Self {
        Self {
            options,
            cache: AtlasReaderCache::new(READER_CACHE_CAPACITY),
        }
    }

    /// A format on `options`, over the shared cache.
    fn format(&self, options: AtlasOptions) -> Arc<dyn FileFormat> {
        Arc::new(AtlasFormat::with_cache(options, self.cache.clone()))
    }
}

/// How many opened collections the runtime keeps. Each holds its own block
/// and I/O caches, so this bounds memory as well as handles.
const READER_CACHE_CAPACITY: u64 = 512;

/// How many footers schema inference reads at once.
const SCHEMA_OPENS_IN_FLIGHT: usize = 16;

impl FileFormatFactory for AtlasFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let mut options = self.options.clone();

        if let Some(value) = format_option(format_options, "read_dimensions") {
            options.read_dimensions = Some(
                value
                    .split(',')
                    .map(|dimension| dimension.trim().to_string())
                    .filter(|dimension| !dimension.is_empty())
                    .collect(),
            );
        }
        Ok(self.format(options))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        self.format(self.options.clone())
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
    /// Datasets are enumerated at plan time; a listing must not open collections.
    fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
        let format = self.get_ext();
        Ok(atlas_markers(objects)
            .into_iter()
            .map(|marker| DatasetMetadata::new(marker.location.to_string(), format.clone()))
            .collect())
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    /// One schema per collection, not per object.
    fn schema_units(&self, objects: &[ObjectMeta]) -> Vec<SchemaUnit> {
        units_over_stores(objects, &atlas_markers(objects))
    }

    /// Atlas opts into the schema cache for a collection read whole.
    ///
    /// TODO(#367): also cache a dimension-projected read.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        let format = format.as_any().downcast_ref::<AtlasFormat>()?;
        if format.options.read_dimensions.is_some() {
            return None;
        }
        Some(SchemaOptions::new(ATLAS_FORMAT).finish())
    }

    /// The plain format. Atlas measures no column.
    ///
    /// See [`AtlasFormat::infer_stats`].
    fn create_for_analysis(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
        _url: &ListingTableUrl,
        _listing: &ListingFactory,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(self.format(AtlasOptions::default()))
    }
}

/// Reads one table's worth of Atlas collections.
#[derive(Debug, Clone)]
pub struct AtlasFormat {
    pub options: AtlasOptions,
    cache: AtlasReaderCache,
}

impl Default for AtlasFormat {
    fn default() -> Self {
        Self::new(AtlasOptions::default())
    }
}

impl AtlasFormat {
    /// A format with a reader cache of its own. The factory shares one
    /// instead, see [`AtlasFormat::with_cache`].
    pub fn new(options: AtlasOptions) -> Self {
        Self::with_cache(options, AtlasReaderCache::new(READER_CACHE_CAPACITY))
    }

    /// A format that opens its collections through `cache`.
    pub fn with_cache(options: AtlasOptions, cache: AtlasReaderCache) -> Self {
        Self { options, cache }
    }

    /// The Arrow schema of the collection at `marker`, labeled by its path.
    /// One footer read through the cache.
    async fn collection_schema(
        &self,
        store: &Arc<dyn ObjectStore>,
        marker: &ObjectMeta,
        widening: &ArrowTypeWidening,
    ) -> Result<LabeledSchema> {
        let atlas = get_or_open_atlas(Some(&self.cache), Arc::clone(store), marker)
            .await
            .map_err(external)?;
        let schema = schema::collection_arrow_schema(&atlas.footer().collection_schema(), widening)
            .with_context(|| {
                format!(
                    "reading the schema of atlas collection '{}'",
                    marker.location
                )
            })
            .map_err(external)?;
        Ok(LabeledSchema::new(
            Arc::new(schema),
            marker.location.as_ref(),
        ))
    }

    /// A source over `table_schema`, on this table's settings and reader cache.
    fn source(&self, table_schema: TableSchema) -> AtlasSource {
        AtlasSource::new(
            self.options.read_dimensions.clone(),
            table_schema,
            self.cache.clone(),
        )
    }
}

pub use beacon_datafusion_ext::nd::exec::nd_scan_plan;

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

    /// The schema of every collection in the listing, merged. Each collection
    /// costs one footer read, a few at a time. Never enumerate its datasets
    /// here, or planning becomes a scan.
    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let started = std::time::Instant::now();
        let markers = atlas_markers(objects);
        if markers.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }

        // One rule for both merges: datasets inside a collection, and collections of this table.
        let widening = session_widening(state);

        // `buffered` keeps the listing order, so the merge names the same
        // collection first however the opens complete.
        let opens: Vec<_> = markers
            .iter()
            .map(|marker| self.collection_schema(store, marker, &widening))
            .collect();
        let schemas: Vec<LabeledSchema> = futures::stream::iter(opens)
            .buffered(SCHEMA_OPENS_IN_FLIGHT)
            .try_collect()
            .await?;

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

    /// Unknown for every column. The scan prunes per dataset from the footer
    /// instead, where the numbers are exact. See [`prune`](crate::prune).
    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    /// Plan every collection into every partition, then wrap the scan in the
    /// nd spine. Nothing opens here; see `deal_rotated`.
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
        let markers = atlas_markers(&listed);

        // A container is never split; its queue shares work among partitions.
        let file_groups = deal_rotated(&markers, state.config().target_partitions());
        tracing::debug!(
            collections = markers.len(),
            partitions = file_groups.len(),
            "atlas create_physical_plan",
        );

        // The source's schema is the nd-encoded form of the logical table schema.
        let encoded = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            conf.file_schema(),
        ));
        let table_schema = TableSchema::new(encoded, conf.table_partition_cols().clone());
        // Preserve a projection already pushed into the incoming source.
        let projection = conf.file_source().projection().cloned();

        let source = self
            .source(table_schema)
            .with_projection(projection)
            .with_type_widening(Arc::clone(&session_widening(state).strategy))
            .with_cancellation(query_cancellation(state));
        // Fail at plan time, so the user sees the error before the scan runs.
        source.require_projection()?;
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_source(Arc::new(source))
            .build();

        nd_scan_plan(conf)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(self.source(table_schema))
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
mod scan_tests {
    use super::*;
    use crate::test_support;
    use arrow::array::AsArray;
    use arrow::datatypes::Int64Type;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::Path;

    /// A table over every collection under `dir`, in a session of
    /// `partitions` target partitions.
    async fn table(dir: &Path, partitions: usize) -> (SessionContext, Arc<ListingTable>) {
        table_with(dir, partitions, AtlasOptions::default()).await
    }

    /// [`table`], on a format with `options`.
    async fn table_with(
        dir: &Path,
        partitions: usize,
        options: AtlasOptions,
    ) -> (SessionContext, Arc<ListingTable>) {
        // The collections sit in subdirectories, which a listing skips by default.
        let config = SessionConfig::new()
            .with_target_partitions(partitions)
            .set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            );
        let ctx = SessionContext::new_with_config(config);
        let url = ListingTableUrl::parse(format!("{}/", dir.display())).unwrap();
        let options = ListingOptions::new(Arc::new(AtlasFormat::new(options)))
            .with_file_extension(ATLAS_MARKER)
            .with_collect_stat(false);
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        (ctx, Arc::new(ListingTable::try_new(config).unwrap()))
    }

    /// Three collections of five datasets, four rows each, under `dir`.
    async fn three_collections(dir: &Path) {
        for name in ["a", "b", "c"] {
            let dir = dir.join(name);
            std::fs::create_dir_all(&dir).unwrap();
            test_support::ranged(&dir, 5).await;
        }
    }

    /// The rows of `temperature` over every collection under `dir`, read in a
    /// session of `partitions` target partitions, and the plan's partition count.
    async fn rows(dir: &Path, partitions: usize) -> (usize, usize) {
        let (ctx, table) = table(dir, partitions).await;
        let df = ctx
            .read_table(table)
            .unwrap()
            .select_columns(&["temperature"])
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let partition_count = plan.properties().output_partitioning().partition_count();
        let batches = df.collect().await.unwrap();
        (batches.iter().map(|b| b.num_rows()).sum(), partition_count)
    }

    /// Three collections, read by three partitions that each hold all of
    /// them. Every row comes out once, and no row comes out twice.
    #[tokio::test]
    async fn every_partition_reads_through_the_pool_and_no_dataset_twice() {
        let tmp = tempfile::tempdir().unwrap();
        three_collections(tmp.path()).await;

        let (alone, one) = rows(tmp.path(), 1).await;
        let (shared, three) = rows(tmp.path(), 3).await;

        assert_eq!(one, 1);
        assert_eq!(three, 3, "every partition holds a group");
        assert_eq!(
            alone,
            3 * 5 * 4,
            "five datasets of four rows per collection"
        );
        assert_eq!(
            shared, alone,
            "a dataset is read by one partition and no other"
        );
    }

    /// `COUNT(*)` projects no column, and a dataset has no row count without
    /// one. The query is refused before any collection opens, and the error
    /// says what to write instead.
    #[tokio::test]
    async fn a_count_of_no_column_is_refused_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        three_collections(tmp.path()).await;
        let (ctx, table) = table(tmp.path(), 3).await;
        ctx.register_table("obs", table).unwrap();

        let error = ctx
            .sql("SELECT count(*) FROM obs")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("no column, no row count")
            .to_string();

        assert!(error.contains("column list"), "{error}");
        assert!(error.contains("count(*)"), "{error}");
    }

    /// A count of a column projects that column, and counts every row of
    /// every dataset once across the partitions.
    #[tokio::test]
    async fn a_count_of_a_column_counts_every_row() {
        let tmp = tempfile::tempdir().unwrap();
        three_collections(tmp.path()).await;
        let (ctx, table) = table(tmp.path(), 3).await;
        ctx.register_table("obs", table).unwrap();

        let batches = ctx
            .sql("SELECT count(temperature) FROM obs")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let count = batches[0].column(0).as_primitive::<Int64Type>().value(0);
        assert_eq!(count, 60, "three collections of five datasets of four rows");
    }

    /// `SELECT *` fails when the query is planned, not when it runs, with the
    /// message that says what to do. A column list reads.
    #[tokio::test]
    async fn select_star_fails_at_plan_time() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_grids(tmp.path()).await;
        let (ctx, table) = table(tmp.path(), 1).await;
        ctx.register_table("mixed", table).unwrap();

        let error = ctx
            .sql("SELECT * FROM mixed")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .expect_err("SELECT * must not plan");
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "a plan error: {error}"
        );
        assert!(error.to_string().contains("column list"), "{error}");

        let batches = ctx
            .sql("SELECT temperature FROM mixed")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4, "one column names one grid");
    }

    /// Two named columns on two grids pass the column-list rule and fail at
    /// the open, where the grids are known. A dimension list chooses one.
    #[tokio::test]
    async fn two_grids_need_a_dimension_list() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_grids(tmp.path()).await;
        let (ctx, table) = table(tmp.path(), 1).await;
        ctx.register_table("mixed", table).unwrap();

        let error = ctx
            .sql("SELECT temperature, grid FROM mixed")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("two grids, no list")
            .to_string();
        assert!(error.contains("more than one grid"), "{error}");

        let options = AtlasOptions {
            read_dimensions: Some(vec!["lat".to_string(), "lon".to_string()]),
        };
        let (ctx, table) = table_with(tmp.path(), 1, options).await;
        ctx.register_table("mixed", table).unwrap();
        let batches = ctx
            .sql("SELECT temperature, grid FROM mixed")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6, "the lat by lon grid; temperature reads null");
    }

    /// Every format a factory builds opens through the factory's one cache,
    /// so a collection one query opened is a hit for the next.
    #[tokio::test]
    async fn the_factory_shares_one_cache_between_its_formats() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (store, marker) = test_support::store_and_marker(tmp.path());
        let factory = AtlasFormatFactory::new(AtlasOptions::default());
        let ctx = SessionContext::new();

        let created = factory.create(&ctx.state(), &HashMap::new()).unwrap();
        let default = factory.default();
        let created = created.as_any().downcast_ref::<AtlasFormat>().unwrap();
        let default = default.as_any().downcast_ref::<AtlasFormat>().unwrap();
        let first = get_or_open_atlas(Some(&created.cache), Arc::clone(&store), &marker)
            .await
            .unwrap();
        let second = get_or_open_atlas(Some(&default.cache), store, &marker)
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second), "the second open must hit");
    }
}
