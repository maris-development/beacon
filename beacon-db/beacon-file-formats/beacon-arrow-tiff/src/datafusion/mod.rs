use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::nd::{encoded_schema, exec::nd_scan_plan};
use beacon_datafusion_ext::type_widening::{label_by_object, session_widening};
use beacon_nd_array::{
    arrow::morsel::reject_partition_columns,
    dataset_cache::DatasetCache,
    nd_file_opener::{CachedNdFileOpener, NdFileOpener},
    nd_morsel_file_source::NdMorselFileSource,
};
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource},
        table_schema::TableSchema,
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::{opener::TiffOpener, options::TiffOptions};

/// Extension the TIFF format factory is registered under.
pub const TIFF_EXTENSION: &str = "tiff";
const TIF_EXTENSION: &str = "tif";

/// The number of opened rasters that a factory keeps in its cache.
const DATASET_CACHE_SIZE: usize = 32;

pub mod opener;
pub mod options;

#[derive(Debug, Clone)]
pub struct TiffFormatFactory {
    pub options: TiffOptions,
    /// The opened rasters, shared by every format this factory builds.
    pub dataset_cache: DatasetCache,
}

impl TiffFormatFactory {
    pub fn new(options: TiffOptions) -> Self {
        Self {
            options,
            dataset_cache: DatasetCache::new(DATASET_CACHE_SIZE),
        }
    }

    /// A format with the options and the dataset cache of this factory.
    fn format(&self) -> TiffFormat {
        TiffFormat::new(self.options.clone()).with_dataset_cache(self.dataset_cache.clone())
    }
}

impl FileFormatFactory for TiffFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(self.format()))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.format())
    }
}

impl GetExt for TiffFormatFactory {
    fn get_ext(&self) -> String {
        TIFF_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for TiffFormatFactory {
    /// TIFF opts into the schema cache on its name alone. `TiffOptions` carries
    /// nothing today, so there is nothing else to tell two reads apart.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        format.downcast_ref::<TiffFormat>()?;
        // The `v2` suffix drops the schemas that the AnyDataset reader cached.
        Some(SchemaOptions::new("tiff-v2").finish())
    }

    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == TIFF_EXTENSION || ext == TIF_EXTENSION)
                    .unwrap_or(false)
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    fn file_extensions(&self) -> Vec<String> {
        vec![TIFF_EXTENSION.to_string(), TIF_EXTENSION.to_string()]
    }
}

#[derive(Debug, Clone)]
pub struct TiffFormat {
    pub options: TiffOptions,
    /// The cache of opened rasters. Without it, every read opens the file.
    dataset_cache: Option<DatasetCache>,
}

impl TiffFormat {
    pub fn new(options: TiffOptions) -> Self {
        Self {
            options,
            dataset_cache: None,
        }
    }

    /// Keep the opened rasters in `cache`, so that schema inference and scans share them.
    pub fn with_dataset_cache(mut self, cache: DatasetCache) -> Self {
        self.dataset_cache = Some(cache);
        self
    }

    /// The opener of this format, behind the dataset cache when the format has one.
    fn opener(&self) -> Arc<dyn NdFileOpener> {
        match &self.dataset_cache {
            Some(cache) => Arc::new(CachedNdFileOpener::new(Arc::new(TiffOpener), cache.clone())),
            None => Arc::new(TiffOpener),
        }
    }

    /// The morsel source that reads the rasters of this format.
    ///
    /// # Arguments
    ///
    /// * `table_schema` - The nd-encoded table schema, as `NdSourceExec` above the scan expects
    pub fn morsel_source(&self, table_schema: TableSchema) -> NdMorselFileSource {
        NdMorselFileSource::new(self.opener(), TIFF_EXTENSION.to_string(), table_schema)
    }
}

#[async_trait::async_trait]
impl FileFormat for TiffFormat {
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        TIFF_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(TIFF_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        use futures::{StreamExt, TryStreamExt};

        // Bounded: each open holds a descriptor until its schema is read, and
        // `try_join_all` would open every file in the listing at once. See the
        // same fix in `beacon_arrow_netcdf`, and issue #361.
        let width = state
            .config_options()
            .execution
            .meta_fetch_concurrency
            .max(1);
        // The scan opens the same files, so the inference fills the dataset cache for it.
        let opener = self.opener();
        let tasks: Vec<_> = objects
            .iter()
            .map(|object| {
                let location = object.location.clone();
                let dataset = opener.open(Arc::clone(store), PartitionedFile::from(object.clone()));
                async move {
                    let dataset = dataset.await.map_err(|e| {
                        exec_datafusion_err!(
                            "Failed to open TIFF dataset {location} for schema inference: {e}"
                        )
                    })?;
                    Ok::<SchemaRef, datafusion::error::DataFusionError>(Arc::new(
                        dataset.arrow_schema(),
                    ))
                }
            })
            .collect();
        let schemas: Vec<SchemaRef> = futures::stream::iter(tasks)
            .buffered(width)
            .try_collect()
            .await?;
        if schemas.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        // The rule of the session decides the result for a column that two
        // files describe differently. Each schema names its file, so a refused
        // column names both files.
        session_widening(state)
            .merge_schemas(&label_by_object(objects, &schemas))
            .map_err(|e| {
                exec_datafusion_err!("Failed to merge the schemas of the TIFF datasets: {}", e)
            })
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
        // Partitioned tables are not supported yet.
        reject_partition_columns("TIFF", &conf)?;

        // The scan emits nd-encoded columns. The nodes above it decode and broadcast them.
        let table_schema =
            TableSchema::from_file_schema(Arc::new(encoded_schema(conf.file_schema())));
        // The incoming source holds the projection that the table scan pushed down.
        let projection = conf.file_source().projection().cloned();
        let source = self
            .morsel_source(table_schema)
            .with_projection(projection)?;
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();

        nd_scan_plan(conf)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(self.morsel_source(table_schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::PhysicalExpr;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    const TEST_TIF_BYTES: &[u8] = include_bytes!("../../test-files/test.tif");

    async fn put_fixture(store: &Arc<InMemory>, path: &Path, bytes: &[u8]) -> ObjectMeta {
        store
            .put(path, bytes::Bytes::copy_from_slice(bytes).into())
            .await
            .expect("should write TIFF fixture bytes");
        store
            .head(path)
            .await
            .expect("should fetch object metadata")
    }

    async fn fetch_schema(store: Arc<dyn ObjectStore>, object: &ObjectMeta) -> SchemaRef {
        let dataset = crate::reader::open_dataset(store, object.location.clone())
            .await
            .expect("the fixture opens");
        Arc::new(dataset.arrow_schema())
    }

    /// The scan of `object` that `create_physical_plan` builds, on `source`.
    fn scan_config(source: NdMorselFileSource, object: &ObjectMeta) -> FileScanConfig {
        FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            Arc::new(source),
        )
        .with_file(PartitionedFile::from(object.clone()))
        .build()
    }

    /// Run the nd plan over `object` in `store`, and join its rows into one batch.
    async fn scan(
        store: Arc<InMemory>,
        object: &ObjectMeta,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> arrow::record_batch::RecordBatch {
        let schema = fetch_schema(store.clone(), object).await;
        let table_schema = TableSchema::from_file_schema(Arc::new(encoded_schema(&schema)));
        let mut source = TiffFormat::new(TiffOptions::default())
            .morsel_source(table_schema)
            .with_pruning_chunk_threshold(0);
        if let Some(predicate) = predicate {
            source = source.with_pushdown_predicate(predicate);
        }
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_object_store(ObjectStoreUrl::parse("memory://").unwrap().as_ref(), store);
        let plan = nd_scan_plan(scan_config(source, object)).unwrap();
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    #[tokio::test]
    async fn infer_schema_reads_real_stripped_geotiff_fixture() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("tests/datafusion/test.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let schema = fetch_schema(store, &object).await;

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        for name in ["band.0", "geo.lat", "geo.lon", "image.width"] {
            assert!(field_names.contains(&name), "schema should contain {name}");
        }
    }

    /// Schema inference leaves the raster in the factory cache, so the scan does not open it again.
    #[tokio::test]
    async fn schema_inference_fills_the_factory_dataset_cache() {
        let store = Arc::new(InMemory::new());
        let object = put_fixture(&store, &Path::from("cached.tif"), TEST_TIF_BYTES).await;
        let factory = TiffFormatFactory::new(TiffOptions::default());
        let object_store: Arc<dyn ObjectStore> = store;

        factory
            .default()
            .infer_schema(
                &datafusion::prelude::SessionContext::new().state(),
                &object_store,
                std::slice::from_ref(&object),
            )
            .await
            .unwrap();

        assert!(factory.dataset_cache.try_get(&object).is_some());
    }

    #[tokio::test]
    async fn the_scan_reads_pixel_center_coordinates() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("tests/datafusion/test2.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let full = scan(store, &object, None).await;
        let schema = full.schema();

        // geo.lat column — values span ~30°N to ~46°N
        let lat_col = full
            .column(schema.index_of("geo.lat").expect("geo.lat column"))
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("geo.lat should be Float64");
        assert!(!lat_col.is_empty());
        // The minimum is the pixel center of the first row: 0.04166667002172143 * 0.5 + 30.16666666498914
        let lat_min = arrow::compute::min(lat_col).unwrap();
        assert!((lat_min - 30.1875).abs() < 1e-6, "lat min={lat_min}");
        assert!(lat_col.values().iter().all(|v| (30.0..=47.0).contains(v)));

        // geo.lon column — values span ~-17°E to ~36°E
        let lon_col = full
            .column(schema.index_of("geo.lon").expect("geo.lon column"))
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("geo.lon should be Float64");
        // The minimum is the pixel center of the first column: 0.0416666671610546 * 0.5 + -17.312499364464315
        let lon_min = arrow::compute::min(lon_col).unwrap();
        assert!(
            (lon_min - -17.291_666_030_883_79).abs() < 1e-6,
            "lon min={lon_min}"
        );
        assert!(lon_col.values().iter().all(|v| (-18.0..=37.0).contains(v)));
    }

    /// A table with `PARTITIONED BY` columns is not supported yet.
    #[tokio::test]
    async fn a_partitioned_table_is_rejected() {
        let store = Arc::new(InMemory::new());
        let object = put_fixture(&store, &Path::from("year=2024/test.tif"), TEST_TIF_BYTES).await;
        let schema = fetch_schema(store, &object).await;
        let year: arrow::datatypes::FieldRef = Arc::new(arrow::datatypes::Field::new(
            "year",
            arrow::datatypes::DataType::Utf8,
            false,
        ));
        let format = TiffFormat::new(TiffOptions::default());
        let source = format.morsel_source(TableSchema::new(schema, vec![year]));

        let error = format
            .create_physical_plan(
                &datafusion::prelude::SessionContext::new().state(),
                scan_config(source, &object),
            )
            .await
            .expect_err("a partitioned table")
            .to_string();

        assert!(error.contains("TIFF"), "{error}");
    }

    #[tokio::test]
    async fn a_pushed_down_predicate_prunes_chunks() {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;

        let store = Arc::new(InMemory::new());
        let path = Path::from("tests/datafusion/test_pred.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;
        let schema = fetch_schema(store.clone(), &object).await;

        // geo.lat > 40.0, on the logical file schema.
        let lat_idx = schema.index_of("geo.lat").expect("geo.lat field");
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("geo.lat", lat_idx)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float64(Some(40.0)))),
        ));

        let full = scan(store, &object, Some(predicate)).await;

        // Pruning drops whole chunks. A chunk that partly holds wanted rows is read in full.
        let total_rows = 380 * 1287;
        assert!(
            full.num_rows() < total_rows,
            "predicate should skip at least one chunk, reducing row count below {total_rows} (got {})",
            full.num_rows()
        );
        assert!(full.num_rows() > 0, "predicate should keep some rows");
    }

    // ── End-to-end via SessionContext (projection + predicate pushdown) ──

    /// Register the bundled `test.tif` as a DataFusion table backed by
    /// [`TiffFormat`] + `ListingTable` over the local filesystem.
    async fn register_example(ctx: &datafusion::prelude::SessionContext) {
        register_fixture(ctx, "test.tif").await;
    }

    /// Register the fixture `test-files/<name>` as the table `tiff_t`.
    async fn register_fixture(ctx: &datafusion::prelude::SessionContext, name: &str) {
        use datafusion::datasource::file_format::FileFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        let file = format!("{}/test-files/{name}", env!("CARGO_MANIFEST_DIR"));
        let table_path = ListingTableUrl::parse(format!("file://{file}")).unwrap();
        let format: Arc<dyn FileFormat> = Arc::new(TiffFormat::new(Default::default()));
        let listing_options = ListingOptions::new(format).with_file_extension("tif");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        let table = ListingTable::try_new(config).unwrap();
        ctx.register_table("tiff_t", Arc::new(table)).unwrap();
    }

    /// Issue #523: a sparse tile must read as nodata and not fail the query.
    #[tokio::test]
    async fn sparse_tiles_count_through_datafusion() {
        let ctx = datafusion::prelude::SessionContext::new();
        register_fixture(&ctx, "sparse_tiled_i16.tif").await;

        let batches = ctx
            .sql("SELECT count(*), count(\"band.0\") FROM tiff_t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = |column: usize| {
            batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("count is Int64")
                .value(0)
        };
        // 64x64 pixels. Two 16x16 tiles hold data, and the sparse tiles read as NULL.
        assert_eq!((count(0), count(1)), (64 * 64, 2 * 16 * 16));
    }

    #[tokio::test]
    async fn projection_pushdown_through_datafusion() {
        let ctx = datafusion::prelude::SessionContext::new();
        register_example(&ctx).await;

        let df = ctx
            .sql("SELECT \"band.0\", \"geo.lat\" FROM tiff_t")
            .await
            .unwrap();

        // Only the two projected columns flow through the plan.
        let names: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["band.0".to_string(), "geo.lat".to_string()]);

        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows > 0);
    }

    #[tokio::test]
    async fn predicate_pushdown_prunes_through_datafusion() {
        let ctx = datafusion::prelude::SessionContext::new();
        register_example(&ctx).await;

        // Latitude never exceeds ~47°, so this predicate excludes every row.
        let rows: usize = ctx
            .sql("SELECT \"geo.lat\" FROM tiff_t WHERE \"geo.lat\" > 1000")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            rows, 0,
            "impossible latitude predicate should yield no rows"
        );
    }

    #[tokio::test]
    async fn predicate_pushdown_selects_subset_through_datafusion() {
        let ctx = datafusion::prelude::SessionContext::new();
        register_example(&ctx).await;

        let batches = ctx
            .sql("SELECT \"geo.lat\" FROM tiff_t WHERE \"geo.lat\" > 40")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut total = 0usize;
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("geo.lat is Float64");
            for i in 0..col.len() {
                assert!(
                    col.value(i) > 40.0,
                    "every returned lat must satisfy the predicate"
                );
            }
            total += b.num_rows();
        }
        assert!(total > 0, "satisfiable predicate should keep some rows");
        assert!(total < 380 * 1287, "predicate should drop some rows");
    }
}

pub mod table_function;
pub use table_function::ReadTiffFunc;
