use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::type_widening::session_widening;
use datafusion::{
    catalog::{memory::DataSourceExec, Session},
    common::{exec_datafusion_err, GetExt, Statistics},
    datasource::{
        file_format::{file_compression_type::FileCompressionType, FileFormat, FileFormatFactory},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource},
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::{options::TiffOptions, source::TiffSource};

const TIFF_EXTENSION: &str = "tiff";
const TIF_EXTENSION: &str = "tif";

pub mod options;
pub mod reader;
pub mod source;

#[derive(Debug, Clone)]
pub struct TiffFormatFactory {
    pub options: TiffOptions,
}

impl TiffFormatFactory {
    pub fn new(options: TiffOptions) -> Self {
        Self { options }
    }
}

impl FileFormatFactory for TiffFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(TiffFormat::new(self.options.clone())))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(TiffFormat::new(self.options.clone()))
    }

    fn as_any(&self) -> &dyn Any {
        self
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
        format.as_any().downcast_ref::<TiffFormat>()?;
        Some(SchemaOptions::new("tiff").finish())
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
}

impl TiffFormat {
    pub fn new(options: TiffOptions) -> Self {
        Self { options }
    }
}

#[async_trait::async_trait]
impl FileFormat for TiffFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        let tasks: Vec<_> = objects
            .iter()
            .map(|object| reader::fetch_schema(store.clone(), object.clone()))
            .collect();
        let schemas: Vec<SchemaRef> = futures::stream::iter(tasks)
            .buffered(width)
            .try_collect()
            .await?;
        if schemas.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        // The rule of the session decides the result for a column that two
        // files describe differently.
        session_widening(state)
            .merge_schemas(&schemas)
            .map_err(|e| {
                exec_datafusion_err!(
                    "Failed to merge the schemas of the TIFF datasets: {}",
                    e
                )
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
        // The scan carries nd data as `beacon.nd`-encoded struct columns, so
        // the file source's schema is the encoded form of the logical table
        // schema. `NdSourceExec` decodes it and `NdBroadcastExec` broadcasts it
        // back to the logical schema above the scan.
        //
        // The `PARTITIONED BY` columns are encoded with it. Their values come
        // from a file's path rather than its contents, and the reader appends
        // them per file, but they reach the plan the same way every other column
        // does — so that one decoder reads the whole batch.
        let encoded_file_schema = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            conf.file_schema(),
        ));
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            encoded_file_schema,
            beacon_nd_array::arrow::partition::encoded_partition_cols(conf.table_partition_cols()),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = TiffSource::new(table_schema).with_projection(projection);

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();

        let data_source: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
        let nd_source = Arc::new(beacon_datafusion_ext::nd::exec::NdSourceExec::try_new(
            data_source,
        )?);
        let broadcast = beacon_datafusion_ext::nd::exec::NdBroadcastExec::try_new(nd_source)?;
        Ok(Arc::new(broadcast))
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(TiffSource::new(table_schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, FileSource};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStoreExt;

    const TEST_TIF_BYTES: &[u8] = include_bytes!("../../test-files/test.tif");

    /// Drain an opener's stream and decode it.
    ///
    /// The opener emits `beacon.nd`-encoded batches for the `NdSourceExec`
    /// above it, so a test that reads the opener directly does that node's job
    /// itself: decode each batch, then broadcast it to the flat rows a query
    /// would see.
    async fn decoded<S>(stream: S) -> Vec<arrow::record_batch::RecordBatch>
    where
        S: futures::Stream<Item = datafusion::error::Result<arrow::record_batch::RecordBatch>>,
    {
        let encoded: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("all batches should be ok");

        encoded
            .iter()
            .map(|batch| {
                beacon_datafusion_ext::nd::decode_nd_record_batch(batch)
                    .expect("the opener emits nd batches")
                    .materialize()
                    .expect("they broadcast")
            })
            .collect()
    }

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

    #[tokio::test]
    async fn infer_schema_reads_real_stripped_geotiff_fixture() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/test.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let schema = reader::fetch_schema(object_store, object)
            .await
            .expect("real stripped GeoTIFF should produce a schema");

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"band.0"),
            "schema should contain band.0"
        );
        assert!(
            field_names.contains(&"geo.lat"),
            "schema should contain geo.lat"
        );
        assert!(
            field_names.contains(&"geo.lon"),
            "schema should contain geo.lon"
        );
        assert!(
            field_names.contains(&"image.width"),
            "schema should contain image.width"
        );
        println!("Schema is: {:?}", schema);
    }

    #[tokio::test]
    async fn opener_streams_record_batches_for_real_fixture() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/test2.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let table_schema = reader::fetch_schema(object_store.clone(), object.clone())
            .await
            .expect("schema");

        // The opener emits nd-encoded batches, so its source carries the encoded
        // schema — the same one `create_physical_plan` builds.
        let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
            beacon_datafusion_ext::nd::encoded_schema(&table_schema),
        ));
        let source = source::TiffSource::new(ts);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("memory://").expect("url"),
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            )
            .build();
            source
                .create_file_opener(object_store, &conf, 0)
                .expect("file opener")
        };

        let stream = file_opener
            .open(datafusion::datasource::listing::PartitionedFile::from(
                object,
            ))
            .expect("open")
            .await
            .expect("stream");

        let batches = decoded(stream).await;
        assert!(!batches.is_empty(), "should produce at least one batch");

        // Concatenate into a single batch for easy column access.
        let full = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");

        let schema = full.schema();

        // geo.lat column — values span ~30°N to ~46°N
        let lat_idx = schema.index_of("geo.lat").expect("geo.lat column");
        let lat_col = full
            .column(lat_idx)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("geo.lat should be Float64");
        assert!(lat_col.len() > 0);
        // First value: lat[0] = 0.04166667002172143 * 0 + 30.16666666498914
        assert!(
            (lat_col.value(0) - 30.166_666_664_989_14).abs() < 1e-6,
            "lat[0]={}",
            lat_col.value(0)
        );
        // All values should be within the expected geographic range.
        for i in 0..lat_col.len() {
            let v = lat_col.value(i);
            assert!(v >= 30.0 && v <= 47.0, "lat[{i}]={v} out of range");
        }

        // geo.lon column — values span ~-17°E to ~36°E
        let lon_idx = schema.index_of("geo.lon").expect("geo.lon column");
        let lon_col = full
            .column(lon_idx)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("geo.lon should be Float64");
        assert!(lon_col.len() > 0);
        // First value: lon[0] = 0.0416666671610546 * 0 + -17.312499364464315
        assert!(
            (lon_col.value(0) - -17.312_499_364_464_315).abs() < 1e-6,
            "lon[0]={}",
            lon_col.value(0)
        );
        // All values should be within the expected geographic range.
        for i in 0..lon_col.len() {
            let v = lon_col.value(i);
            assert!(v >= -18.0 && v <= 37.0, "lon[{i}]={v} out of range");
        }
    }

    /// A partitioned table carries the value of a raster's path on every row
    /// that raster contributes.
    ///
    /// The value is in the *path*, and a TIFF scan reads a whole collection
    /// behind one plan entry, so `FileStream` cannot append it: it does not know
    /// which raster a batch came from. The reader appends it itself instead —
    /// per file, which is per morsel.
    #[tokio::test]
    async fn a_partitioned_raster_carries_the_value_of_its_path() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("year=2024/test.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let file_schema = reader::fetch_schema(object_store.clone(), object.clone())
            .await
            .expect("schema");

        // What `create_physical_plan` builds for a partitioned table: the file
        // schema encoded, and the partition column encoded beside it.
        let year: arrow::datatypes::FieldRef = Arc::new(arrow::datatypes::Field::new(
            "year",
            arrow::datatypes::DataType::Utf8,
            false,
        ));
        let ts = datafusion::datasource::table_schema::TableSchema::new(
            Arc::new(beacon_datafusion_ext::nd::encoded_schema(&file_schema)),
            beacon_nd_array::arrow::partition::encoded_partition_cols(&[year]),
        );

        let source = source::TiffSource::new(ts);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("memory://").expect("url"),
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            )
            .build();
            source
                .create_file_opener(object_store, &conf, 0)
                .expect("file opener")
        };

        let mut file = datafusion::datasource::listing::PartitionedFile::from(object);
        file.partition_values = vec![datafusion::scalar::ScalarValue::Utf8(Some(
            "2024".to_string(),
        ))];

        let stream = file_opener.open(file).expect("open").await.expect("stream");
        let batches = decoded(stream).await;
        let full = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");

        assert!(full.num_rows() > 0, "the raster is read");
        let years = full
            .column(full.schema().index_of("year").expect("the partition column"))
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("year is Utf8");
        assert!(
            (0..full.num_rows()).all(|row| years.value(row) == "2024"),
            "every row of the raster carries the value of its directory"
        );
    }

    #[tokio::test]
    async fn opener_with_predicate_filters_rows() {
        use datafusion::config::ConfigOptions;
        use datafusion::datasource::physical_plan::FileSource;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;

        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/test_pred.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let table_schema = reader::fetch_schema(object_store.clone(), object.clone())
            .await
            .expect("schema");

        // Build predicate: geo.lat > 40.0
        // The Column index must match geo.lat's position in the file schema.
        let lat_idx = table_schema.index_of("geo.lat").expect("geo.lat field");
        let predicate: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("geo.lat", lat_idx)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Float64(Some(40.0)))),
            ));

        // Push the predicate into a TiffSource via try_pushdown_filters.
        let source_with_predicate: Arc<dyn FileSource> = {
            // The opener emits nd-encoded batches, so its source carries the
            // encoded schema — the same one `create_physical_plan` builds.
            let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
                beacon_datafusion_ext::nd::encoded_schema(&table_schema),
            ));
            let base_source = source::TiffSource::new(ts);
            let pushdown = base_source
                .try_pushdown_filters(vec![predicate], &ConfigOptions::default())
                .expect("try_pushdown_filters");
            pushdown.updated_node.expect("updated node with predicate")
        };

        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("memory://").expect("url"),
                source_with_predicate.clone(),
            )
            .build();
            source_with_predicate
                .create_file_opener(object_store, &conf, 0)
                .expect("file opener")
        };

        let stream = file_opener
            .open(datafusion::datasource::listing::PartitionedFile::from(
                object,
            ))
            .expect("open")
            .await
            .expect("stream");

        let batches = decoded(stream).await;
        assert!(!batches.is_empty(), "should produce at least one batch");

        let full = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");

        // Predicate pushdown here is coarse-grained: entire chunks whose coordinate range
        // falls entirely outside the predicate are skipped (no I/O). Chunks that partially
        // overlap are emitted in full. We therefore only verify that I/O was reduced, not
        // that every row satisfies the predicate.
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
        use datafusion::datasource::file_format::FileFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        let file = concat!(env!("CARGO_MANIFEST_DIR"), "/test-files/test.tif");
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
