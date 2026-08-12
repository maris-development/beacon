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
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        // Per-table override from `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
        let read_dimensions = format_options.get("read_dimensions").map(|value| {
            value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });
        Ok(Arc::new(
            TiffFormat::new(self.options.clone()).with_read_dimensions(read_dimensions),
        ))
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

#[derive(Debug, Clone, Default)]
pub struct TiffFormat {
    pub options: TiffOptions,
    /// Explicit dimensions requested via `read_tiff(paths, ['dims'])` or a
    /// `CREATE EXTERNAL TABLE ... OPTIONS (read_dimensions '...')`. When set,
    /// only variables whose dimensions are a subset of these are read; when
    /// `None`, a broadcast-compatible default is auto-selected.
    pub read_dimensions: Option<Vec<String>>,
}

impl TiffFormat {
    pub fn new(options: TiffOptions) -> Self {
        Self {
            options,
            read_dimensions: None,
        }
    }

    /// Returns a copy of this format that reads only the variables belonging to
    /// `read_dimensions` (or auto-selects a default when `None`).
    pub fn with_read_dimensions(mut self, read_dimensions: Option<Vec<String>>) -> Self {
        self.read_dimensions = read_dimensions;
        self
    }
}

/// Wrap a TIFF file scan in the nd spine: `NdBroadcastExec` → `NdSourceExec` →
/// `DataSourceExec`.
///
/// The scan carries nd data as `beacon.nd`-encoded struct columns, so
/// `NdSourceExec` decodes it and `NdBroadcastExec` broadcasts it back to the
/// logical table schema above the scan.
fn nd_scan_plan(conf: FileScanConfig) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let data_source: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
    let nd_source = Arc::new(beacon_datafusion_ext::nd::exec::NdSourceExec::try_new(
        data_source,
    )?);
    Ok(Arc::new(
        beacon_datafusion_ext::nd::exec::NdBroadcastExec::try_new(nd_source)?,
    ))
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
            .map(|object| {
                reader::fetch_schema(store.clone(), object.clone(), self.read_dimensions.clone())
            })
            .collect();
        let schemas: Vec<SchemaRef> = futures::stream::iter(tasks)
            .buffered(width)
            .try_collect()
            .await?;
        if schemas.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        let schema = super_type_schema(&schemas).map_err(|e| {
            exec_datafusion_err!(
                "Failed to compute super type schema for TIFF datasets: {}",
                e
            )
        })?;
        Ok(schema.into())
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
        // The scan carries nd data as `beacon.nd`-encoded struct columns, so the
        // file source's schema is the encoded form of the logical table schema.
        let encoded_file_schema = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            conf.file_schema(),
        ));
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            encoded_file_schema,
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = TiffSource::new(table_schema)
            .with_read_dimensions(self.read_dimensions.clone())
            .with_projection(projection);

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();

        nd_scan_plan(conf)
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(TiffSource::new(table_schema).with_read_dimensions(self.read_dimensions.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    const TEST_TIF_BYTES: &[u8] = include_bytes!("../../test-files/test.tif");

    /// The bundled `test.tif` is 1287 × 380, single band, float32.
    const WIDTH: usize = 1287;
    const HEIGHT: usize = 380;

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

    /// Register the bundled `test.tif` as a DataFusion table backed by
    /// [`TiffFormat`] + `ListingTable` over the local filesystem.
    async fn register_example_with(ctx: &SessionContext, format: TiffFormat) {
        use datafusion::datasource::file_format::FileFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        let file = concat!(env!("CARGO_MANIFEST_DIR"), "/test-files/test.tif");
        let table_path = ListingTableUrl::parse(format!("file://{file}")).unwrap();
        let format: Arc<dyn FileFormat> = Arc::new(format);
        let listing_options = ListingOptions::new(format).with_file_extension("tif");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        let table = ListingTable::try_new(config).unwrap();
        ctx.register_table("tiff_t", Arc::new(table)).unwrap();
    }

    async fn register_example(ctx: &SessionContext) {
        register_example_with(ctx, TiffFormat::new(Default::default())).await;
    }

    /// A session with the nd pushdown rules registered — the same wiring
    /// beacon-core installs. Single partition so row order is deterministic
    /// (the differential tests compare results positionally).
    fn ctx_with_pushdown() -> SessionContext {
        use datafusion::execution::session_state::SessionStateBuilder;

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(1))
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(
                beacon_datafusion_ext::nd::NdProjectionPushdown::new(),
            ))
            .with_physical_optimizer_rule(Arc::new(
                beacon_datafusion_ext::nd::NdFilterPushdown::new(),
            ))
            .build();
        SessionContext::new_with_state(state)
    }

    #[tokio::test]
    async fn infer_schema_reads_real_stripped_geotiff_fixture() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/test.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let schema = reader::fetch_schema(object_store, object, None)
            .await
            .expect("real stripped GeoTIFF should produce a schema");

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        for expected in ["band.0", "geo.lat", "geo.lon", "image.width"] {
            assert!(
                field_names.contains(&expected),
                "schema should contain {expected}: {field_names:?}"
            );
        }
    }

    /// Explicit `read_dimensions` narrows the schema to the variables living on
    /// the requested axis: `geo.lat` is on `y`, `geo.lon` on `x`, and the band on
    /// both. Scalar metadata (rank-0) survives every narrowing.
    #[tokio::test]
    async fn read_dimensions_narrows_the_schema_to_one_axis() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/test_dims.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let schema = reader::fetch_schema(object_store, object, Some(vec!["y".to_string()]))
            .await
            .expect("narrowed schema");

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"geo.lat"), "geo.lat is on y: {names:?}");
        assert!(names.contains(&"image.width"), "scalars survive: {names:?}");
        assert!(!names.contains(&"geo.lon"), "geo.lon is on x: {names:?}");
        assert!(!names.contains(&"band.0"), "the band is on y,x: {names:?}");
    }

    /// Reading on the `y` axis alone makes the table one row per image row,
    /// instead of the full `y × x` grid `count_star_counts_the_full_grid` sees.
    #[tokio::test]
    async fn read_dimensions_narrows_the_row_count_to_one_axis() {
        use arrow::array::Int64Array;

        let ctx = SessionContext::new();
        register_example_with(
            &ctx,
            TiffFormat::new(Default::default()).with_read_dimensions(Some(vec!["y".to_string()])),
        )
        .await;

        let batches = ctx
            .sql("SELECT COUNT(*) AS n FROM tiff_t")
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
        assert_eq!(
            n as usize, HEIGHT,
            "the y axis alone has one row per image row"
        );
    }

    // ── nd pipeline: plan shape ──────────────────────────────────────────

    /// The physical plan is the nd spine over the standard file scan:
    /// `NdBroadcastExec` → `NdSourceExec` → `DataSourceExec`, in that nesting
    /// order (parent above child in the indented render).
    #[tokio::test]
    async fn physical_plan_is_nd_spine_over_scan() {
        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let plan = ctx
            .sql("SELECT \"band.0\" FROM tiff_t")
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
            "plan must contain the nd spine over a DataSourceExec:\n{rendered}"
        );
        assert!(
            broadcast < source && source < scan,
            "expected NdBroadcastExec → NdSourceExec → DataSourceExec nesting:\n{rendered}"
        );
    }

    /// With the projection rule registered, `SELECT "geo.lat" * 2` plans with an
    /// `NdProjectionExec` *below* the `NdBroadcastExec` — so the arithmetic runs
    /// on the 380-element latitude axis, not on all 380 × 1287 grid cells — and
    /// produces the same values as a session without the rule.
    #[tokio::test]
    async fn projection_pushdown_fires_end_to_end() {
        use arrow::compute::concat_batches;

        let sql = "SELECT \"geo.lat\" * 2 AS lat2 FROM tiff_t";

        let on = ctx_with_pushdown();
        register_example(&on).await;
        let df = on.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();

        let broadcast = rendered.find("NdBroadcastExec");
        let projection = rendered.find("NdProjectionExec");
        let source = rendered.find("NdSourceExec");
        assert!(
            projection.is_some() && broadcast < projection && projection < source,
            "expected NdBroadcastExec → NdProjectionExec → NdSourceExec:\n{rendered}"
        );
        let actual = df.collect().await.unwrap();

        // Same single-partition config so row order matches positionally.
        let off = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        register_example(&off).await;
        let expected = off.sql(sql).await.unwrap().collect().await.unwrap();

        let schema = expected[0].schema();
        assert_eq!(
            concat_batches(&schema, &actual).unwrap(),
            concat_batches(&schema, &expected).unwrap(),
        );
    }

    /// With the filter rule registered, `WHERE "geo.lat" > 40` sinks into an
    /// `NdFilterExec` below the broadcast — the grid is selected before it is
    /// materialized — and the rows match the unoptimized session.
    #[tokio::test]
    async fn filter_pushdown_fires_end_to_end() {
        use arrow::compute::concat_batches;

        let sql = "SELECT \"geo.lat\" FROM tiff_t WHERE \"geo.lat\" > 40";

        let on = ctx_with_pushdown();
        register_example(&on).await;
        let df = on.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();

        let broadcast = rendered.find("NdBroadcastExec");
        let filter = rendered.find("NdFilterExec");
        let source = rendered.find("NdSourceExec");
        assert!(
            filter.is_some() && broadcast < filter && filter < source,
            "expected NdBroadcastExec → NdFilterExec → NdSourceExec:\n{rendered}"
        );
        let actual = df.collect().await.unwrap();

        let off = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        register_example(&off).await;
        let expected = off.sql(sql).await.unwrap().collect().await.unwrap();

        let schema = expected[0].schema();
        assert_eq!(
            concat_batches(&schema, &actual).unwrap(),
            concat_batches(&schema, &expected).unwrap(),
        );
    }

    // ── end-to-end reads ─────────────────────────────────────────────────

    /// The two coordinate axes — `geo.lat` on `y`, `geo.lon` on `x` — broadcast
    /// against each other into their full cross product, with the values the
    /// GeoTIFF's ModelTransformation tag defines.
    ///
    /// Assertions are order-independent on purpose: the nd spine derives the
    /// grid's axis order from the widest projected column, so two same-rank
    /// coordinates leave the row order unspecified (as for every nd format).
    #[tokio::test]
    async fn end_to_end_reads_broadcast_coordinates() {
        use arrow::array::{Float64Array, Int64Array};

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let batches = ctx
            .sql(
                r#"SELECT COUNT(*)                    AS rows,
                          COUNT(DISTINCT "geo.lat")   AS lats,
                          COUNT(DISTINCT "geo.lon")   AS lons,
                          MIN("geo.lat")              AS lat_min,
                          MAX("geo.lat")              AS lat_max,
                          MIN("geo.lon")              AS lon_min
                   FROM tiff_t"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let row = &batches[0];
        let int = |name: &str| {
            row.column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        let float = |name: &str| {
            row.column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
        };

        // Each axis contributes its own extent, and the table is their product.
        assert_eq!(int("lats") as usize, HEIGHT);
        assert_eq!(int("lons") as usize, WIDTH);
        assert_eq!(int("rows") as usize, HEIGHT * WIDTH);

        // ModelTransformationTag: lat[y] = 0.04166667002172143 * y + 30.16666666498914
        //                         lon[x] = 0.0416666671610546  * x + -17.312499364464315
        assert!((float("lat_min") - 30.166_666_664_989_14).abs() < 1e-6);
        assert!((float("lat_max") - 45.958_334_603_221_566).abs() < 1e-6);
        assert!((float("lon_min") - -17.312_499_364_464_315).abs() < 1e-6);
    }

    /// A rank-0 metadata scalar (`image.width`) rides the nd encoding and
    /// broadcasts to a constant column over every row of the grid its
    /// co-selected variable establishes — here `band.0`, the full `y × x` band.
    #[tokio::test]
    async fn end_to_end_broadcasts_scalar_metadata() {
        use arrow::array::Int64Array;

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let batches = ctx
            .sql(
                r#"SELECT COUNT(DISTINCT "image.width") AS distinct_widths,
                          COUNT("image.width")          AS scalar_rows,
                          COUNT("geo.lat")              AS coord_rows,
                          COUNT("band.0")               AS band_values
                   FROM tiff_t"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let int = |name: &str| {
            batches[0]
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(int("distinct_widths"), 1, "a scalar is a single constant");
        // `band.0` is the widest column, so the grid is the full image.
        assert_eq!(
            int("scalar_rows") as usize,
            HEIGHT * WIDTH,
            "the scalar must be broadcast onto every grid row"
        );
        assert_eq!(
            int("coord_rows") as usize,
            HEIGHT * WIDTH,
            "the latitude axis must be broadcast onto every grid row"
        );
        // The band's nodata pixels come back as nulls, so it counts fewer.
        assert!(int("band_values") > 0);
        assert!(
            int("band_values") < int("scalar_rows"),
            "the fixture's nodata pixels must be null"
        );
    }

    /// `COUNT(*)` projects no columns, so the opener drives the read with the
    /// highest-volume variable and reports the full broadcast row count.
    #[tokio::test]
    async fn count_star_counts_the_full_grid() {
        use arrow::array::Int64Array;

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let batches = ctx
            .sql("SELECT COUNT(*) AS n FROM tiff_t")
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
        assert_eq!(n as usize, HEIGHT * WIDTH);
    }

    #[tokio::test]
    async fn projection_pushdown_through_datafusion() {
        let ctx = SessionContext::new();
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
        assert_eq!(rows, HEIGHT * WIDTH);
    }

    #[tokio::test]
    async fn predicate_prunes_every_row_through_datafusion() {
        let ctx = SessionContext::new();
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
    async fn predicate_selects_subset_through_datafusion() {
        let ctx = SessionContext::new();
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
        assert!(total < HEIGHT * WIDTH, "predicate should drop some rows");
    }
}

pub mod table_function;
pub use table_function::ReadTiffFunc;
