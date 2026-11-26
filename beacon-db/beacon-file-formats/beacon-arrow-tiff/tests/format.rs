//! TIFF tables through DataFusion: projection, filters, counts, limits and partitions.

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use beacon_arrow_tiff::datafusion::TiffFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;

/// Register `test-files/<name>` as the table `tiff_t`.
async fn register(ctx: &SessionContext, name: &str) {
    let file = format!("{}/test-files/{name}", env!("CARGO_MANIFEST_DIR"));
    register_path(ctx, &file, |options| options).await;
}

async fn register_path(
    ctx: &SessionContext,
    path: &str,
    options: impl FnOnce(ListingOptions) -> ListingOptions,
) {
    let url = ListingTableUrl::parse(format!("file://{path}")).unwrap();
    let format: Arc<dyn FileFormat> = Arc::new(TiffFormat::new(Default::default()));
    let config = ListingTableConfig::new(url)
        .with_listing_options(options(
            ListingOptions::new(format).with_file_extension("tif"),
        ))
        .infer_schema(&ctx.state())
        .await
        .unwrap();
    let table = ListingTable::try_new(config).unwrap();
    ctx.register_table("tiff_t", Arc::new(table)).unwrap();
}

async fn sql(ctx: &SessionContext, query: &str) -> Vec<RecordBatch> {
    ctx.sql(query).await.unwrap().collect().await.unwrap()
}

/// The Int64 at row 0 of `column` of the first batch.
fn int(batches: &[RecordBatch], column: usize) -> i64 {
    batches[0]
        .column(column)
        .as_primitive::<Int64Type>()
        .value(0)
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Issue #523: a sparse tile reads as nodata and does not fail the query.
#[tokio::test]
async fn sparse_tiles_count_through_datafusion() {
    let ctx = SessionContext::new();
    register(&ctx, "sparse_tiled_i16.tif").await;
    let batches = sql(&ctx, "SELECT count(*), count(\"band.0\") FROM tiff_t").await;
    // 64x64 pixels. Two 16x16 tiles hold data, and the sparse tiles read as NULL.
    assert_eq!((int(&batches, 0), int(&batches, 1)), (64 * 64, 2 * 16 * 16));
}

#[tokio::test]
async fn count_star_counts_pixels() {
    let ctx = SessionContext::new();
    register(&ctx, "test.tif").await;
    let batches = sql(&ctx, "SELECT count(*) FROM tiff_t").await;
    assert_eq!(int(&batches, 0), 380 * 1287);
}

#[tokio::test]
async fn projection_pushdown_through_datafusion() {
    let ctx = SessionContext::new();
    register(&ctx, "test.tif").await;
    let df = ctx
        .sql("SELECT \"band.0\", \"geo.lat\" FROM tiff_t")
        .await
        .unwrap();
    let names: Vec<&str> = df
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["band.0", "geo.lat"]);
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_columns(), 2);
    assert_eq!(rows(&batches), 380 * 1287);
}

#[tokio::test]
async fn predicate_pushdown_prunes_through_datafusion() {
    let ctx = SessionContext::new();
    register(&ctx, "test.tif").await;
    let batches = sql(
        &ctx,
        "SELECT \"geo.lat\" FROM tiff_t WHERE \"geo.lat\" > 1000",
    )
    .await;
    assert_eq!(rows(&batches), 0, "an impossible latitude gives no rows");
}

#[tokio::test]
async fn predicate_pushdown_selects_subset_through_datafusion() {
    let ctx = SessionContext::new();
    register(&ctx, "test.tif").await;
    let batches = sql(
        &ctx,
        "SELECT \"geo.lat\" FROM tiff_t WHERE \"geo.lat\" > 40",
    )
    .await;
    for batch in &batches {
        let lat = batch.column(0).as_primitive::<Float64Type>();
        assert!(
            lat.values().iter().all(|&v| v > 40.0),
            "every lat satisfies the filter"
        );
    }
    let total = rows(&batches);
    assert!(
        total > 0 && total < 380 * 1287,
        "the filter keeps some rows: {total}"
    );
}

#[tokio::test]
async fn a_limit_returns_its_rows() {
    let ctx = SessionContext::new();
    register(&ctx, "test.tif").await;
    let batches = sql(&ctx, "SELECT \"band.0\" FROM tiff_t LIMIT 10").await;
    assert_eq!(rows(&batches), 10);
}

/// A filter behind a cast keeps each row that it matches.
#[tokio::test]
async fn a_cast_filter_keeps_every_row() {
    let ctx = SessionContext::new();
    register(&ctx, "test.tif").await;
    let cast = sql(
        &ctx,
        "SELECT count(\"band.0\") FROM tiff_t WHERE CAST(\"geo.lat\" AS INT) = 40",
    )
    .await;
    let range = sql(
        &ctx,
        "SELECT count(\"band.0\") FROM tiff_t WHERE \"geo.lat\" >= 40 AND \"geo.lat\" < 41",
    )
    .await;
    assert!(int(&range, 0) > 0);
    assert_eq!(int(&cast, 0), int(&range, 0));
}

#[tokio::test]
async fn a_file_without_geo_tags_reads_its_bands() {
    let ctx = SessionContext::new();
    register(&ctx, "plain_no_geo_u8.tif").await;
    let df = ctx.sql("SELECT * FROM tiff_t").await.unwrap();
    assert!(df.schema().field_with_unqualified_name("geo.lat").is_err());
    let batches = sql(
        &ctx,
        "SELECT count(\"band.0\"), max(\"band.0\") FROM tiff_t",
    )
    .await;
    assert_eq!(int(&batches, 0), 6 * 4);
    let max = arrow::util::display::array_value_to_string(batches[0].column(1), 0).unwrap();
    // Pixel (row, col) holds (row * 100 + col) as u8, so row 3 wraps to 44..49.
    assert_eq!(max, "205", "the maximum is pixel (2, 5)");
}

#[tokio::test]
async fn a_partitioned_table_is_refused() {
    let root = std::env::temp_dir().join(format!("beacon-tiff-partition-{}", std::process::id()));
    let dir = root.join("year=2024");
    std::fs::create_dir_all(&dir).unwrap();
    let fixture = format!(
        "{}/test-files/plain_no_geo_u8.tif",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::copy(fixture, dir.join("plain.tif")).unwrap();

    let ctx = SessionContext::new();
    let root_path = format!("{}/", root.display()).replace('\\', "/");
    register_path(&ctx, &root_path, |options| {
        options.with_table_partition_cols(vec![("year".to_string(), DataType::Utf8)])
    })
    .await;
    let error = ctx
        .sql("SELECT \"band.0\" FROM tiff_t")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("a partitioned TIFF table is refused");
    assert!(
        error.to_string().contains("PARTITIONED BY year"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn infer_schema_names_the_tags() {
    let ctx = SessionContext::new();
    register(&ctx, "test.tif").await;
    let df = ctx.sql("SELECT * FROM tiff_t").await.unwrap();
    for name in ["band.0", "geo.lat", "geo.lon", "image.width"] {
        assert!(
            df.schema().field_with_unqualified_name(name).is_ok(),
            "the schema has {name}"
        );
    }
}
