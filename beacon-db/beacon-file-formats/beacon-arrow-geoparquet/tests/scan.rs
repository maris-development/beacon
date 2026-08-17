//! End-to-end checks on the GeoParquet scan: the columns it returns, and the
//! row groups it reads.
//!
//! Every projection here starts at a column other than the first. That is the
//! shape [#378](https://github.com/maris-development/beacon/issues/378) reported:
//! a scan that selected the right columns but kept their old positions ran only
//! while the projection began at column zero, and Beacon writes the geometry
//! column last.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use beacon_arrow_geoparquet::datafusion::{GeoParquetFormat, GeoParquetOptions};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor, accept, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use geoarrow::array::PointBuilder;
use geoarrow::datatypes::{Dimension, Metadata, PointType};
use geoarrow_array::GeoArrowArray;
use geoparquet::writer::{
    GeoParquetRecordBatchEncoder, GeoParquetWriterEncoding, GeoParquetWriterOptionsBuilder,
};
use object_store::ObjectMeta;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Rows per row group in the fixture, so a spatial filter has something to skip.
const ROW_GROUP_ROWS: usize = 4;

/// A file of `groups * ROW_GROUP_ROWS` rows over four columns.
///
/// `geometry` is last, and its points march east: row `i` sits at
/// `(i, i)`. So a box over a few eastings selects a known slice of the
/// rows *and* a known slice of the row groups.
fn write_fixture(path: &std::path::Path, groups: usize) {
    let point_type = PointType::new(Dimension::XY, Arc::new(Metadata::default()));
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("time", DataType::Int64, false)),
        Arc::new(Field::new("longitude", DataType::Float64, true)),
        Arc::new(Field::new("temperature", DataType::Float64, true)),
        Arc::new(point_type.to_field("geometry", true)),
    ]));

    let rows = groups * ROW_GROUP_ROWS;
    let mut points = PointBuilder::new(point_type);
    for i in 0..rows {
        points.push_coord(Some(&(i as f64, i as f64)));
    }
    let geometry: ArrayRef = points.finish().to_array_ref();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| 10 * i as i64),
            )) as ArrayRef,
            Arc::new(Float64Array::from_iter_values((0..rows).map(|i| i as f64))),
            Arc::new(Float64Array::from_iter_values(
                (0..rows).map(|i| 100.0 + i as f64),
            )),
            geometry,
        ],
    )
    .unwrap();

    let options = GeoParquetWriterOptionsBuilder::default()
        .set_encoding(GeoParquetWriterEncoding::GeoArrow)
        .build();
    let mut encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options).unwrap();

    let properties = WriterProperties::builder()
        .set_max_row_group_size(ROW_GROUP_ROWS)
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, encoder.target_schema(), Some(properties)).unwrap();
    writer
        .write(&encoder.encode_record_batch(&batch).unwrap())
        .unwrap();
    let kv = encoder.into_keyvalue().unwrap();
    writer.append_key_value_metadata(kv);
    writer.finish().unwrap();
}

fn format() -> Arc<GeoParquetFormat> {
    Arc::new(GeoParquetFormat::new(GeoParquetOptions {
        longitude_column: None,
        latitude_column: None,
    }))
}

/// A session over one GeoParquet file, with the spatial functions registered.
async fn table(groups: usize) -> (SessionContext, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(&dir.path().join("part-0.geoparquet"), groups);

    // One partition, so the row group counters below read as one file's worth.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    datafusion_spatial::register_all(&ctx);

    let options = ListingOptions::new(format()).with_file_extension(".geoparquet");
    let url = ListingTableUrl::parse(dir.path().to_str().unwrap()).unwrap();
    let schema = options.infer_schema(&ctx.state(), &url).await.unwrap();
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).unwrap()))
        .unwrap();
    (ctx, dir)
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning {sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("running {sql}: {e}"))
}

fn column_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

fn one_batch(batches: Vec<RecordBatch>) -> RecordBatch {
    let schema = batches.first().expect("at least one batch").schema();
    arrow::compute::concat_batches(&schema, &batches).expect("concat")
}

// ── the columns the scan returns ────────────────────────────────────────────

/// A projection of one column after the first returns that column's values.
#[tokio::test]
async fn projects_a_column_after_the_first() {
    let (ctx, _dir) = table(1).await;
    let batch = one_batch(query(&ctx, "SELECT temperature FROM t ORDER BY temperature").await);

    assert_eq!(column_names(&batch), vec!["temperature"]);
    assert_eq!(
        batch.column(0).as_primitive::<Float64Type>().values(),
        &[100.0, 101.0, 102.0, 103.0]
    );
}

/// A filter on a column after the first reads that column, not its neighbour.
#[tokio::test]
async fn filters_on_a_column_after_the_first() {
    let (ctx, _dir) = table(1).await;
    let batch = one_batch(query(&ctx, "SELECT count(*) FROM t WHERE temperature > 101.5").await);
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 2);

    // `longitude` holds 0..4 and `temperature` 100..104, so a predicate that
    // read the wrong column would answer 0 or 4 here, never 2.
    let batch = one_batch(query(&ctx, "SELECT count(*) FROM t WHERE longitude > 1.5").await);
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 2);
}

/// The geometry column is written last, so every query over it projects past
/// every plain column.
#[tokio::test]
async fn projects_the_geometry_column() {
    let (ctx, _dir) = table(1).await;
    let batch = one_batch(query(&ctx, "SELECT geometry FROM t").await);

    let geometry = batch.column(0).as_struct();
    assert_eq!(
        geometry
            .column_by_name("x")
            .expect("x child")
            .as_primitive::<Float64Type>()
            .values(),
        &[0.0, 1.0, 2.0, 3.0]
    );
}

/// An aliased projection is pushed into the scan whole. The scan has to rename
/// the column it read, not look for one under the alias and null-fill.
#[tokio::test]
async fn applies_an_aliased_projection() {
    let (ctx, _dir) = table(1).await;
    let batch = one_batch(query(&ctx, "SELECT temperature AS t_degrees FROM t").await);

    assert_eq!(batch.schema().field(0).name(), "t_degrees");
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(
        batch.column(0).as_primitive::<Float64Type>().values(),
        &[100.0, 101.0, 102.0, 103.0]
    );
}

/// A projection that reorders columns returns them in the order asked for.
#[tokio::test]
async fn applies_a_reordered_projection() {
    let (ctx, _dir) = table(1).await;
    let batch = one_batch(query(&ctx, "SELECT temperature, time FROM t").await);

    assert_eq!(column_names(&batch), vec!["temperature", "time"]);
    assert_eq!(
        batch.column(1).as_primitive::<Int64Type>().values(),
        &[0, 10, 20, 30]
    );
}

/// `count(*)` selects no column at all, which the reader has to carry as a row
/// count rather than an empty batch.
#[tokio::test]
async fn counts_rows_without_reading_a_column() {
    let (ctx, _dir) = table(3).await;
    let batch = one_batch(query(&ctx, "SELECT count(*) FROM t").await);
    assert_eq!(
        batch.column(0).as_primitive::<Int64Type>().value(0),
        (3 * ROW_GROUP_ROWS) as i64
    );
}

/// A file split across partitions by byte range is read once, not once per
/// partition.
///
/// DataFusion divides a large file into ranges and hands one to each partition.
/// A reader that ignores the range reads the whole file every time, so the scan
/// returns each row as many times as there are partitions.
#[tokio::test]
async fn a_file_split_across_partitions_returns_each_row_once() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(&dir.path().join("part-0.geoparquet"), 8);

    // Four partitions, and any file at all is large enough to divide.
    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_usize("datafusion.optimizer.repartition_file_min_size", 1);
    let ctx = SessionContext::new_with_config(config);

    let options = ListingOptions::new(format()).with_file_extension(".geoparquet");
    let url = ListingTableUrl::parse(dir.path().to_str().unwrap()).unwrap();
    let schema = options.infer_schema(&ctx.state(), &url).await.unwrap();
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).unwrap()))
        .unwrap();

    let plan = ctx
        .sql("SELECT time FROM t")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    assert!(
        plan.properties().output_partitioning().partition_count() > 1,
        "this test needs the file divided across partitions"
    );

    let batch = one_batch(collect(plan, ctx.task_ctx()).await.expect("run"));
    let rows = 8 * ROW_GROUP_ROWS;
    assert_eq!(batch.num_rows(), rows);

    let mut times: Vec<i64> = batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();
    times.sort_unstable();
    assert_eq!(
        times,
        (0..rows).map(|i| 10 * i as i64).collect::<Vec<_>>(),
        "every row appears exactly once"
    );
}

// ── spatial predicates ──────────────────────────────────────────────────────

/// A spatial predicate over the geometry column runs, and answers the rows the
/// box holds.
#[tokio::test]
async fn a_spatial_predicate_reads_the_geometry_column() {
    let (ctx, _dir) = table(4).await;
    // Points sit at (i, i) for i in 0..16. The box holds i = 2, 3, 4, 5.
    let batch = one_batch(
        query(
            &ctx,
            "SELECT count(*) FROM t \
             WHERE ST_Intersects(geometry, ST_GeomFromText('POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))'))",
        )
        .await,
    );
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 4);
}

/// The same predicate drops the row groups whose own box misses the query box.
#[tokio::test]
async fn a_spatial_predicate_prunes_row_groups() {
    let (ctx, _dir) = table(4).await;
    // Row groups cover i = 0..4, 4..8, 8..12 and 12..16. This box lies inside
    // the first one, so three of the four are dropped unread.
    let plan = ctx
        .sql(
            "SELECT count(*) FROM t \
             WHERE ST_Intersects(geometry, ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))",
        )
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");

    let batches = collect(plan.clone(), ctx.task_ctx()).await.expect("run");
    assert_eq!(
        one_batch(batches)
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0),
        3,
        "points (0,0), (1,1) and (2,2) lie in the box"
    );

    assert_eq!(counter(&plan, "geoparquet_row_groups_considered"), Some(4));
    assert_eq!(counter(&plan, "geoparquet_row_groups_pruned"), Some(3));
}

/// A box that misses every row group drops the whole file, and the scan says so.
#[tokio::test]
async fn a_box_outside_the_file_prunes_it_whole() {
    let (ctx, _dir) = table(4).await;
    let plan = ctx
        .sql(
            "SELECT count(*) FROM t \
             WHERE ST_Intersects(geometry, ST_GeomFromText('POLYGON((90 90, 91 90, 91 91, 90 91, 90 90))'))",
        )
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");

    let batches = collect(plan.clone(), ctx.task_ctx()).await.expect("run");
    assert_eq!(
        one_batch(batches)
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0),
        0
    );
    assert_eq!(counter(&plan, "geoparquet_row_groups_pruned"), Some(4));
    assert_eq!(counter(&plan, "geoparquet_files_pruned"), Some(1));
}

/// A predicate a box cannot decide leaves every row group in place, and still
/// answers correctly.
#[tokio::test]
async fn an_unprunable_predicate_reads_every_row_group() {
    let (ctx, _dir) = table(4).await;
    let plan = ctx
        .sql(
            "SELECT count(*) FROM t \
             WHERE ST_Distance(geometry, ST_GeomFromText('POINT(0 0)')) < 3.0",
        )
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");

    let batches = collect(plan.clone(), ctx.task_ctx()).await.expect("run");
    assert_eq!(
        one_batch(batches)
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0),
        3,
        "(0,0), (1,1) and (2,2) lie within 3 of the origin"
    );
    // `ST_Distance(...) < 3` states no box, so no row group was even considered
    // for pruning and none was dropped.
    assert_eq!(counter(&plan, "geoparquet_row_groups_considered"), Some(0));
    assert_eq!(counter(&plan, "geoparquet_row_groups_pruned"), Some(0));
}

// ── statistics ──────────────────────────────────────────────────────────────

/// A GeoParquet file reports its row count and a range per plain column, so file
/// pruning can drop it before the row group step runs.
#[tokio::test]
async fn infer_stats_reports_a_range_per_plain_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("part-0.geoparquet");
    write_fixture(&path, 2);

    let ctx = SessionContext::new();
    let store = ctx
        .runtime_env()
        .object_store(&ListingTableUrl::parse(dir.path().to_str().unwrap()).unwrap())
        .unwrap();
    let object = ObjectMeta {
        location: object_store::path::Path::from_filesystem_path(&path).unwrap(),
        last_modified: Default::default(),
        size: std::fs::metadata(&path).unwrap().len(),
        e_tag: None,
        version: None,
    };

    let format = format();
    let schema = format
        .infer_schema(&ctx.state(), &store, std::slice::from_ref(&object))
        .await
        .expect("schema");
    let stats = format
        .infer_stats(&ctx.state(), &store, schema.clone(), &object)
        .await
        .expect("statistics");

    assert_eq!(
        stats.num_rows.get_value().copied(),
        Some(2 * ROW_GROUP_ROWS)
    );

    let range = |name: &str| {
        let index = schema.index_of(name).expect("column");
        let column = &stats.column_statistics[index];
        (
            column.min_value.get_value().cloned(),
            column.max_value.get_value().cloned(),
        )
    };

    use datafusion::common::ScalarValue;
    assert_eq!(
        range("time"),
        (
            Some(ScalarValue::Int64(Some(0))),
            Some(ScalarValue::Int64(Some(70)))
        )
    );
    assert_eq!(
        range("temperature"),
        (
            Some(ScalarValue::Float64(Some(100.0))),
            Some(ScalarValue::Float64(Some(107.0)))
        )
    );

    // The geometry column reports no range. A range is one minimum and one
    // maximum value, and a bounding box is neither, so a struct scalar here
    // would be a number nobody can read. Its boxes are per row group, and the
    // scan uses them there.
    assert_eq!(range("geometry"), (None, None));
}

// ── plan metrics ────────────────────────────────────────────────────────────

/// The value of a named counter anywhere in `plan`, or `None` if no node has one.
fn counter(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<usize> {
    struct Find<'a> {
        name: &'a str,
        found: Option<usize>,
    }

    impl ExecutionPlanVisitor for Find<'_> {
        type Error = std::convert::Infallible;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if let Some(metrics) = plan.metrics() {
                for metric in metrics.iter() {
                    if metric.value().name() == self.name {
                        let total = self.found.unwrap_or(0) + metric.value().as_usize();
                        self.found = Some(total);
                    }
                }
            }
            Ok(true)
        }
    }

    let mut find = Find { name, found: None };
    accept(plan.as_ref(), &mut find).expect("the visit cannot fail");
    find.found
}
