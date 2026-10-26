//! Native function plugins: load through datafusion-ffi, survive a restart, and
//! refuse a wrong ABI, a panic and an alias collision.

mod common;

use arrow::array::{Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::plugins::{example_plugin, put_plugin_folder};
use common::{restartable_runtime, scalar_i64, TestRuntime};

async fn new_runtime(tag: &str) -> TestRuntime {
    restartable_runtime(tag, |b| b).await
}

async fn install(rt: &TestRuntime, archive: Vec<u8>) -> anyhow::Result<()> {
    rt.runtime
        .install_plugin_archive("example", Bytes::from(archive), "test")
        .await
}

async fn install_error(rt: &TestRuntime, archive: Vec<u8>) -> String {
    match install(rt, archive).await {
        Ok(()) => panic!("expected the install of 'example' to fail"),
        Err(error) => format!("{error:#}"),
    }
}

async fn sql_error(rt: &TestRuntime, sql: &str) -> String {
    match rt.try_sql(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`"),
        Err(error) => format!("{error:#}"),
    }
}

fn scalar_f64(batches: &[RecordBatch]) -> f64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("expected a Float64 column")
        .value(0)
}

async fn f64_of(rt: &TestRuntime, sql: &str) -> f64 {
    scalar_f64(&rt.sql(sql).await)
}

/// The status and error of plugin `example` in `beacon.system.plugins`.
async fn example_status(rt: &TestRuntime) -> (String, Option<String>) {
    let batches = rt
        .sql("SELECT status, error FROM beacon.system.plugins WHERE name = 'example'")
        .await;
    let batch = &batches[0];
    let text = |col: usize| {
        let column = batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a Utf8 column");
        (!column.is_null(0)).then(|| column.value(0).to_string())
    };
    (text(0).unwrap(), text(1))
}

#[tokio::test(flavor = "multi_thread")]
async fn native_and_sql_functions_work() {
    let rt = new_runtime("native-works").await;
    install(&rt, example_plugin(false)).await.unwrap();
    assert_eq!(f64_of(&rt, "SELECT example_add_one(1.5)").await, 2.5);
    assert_eq!(f64_of(&rt, "SELECT example_add_two(1.0)").await, 3.0);
    assert_eq!(f64_of(&rt, "SELECT example_inc(0.0)").await, 1.0);
    let (status, error) = example_status(&rt).await;
    assert_eq!((status.as_str(), error), ("loaded", None));
}

#[tokio::test(flavor = "multi_thread")]
async fn native_plugin_survives_a_restart() {
    let rt = new_runtime("native-restart").await;
    install(&rt, example_plugin(false)).await.unwrap();
    let rt = rt.restart().await;
    assert_eq!(f64_of(&rt, "SELECT example_add_one(1.5)").await, 2.5);
    assert_eq!(f64_of(&rt, "SELECT example_add_two(1.0)").await, 3.0);
    let error = sql_error(&rt, "DROP FUNCTION example_inc").await;
    assert!(error.contains("belongs to plugin 'example'"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_ffi_version_fails_and_server_runs() {
    let rt = new_runtime("native-wrong-abi").await;
    let error = install_error(&rt, example_plugin(true)).await;
    assert!(error.contains("DataFusion FFI 0"), "{error}");
    assert_eq!(scalar_i64(&rt.sql("SELECT CAST(1 AS BIGINT)").await), 1);
    let error = sql_error(&rt, "SELECT example_add_one(1.0)").await;
    assert!(error.contains("example_add_one"), "{error}");

    // A fresh root with the plugin folder written by hand, so only startup checks it.
    let rt = new_runtime("native-wrong-abi-startup").await;
    put_plugin_folder(&rt.plugins_dir(), "example", &example_plugin(true));
    let rt = rt.restart().await;
    assert_eq!(scalar_i64(&rt.sql("SELECT CAST(1 AS BIGINT)").await), 1);
    let (status, error) = example_status(&rt).await;
    assert_eq!(status, "failed");
    assert!(
        error
            .as_deref()
            .is_some_and(|e| e.contains("DataFusion FFI 0")),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn native_reinstall_after_uninstall_works() {
    let rt = new_runtime("native-reinstall").await;
    let archive = example_plugin(false);
    install(&rt, archive.clone()).await.unwrap();
    rt.sql("UNINSTALL example").await;
    install(&rt, archive).await.unwrap();
    assert_eq!(f64_of(&rt, "SELECT example_add_one(1.0)").await, 2.0);
    let (status, error) = example_status(&rt).await;
    assert_eq!((status.as_str(), error), ("loaded", None));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_is_a_query_error() {
    let rt = new_runtime("native-panic").await;
    install(&rt, example_plugin(false)).await.unwrap();
    let error = sql_error(&rt, "SELECT example_panic()").await;
    assert!(error.contains("panicked"), "{error}");
    assert_eq!(f64_of(&rt, "SELECT example_add_one(1.0)").await, 2.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alias_collision_fails_the_plugin() {
    let rt = new_runtime("native-alias").await;
    rt.sql("CREATE FUNCTION example_inc(x DOUBLE) RETURNS DOUBLE RETURN x + 10")
        .await;
    let error = install_error(&rt, example_plugin(false)).await;
    assert!(error.contains("example_inc"), "{error}");
    let error = sql_error(&rt, "SELECT example_add_one(1.0)").await;
    assert!(error.contains("example_add_one"), "{error}");
    assert_eq!(f64_of(&rt, "SELECT example_inc(1.0)").await, 11.0);
}

/// Writes `points.parquet` into the datasets root: two row groups, west then east.
fn write_points(rt: &TestRuntime) {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lon", DataType::Float64, false),
        Field::new("lat", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Float64Array::from(vec![-10.0, -9.0, 3.0, 4.0])),
            Arc::new(Float64Array::from(vec![10.0, 11.0, 52.0, 53.0])),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let file = std::fs::File::create(rt.datasets_dir().join("points.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn plan_text(rt: &TestRuntime, sql: &str) -> String {
    let batches = rt.sql(sql).await;
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

async fn sorted_ids(rt: &TestRuntime, sql: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = rt
        .sql(sql)
        .await
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("an Int64 column");
            ids.values().to_vec()
        })
        .collect();
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn native_hooks_give_a_pruning_predicate() {
    let rt = new_runtime("native-hooks").await;
    install(&rt, example_plugin(false)).await.unwrap();
    write_points(&rt);
    let sql = "SELECT id FROM read_parquet('points.parquet') \
               WHERE example_in_box(lon, lat, 2, 50, 8, 56)";
    let plan = plan_text(&rt, &format!("EXPLAIN {sql}")).await;
    assert!(plan.contains("example_in_box("), "{plan}");
    assert!(plan.contains("lon >= Float64(2)"), "{plan}");
    assert!(plan.contains("lat <= Float64(56)"), "{plan}");
    let analyzed = plan_text(&rt, &format!("EXPLAIN ANALYZE {sql}")).await;
    assert!(
        analyzed.contains("row_groups_pruned_statistics=2 total → 1 matched"),
        "{analyzed}"
    );
    assert_eq!(sorted_ids(&rt, sql).await, [3, 4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_native_hook_is_ignored() {
    let rt = new_runtime("native-bad-hooks").await;
    install(&rt, example_plugin(false)).await.unwrap();
    write_points(&rt);
    let sql = "SELECT id FROM read_parquet('points.parquet') \
               WHERE example_bad_box(lon, lat, 2, 50, 8, 56)";
    let plan = plan_text(&rt, &format!("EXPLAIN {sql}")).await;
    assert!(!plan.contains("lon >= Float64(2)"), "{plan}");
    assert_eq!(sorted_ids(&rt, sql).await, [3, 4]);
}
