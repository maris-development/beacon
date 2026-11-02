//! Table type plugins through the real loader: the fixture library exports `exkv`
//! (CSV files in a local folder), `expanic` (panics in `create`), `exgridkv` (a 2x3 nd
//! grid) and `exndmixed` (nd and plain columns).

mod common;

use bytes::Bytes;
use common::plugins::example_plugin;
use common::{restartable_runtime, scalar_i64, total_rows, TestRuntime};

async fn install(rt: &TestRuntime) {
    rt.runtime
        .install_plugin_archive("example", Bytes::from(example_plugin(false)), "test")
        .await
        .unwrap();
}

async fn runtime_with_plugin(tag: &str) -> TestRuntime {
    let rt = restartable_runtime(tag, |b| b).await;
    install(&rt).await;
    rt
}

/// A folder for the plugin's own files, outside the Beacon stores.
fn kv_folder(rt: &TestRuntime) -> String {
    let folder = rt.tmp_dir().parent().unwrap().join("kv-data");
    folder.to_string_lossy().replace('\\', "/")
}

async fn create_kv(rt: &TestRuntime, stored_as: &str) {
    let folder = kv_folder(rt);
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE t (a BIGINT, b VARCHAR) STORED AS {stored_as} LOCATION '{folder}'"
    ))
    .await;
}

async fn sql_error(rt: &TestRuntime, sql: &str) -> String {
    match rt.try_sql(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`"),
        Err(error) => format!("{error:#}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn create_insert_select() {
    let rt = runtime_with_plugin("tables-create").await;
    create_kv(&rt, "exkv").await;
    let inserted = rt.sql("INSERT INTO t VALUES (1, 'x'), (2, 'y')").await;
    let count = inserted[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("a UInt64 count")
        .value(0);
    assert_eq!(count, 2);
    assert_eq!(scalar_i64(&rt.sql("SELECT sum(a) FROM t").await), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn stored_as_ignores_case() {
    let rt = runtime_with_plugin("tables-case").await;
    create_kv(&rt, "ExKv").await;
    rt.sql("INSERT INTO t VALUES (5, 'z')").await;
    assert_eq!(scalar_i64(&rt.sql("SELECT sum(a) FROM t").await), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_function_filter_gives_correct_rows() {
    let rt = runtime_with_plugin("tables-filter").await;
    create_kv(&rt, "exkv").await;
    rt.sql("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .await;
    let batches = rt
        .sql("SELECT a FROM t WHERE example_add_one(CAST(a AS DOUBLE)) > 2.5")
        .await;
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_is_a_sql_error() {
    let rt = runtime_with_plugin("tables-panic").await;
    let error = sql_error(
        &rt,
        "CREATE EXTERNAL TABLE p STORED AS expanic LOCATION 'x'",
    )
    .await;
    assert!(error.contains("expanic create"), "{error}");
    assert_eq!(scalar_i64(&rt.sql("SELECT CAST(1 AS BIGINT)").await), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_table_survives_a_restart() {
    let rt = runtime_with_plugin("tables-restart").await;
    create_kv(&rt, "exkv").await;
    rt.sql("INSERT INTO t VALUES (4, 'x'), (6, 'y')").await;
    let rt = rt.restart().await;
    assert_eq!(scalar_i64(&rt.sql("SELECT sum(a) FROM t").await), 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_uninstall_stops_the_table() {
    let rt = runtime_with_plugin("tables-uninstall").await;
    create_kv(&rt, "exkv").await;
    rt.sql("INSERT INTO t VALUES (7, 'x')").await;
    rt.sql("UNINSTALL example").await;
    let error = sql_error(&rt, "SELECT * FROM t").await;
    assert!(error.contains("plugin 'example'"), "{error}");

    // Without the plugin a restart skips the table, and keeps its definition.
    let rt = rt.restart().await;
    let error = sql_error(&rt, "SELECT * FROM t").await;
    assert!(error.contains("not found"), "{error}");
    install(&rt).await;
    let rt = rt.restart().await;
    assert_eq!(scalar_i64(&rt.sql("SELECT sum(a) FROM t").await), 7);
}

async fn create_grid(rt: &TestRuntime) {
    rt.sql("CREATE EXTERNAL TABLE g STORED AS exgridkv LOCATION 'x'")
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_nd_table_broadcasts_the_grid() {
    let rt = runtime_with_plugin("tables-nd-select").await;
    create_grid(&rt).await;
    let batches = rt.sql("SELECT lat, lon, v FROM g").await;
    assert_eq!(total_rows(&batches), 6);
    let schema = batches[0].schema();
    assert_eq!(
        schema.field(0).data_type(),
        &arrow::datatypes::DataType::Int64
    );
    assert_eq!(
        schema.field(2).data_type(),
        &arrow::datatypes::DataType::Float64
    );
    // Each lat value repeats over the 3 lon values.
    let sum = rt.sql("SELECT sum(lat) FROM g WHERE v IS NOT NULL").await;
    assert_eq!(scalar_i64(&sum), 90);
    let sum = rt.sql("SELECT sum(lon) FROM g WHERE v IS NOT NULL").await;
    assert_eq!(scalar_i64(&sum), 12);
    let sum = rt
        .sql("SELECT CAST(sum(v - lat - lon) AS BIGINT) FROM g")
        .await;
    assert_eq!(scalar_i64(&sum), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_nd_table_counts_the_grid_cells() {
    let rt = runtime_with_plugin("tables-nd-count").await;
    create_grid(&rt).await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM g").await), 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_nd_table_filters_on_a_coordinate() {
    let rt = runtime_with_plugin("tables-nd-filter").await;
    create_grid(&rt).await;
    let count = rt.sql("SELECT count(v) FROM g WHERE lat = 20").await;
    assert_eq!(scalar_i64(&count), 3);
    let sum = rt
        .sql("SELECT CAST(sum(v) AS BIGINT) FROM g WHERE lat = 20 AND lon > 1")
        .await;
    assert_eq!(scalar_i64(&sum), 45);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_nd_schema_is_refused() {
    let rt = runtime_with_plugin("tables-nd-mixed").await;
    let error = sql_error(
        &rt,
        "CREATE EXTERNAL TABLE m STORED AS exndmixed LOCATION 'x'",
    )
    .await;
    assert!(
        error.contains("mixes beacon.nd columns and plain columns"),
        "{error}"
    );
    assert!(error.contains("name"), "{error}");
}
