//! End-to-end crawler test for the CSV format: a Hive-partitioned tree of `.csv`
//! files is discovered and registered as a queryable, partition-pruned table.
//!
//! In its own test binary for isolated `BEACON_*` env.

use std::path::Path;
use std::sync::Arc;

use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

async fn run(runtime: &Runtime, sql: &str) -> Vec<RecordBatch> {
    runtime
        .run_query(Query::sql(sql.to_string()), beacon_core::AuthIdentity::system())
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
        .into_record_stream()
        .unwrap_or_else(|e| panic!("expected stream: {sql}\n{e}"))
        .try_collect()
        .await
        .unwrap_or_else(|e| panic!("stream failed: {sql}\n{e}"))
}

fn scalar_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0)
}

/// Write a CSV file (header `v,name`, one data row) at `path`.
fn write_csv(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, "v,name\n1,a\n").unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn crawler_discovers_partitioned_csv() {
    let tmp = std::env::temp_dir().join(format!("beacon_crawl_csv_{}", std::process::id()));
    std::env::set_var("BEACON_DATA_DIR", &tmp);
    std::env::set_var("BEACON_ENABLE_FS_EVENTS", "false");
    std::env::set_var("BEACON_FLIGHT_SQL_ENABLE", "false");

    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");
    let datasets = runtime.config().storage.datasets_dir.clone();

    write_csv(&datasets.join("csv_src/year=2024/a.csv"));
    write_csv(&datasets.join("csv_src/year=2025/b.csv"));

    run(&runtime, "CREATE CRAWLER csvc ON 'csv_src/'").await;
    run(&runtime, "RUN CRAWLER csvc").await;

    let count = scalar_count(&run(&runtime, "SELECT count(*) FROM csv_src").await);
    assert_eq!(count, 2, "both CSV partition files should be discovered");

    let pruned =
        scalar_count(&run(&runtime, "SELECT count(*) FROM csv_src WHERE year = '2024'").await);
    assert_eq!(pruned, 1, "partition column 'year' should filter to one file");

    let _ = std::fs::remove_dir_all(&tmp);
}
