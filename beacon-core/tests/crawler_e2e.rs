//! End-to-end test for the Glue-style crawler driven through SQL DDL:
//! `CREATE CRAWLER` -> `RUN CRAWLER` discovers a partitioned Parquet tree and
//! registers it as a queryable external table; `SHOW CRAWLERS` lists it.

use std::path::Path;
use std::sync::Arc;

use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures::TryStreamExt;

/// Run SQL as super-user and collect the result batches.
async fn run(runtime: &Runtime, sql: &str) -> Vec<RecordBatch> {
    runtime
        .run_query(Query::sql(sql.to_string()), true)
        .await
        .unwrap_or_else(|error| panic!("SQL failed to plan/execute: {sql}\n{error}"))
        .into_record_stream()
        .unwrap_or_else(|error| panic!("expected a streamed result: {sql}\n{error}"))
        .try_collect()
        .await
        .unwrap_or_else(|error| panic!("SQL stream failed: {sql}\n{error}"))
}

fn scalar_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column should be Int64")
        .value(0)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Write a single-row Parquet file (columns `v: Int64`, `name: Utf8`) at `path`,
/// creating parent directories. The partition column is encoded in the path only.
fn write_parquet(path: &Path, schema: &Arc<Schema>) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn crawler_discovers_partitioned_parquet() {
    // Isolate this test process: a private data dir, and no FS-event cache so the
    // freshly written files are visible immediately to the crawl.
    let tmp = std::env::temp_dir().join(format!("beacon_crawl_e2e_{}", std::process::id()));
    std::env::set_var("BEACON_DATA_DIR", &tmp);
    std::env::set_var("BEACON_ENABLE_FS_EVENTS", "false");
    std::env::set_var("BEACON_FLIGHT_SQL_ENABLE", "false");

    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");

    // Lay down a Hive-partitioned Parquet tree under the datasets store.
    let datasets = runtime.config().storage.datasets_dir.clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    write_parquet(&datasets.join("crawl_src/year=2024/d.parquet"), &schema);
    write_parquet(&datasets.join("crawl_src/year=2025/d.parquet"), &schema);

    // Define + run the crawler.
    run(&runtime, "CREATE CRAWLER cr ON 'crawl_src/'").await;
    run(&runtime, "RUN CRAWLER cr").await;

    // SHOW CRAWLERS lists the crawler.
    let shown = run(&runtime, "SHOW CRAWLERS").await;
    assert!(total_rows(&shown) >= 1, "SHOW CRAWLERS should list 'cr'");

    // The discovered table is named after the base prefix leaf ('crawl_src').
    let count = scalar_count(&run(&runtime, "SELECT count(*) FROM crawl_src").await);
    assert_eq!(count, 2, "both partition files should be discovered");

    // The Hive partition column 'year' exists and prunes correctly.
    let pruned =
        scalar_count(&run(&runtime, "SELECT count(*) FROM crawl_src WHERE year = '2024'").await);
    assert_eq!(pruned, 1, "partition column 'year' should filter to one file");

    // Re-running is idempotent: the crawler owns the table, so it updates (not
    // skips) it, and the row count is unchanged.
    run(&runtime, "RUN CRAWLER cr").await;
    let recount = scalar_count(&run(&runtime, "SELECT count(*) FROM crawl_src").await);
    assert_eq!(recount, 2, "re-running the crawler should be idempotent");

    // Restart over the same data dir: both the crawled table (as a normal external
    // table) and the crawler definition must reload from disk.
    drop(runtime);
    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should reboot");

    // The crawled table survives restart and is still queryable.
    let reloaded = scalar_count(&run(&runtime, "SELECT count(*) FROM crawl_src").await);
    assert_eq!(reloaded, 2, "crawled table should reload after restart");

    // Its config view loads, with the internal ownership marker hidden.
    let cfg = runtime
        .list_table_config("crawl_src".to_string())
        .await
        .expect("reloaded crawled table should have a config view");
    let cfg_json = serde_json::to_string(&cfg).unwrap();
    assert!(
        !cfg_json.contains("__crawler__"),
        "internal ownership marker must be hidden from table config: {cfg_json}"
    );

    // The crawler definition reloaded too.
    assert!(
        total_rows(&run(&runtime, "SHOW CRAWLERS").await) >= 1,
        "crawler should reload after restart"
    );

    // Dropping the crawler leaves the table in place (deletion semantics).
    run(&runtime, "DROP CRAWLER cr").await;
    let after = run(&runtime, "SHOW CRAWLERS").await;
    assert_eq!(total_rows(&after), 0, "crawler should be gone after DROP");
    let still_there = scalar_count(&run(&runtime, "SELECT count(*) FROM crawl_src").await);
    assert_eq!(still_there, 2, "crawled table should survive DROP CRAWLER");

    let _ = std::fs::remove_dir_all(&tmp);
}
