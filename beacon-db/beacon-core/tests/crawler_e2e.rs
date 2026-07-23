//! End-to-end test for the Glue-style crawler driven through SQL DDL:
//! `CREATE CRAWLER` -> `RUN CRAWLER` discovers a partitioned Parquet tree and
//! registers it as a queryable external table; `SHOW CRAWLERS` lists it.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{restartable_runtime, scalar_i64, total_rows};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;

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
    // A persistent tables store: the restart below must reload the crawled table and
    // the crawler definition from it.
    let rt = restartable_runtime("crawl-e2e", |b| b).await;

    // Lay down a Hive-partitioned Parquet tree under the datasets store.
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    write_parquet(
        &rt.datasets_dir().join("crawl_src/year=2024/d.parquet"),
        &schema,
    );
    write_parquet(
        &rt.datasets_dir().join("crawl_src/year=2025/d.parquet"),
        &schema,
    );

    // Define + run the crawler.
    rt.sql("CREATE CRAWLER cr ON 'crawl_src/'").await;
    rt.sql("RUN CRAWLER cr").await;

    // SHOW CRAWLERS lists the crawler.
    let shown = rt.sql("SHOW CRAWLERS").await;
    assert!(total_rows(&shown) >= 1, "SHOW CRAWLERS should list 'cr'");

    // The discovered table is named after the base prefix leaf ('crawl_src').
    let count = scalar_i64(&rt.sql("SELECT count(*) FROM crawl_src").await);
    assert_eq!(count, 2, "both partition files should be discovered");

    // The Hive partition column 'year' exists and prunes correctly.
    let pruned = scalar_i64(&rt.sql("SELECT count(*) FROM crawl_src WHERE year = '2024'").await);
    assert_eq!(pruned, 1, "partition column 'year' should filter to one file");

    // Re-running is idempotent: the crawler owns the table, so it updates (not
    // skips) it, and the row count is unchanged.
    rt.sql("RUN CRAWLER cr").await;
    let recount = scalar_i64(&rt.sql("SELECT count(*) FROM crawl_src").await);
    assert_eq!(recount, 2, "re-running the crawler should be idempotent");

    // Restart over the same storage: both the crawled table (as a normal external
    // table) and the crawler definition must reload.
    let rt = rt.restart().await;

    // The crawled table survives restart and is still queryable.
    let reloaded = scalar_i64(&rt.sql("SELECT count(*) FROM crawl_src").await);
    assert_eq!(reloaded, 2, "crawled table should reload after restart");

    // (The config view's hiding of the internal `__crawler__` ownership marker is
    // covered by `api::table_config_redaction_tests`: `Runtime::list_table_config`
    // no longer exists, and the hiding is a pure function of the definition.)

    // The crawler definition reloaded too.
    assert!(
        total_rows(&rt.sql("SHOW CRAWLERS").await) >= 1,
        "crawler should reload after restart"
    );

    // Dropping the crawler leaves the table in place (deletion semantics).
    rt.sql("DROP CRAWLER cr").await;
    let after = rt.sql("SHOW CRAWLERS").await;
    assert_eq!(total_rows(&after), 0, "crawler should be gone after DROP");
    let still_there = scalar_i64(&rt.sql("SELECT count(*) FROM crawl_src").await);
    assert_eq!(still_there, 2, "crawled table should survive DROP CRAWLER");
}
