//! Crawler behaviour for Zarr stores.
//!
//! Zarr is a directory/marker-based store (`<name>.zarr/zarr.json`), read via
//! `read_zarr`'s `SuperListingTable` — there is no `CREATE EXTERNAL TABLE STORED
//! AS ZARR` path. The crawler therefore intentionally **skips** Zarr stores (its
//! v1 rule requires the file extension to equal the format). This test locks in
//! that safe behaviour: a Zarr store under the crawled prefix is ignored without
//! breaking the crawl, while a co-located CSV is still registered.
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
        .run_query(Query::sql(sql.to_string()), true)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
        .into_record_stream()
        .unwrap_or_else(|e| panic!("expected stream: {sql}\n{e}"))
        .try_collect()
        .await
        .unwrap_or_else(|e| panic!("stream failed: {sql}\n{e}"))
}

/// `SELECT count(*)`; `None` when the table does not exist.
async fn try_count(runtime: &Runtime, table: &str) -> Option<i64> {
    let result = runtime
        .run_query(Query::sql(format!("SELECT count(*) FROM {table}")), true)
        .await
        .ok()?;
    let batches: Vec<RecordBatch> = result.into_record_stream().ok()?.try_collect().await.ok()?;
    Some(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64")
            .value(0),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn crawler_skips_zarr_stores_but_still_crawls_siblings() {
    let tmp = std::env::temp_dir().join(format!("beacon_crawl_zarr_{}", std::process::id()));
    std::env::set_var("BEACON_DATA_DIR", &tmp);
    std::env::set_var("BEACON_ENABLE_FS_EVENTS", "false");
    std::env::set_var("BEACON_FLIGHT_SQL_ENABLE", "false");

    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");
    let datasets = runtime.config().storage.datasets_dir.clone();

    // A Zarr store marker (recognised by discovery as a Zarr dataset) ...
    let zarr_json = datasets.join("mixed/cube.zarr/zarr.json");
    std::fs::create_dir_all(zarr_json.parent().unwrap()).unwrap();
    std::fs::write(&zarr_json, "{}").unwrap();
    // ... alongside a regular CSV under the same crawled prefix.
    let csv = datasets.join("mixed/readings/r.csv");
    std::fs::create_dir_all(csv.parent().unwrap()).unwrap();
    std::fs::write(&csv, "v,name\n1,a\n").unwrap();

    // The crawl must succeed (the Zarr store must not error it out).
    run(&runtime, "CREATE CRAWLER mixed ON 'mixed/'").await;
    run(&runtime, "RUN CRAWLER mixed").await;

    // The CSV sibling is registered ...
    assert_eq!(
        try_count(&runtime, "readings").await,
        Some(1),
        "the CSV dataset should be crawled into a table"
    );

    // ... but the Zarr store is skipped (no `cube`/`cube_zarr` table is created).
    assert_eq!(
        try_count(&runtime, "cube_zarr").await,
        None,
        "Zarr stores are not crawlable in v1 and must be skipped"
    );
    assert_eq!(try_count(&runtime, "cube").await, None);

    let _ = std::fs::remove_dir_all(&tmp);
}
