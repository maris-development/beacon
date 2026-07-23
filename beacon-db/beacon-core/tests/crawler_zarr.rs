//! Crawler behaviour for Zarr stores.
//!
//! Zarr is a directory/marker-based store (`<name>.zarr/zarr.json`), read via
//! `read_zarr`'s `SuperListingTable` — there is no `CREATE EXTERNAL TABLE STORED
//! AS ZARR` path. The crawler therefore intentionally **skips** Zarr stores (its
//! v1 rule requires the file extension to equal the format). This test locks in
//! that safe behaviour: a Zarr store under the crawled prefix is ignored without
//! breaking the crawl, while a co-located CSV is still registered.

mod common;

use common::{scalar_i64, write_file, TestRuntime};

/// `SELECT count(*)`; `None` when the table does not exist.
async fn try_count(rt: &TestRuntime, table: &str) -> Option<i64> {
    let identity = rt.admin().await;
    let batches = rt
        .try_sql_as(&format!("SELECT count(*) FROM {table}"), identity)
        .await
        .ok()?;
    Some(scalar_i64(&batches))
}

#[tokio::test(flavor = "multi_thread")]
async fn crawler_skips_zarr_stores_but_still_crawls_siblings() {
    let rt = common::runtime("crawl-zarr").await;
    let datasets = rt.datasets_dir().to_path_buf();

    // A Zarr store marker (recognised by discovery as a Zarr dataset) ...
    write_file(&datasets.join("mixed/cube.zarr/zarr.json"), "{}");
    // ... alongside a regular CSV under the same crawled prefix.
    write_file(&datasets.join("mixed/readings/r.csv"), "v,name\n1,a\n");

    // The crawl must succeed (the Zarr store must not error it out).
    rt.sql("CREATE CRAWLER mixed ON 'mixed/'").await;
    rt.sql("RUN CRAWLER mixed").await;

    // The CSV sibling is registered ...
    assert_eq!(
        try_count(&rt, "readings").await,
        Some(1),
        "the CSV dataset should be crawled into a table"
    );

    // ... but the Zarr store is skipped (no `cube`/`cube_zarr` table is created).
    assert_eq!(
        try_count(&rt, "cube_zarr").await,
        None,
        "Zarr stores are not crawlable in v1 and must be skipped"
    );
    assert_eq!(try_count(&rt, "cube").await, None);
}
