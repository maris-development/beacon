//! End-to-end tests for `VACUUM`, which rewrites `beacon.db` to its live size.

mod common;

use std::path::{Path, PathBuf};

use beacon_core::AuthIdentity;
use common::TestRuntime;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::record_batch::RecordBatch;
use tempfile::TempDir;

/// A runtime on a `beacon.db` file in `dir`.
async fn file_runtime(dir: &TempDir, read_only: bool) -> (TestRuntime, PathBuf) {
    let db_path = dir.path().join("beacon.db");
    let rt = common::runtime_with("vacuum", |b| {
        b.with_db_path(db_path.clone()).with_read_only(read_only)
    })
    .await;
    (rt, db_path)
}

/// Read a named `UInt64` column out of the single-row `VACUUM` report.
fn report_value(batches: &[RecordBatch], column: &str) -> u64 {
    let batch = &batches[0];
    let index = batch.schema().index_of(column).expect("report column");
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("report columns should be UInt64")
        .value(0)
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).expect("beacon.db metadata").len()
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_shrinks_the_file_and_keeps_the_tables() {
    let dir = TempDir::new().unwrap();
    let (rt, db_path) = file_runtime(&dir, false).await;

    rt.sql("CREATE TABLE t (id BIGINT, name VARCHAR)").await;
    for id in 0..20 {
        rt.sql(&format!(
            "INSERT INTO t VALUES ({id}, '{}')",
            "x".repeat(100_000)
        ))
        .await;
    }
    rt.sql("COMPACT TABLE t WITH ('cleanup_older_than' '0s')")
        .await;
    let size_before = file_len(&db_path);

    let report = rt.sql("VACUUM").await;

    let size_after = report_value(&report, "size_after_bytes");
    assert_eq!(report_value(&report, "size_before_bytes"), size_before);
    assert!(size_after < size_before, "{size_before} -> {size_after}");
    assert_eq!(size_after, file_len(&db_path));
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM t").await),
        20
    );
    rt.sql("INSERT INTO t VALUES (20, 'y')").await;
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM t").await),
        21
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_needs_a_super_user() {
    let dir = TempDir::new().unwrap();
    let (rt, db_path) = file_runtime(&dir, false).await;
    let size_before = file_len(&db_path);

    let result = rt.try_sql_as("VACUUM", AuthIdentity::empty()).await;

    assert!(result.is_err());
    assert_eq!(file_len(&db_path), size_before);
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_fails_on_an_in_memory_database() {
    let rt = common::runtime("vacuum-memory").await;

    let error = rt.try_sql("VACUUM").await.unwrap_err();

    assert!(
        format!("{error:#}").contains("VACUUM is not available"),
        "{error:#}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_fails_on_a_read_only_database() {
    let dir = TempDir::new().unwrap();
    // The first runtime creates the file, and its drop releases the file lock.
    drop(file_runtime(&dir, false).await);
    let (rt, _db_path) = file_runtime(&dir, true).await;

    let error = rt.try_sql("VACUUM").await.unwrap_err();

    assert!(
        format!("{error:#}").contains("VACUUM is not available"),
        "{error:#}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_is_refused_while_another_query_is_active() {
    let dir = TempDir::new().unwrap();
    let (rt, _db_path) = file_runtime(&dir, false).await;
    // A result that the caller did not drain is an active query.
    let open_result = rt
        .runtime
        .run_query(
            beacon_core::query::Query::sql("SELECT 1".to_string()),
            rt.admin().await,
        )
        .await
        .unwrap();

    let error = rt.try_sql("VACUUM").await.unwrap_err();

    assert!(
        format!("{error:#}").contains("1 other queries are active"),
        "{error:#}"
    );
    drop(open_result);
    rt.sql("VACUUM").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_work_again_after_a_vacuum() {
    let dir = TempDir::new().unwrap();
    let (rt, _db_path) = file_runtime(&dir, false).await;

    rt.sql("VACUUM").await;
    rt.sql("VACUUM").await;

    assert_eq!(common::total_rows(&rt.sql("SELECT 1").await), 1);
}
