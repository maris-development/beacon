//! Dropping a runtime must release the redb tables store's exclusive lock on
//! `beacon.db`, so the file can be reopened (a restart, or a second instance).
//!
//! Regression test. Anything registered *in* the session's catalog that retains the
//! session — e.g. an `ExternalTable` holding a `SessionState` clone, which shares the
//! session's `catalog_list` — forms a cycle that keeps the object-store registry, and
//! hence the lock, alive for the process lifetime. That is invisible with a plain
//! filesystem tables store and fatal with a single-file redb one, so each way of
//! reaching a table is covered here.
mod common;

use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use beacon_object_storage::StorageConfig;
use futures::TryStreamExt;

fn storage(root: &std::path::Path) -> StorageConfig {
    let s = StorageConfig {
        data_dir: root.to_path_buf(),
        datasets_dir: root.join("datasets"),
        db_path: Some(root.join("beacon.db")),
        tmp_dir: root.join("tmp"),
        enable_fs_events: false,
        ..Default::default()
    };
    for d in [&s.data_dir, &s.datasets_dir, &s.tmp_dir] {
        std::fs::create_dir_all(d).unwrap();
    }
    s
}

async fn sql(rt: &Runtime, q: &str) {
    rt.run_query(Query::sql(q.to_string()), AuthIdentity::system())
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .into_record_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
}

/// Barest case: build a runtime on a redb file, drop it, reopen.
#[tokio::test(flavor = "multi_thread")]
async fn bare_runtime_releases_redb_lock_on_drop() {
    let root = tempfile::tempdir().unwrap();
    let s = storage(root.path());
    let rt = RuntimeBuilder::new()
        .with_storage(s.clone())
        .build()
        .await
        .expect("first build");
    drop(rt);
    let rt2 = RuntimeBuilder::new().with_storage(s).build().await;
    assert!(rt2.is_ok(), "reopen after drop failed: {:?}", rt2.err());
}

/// After creating a managed table.
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_create_table() {
    let root = tempfile::tempdir().unwrap();
    let s = storage(root.path());
    let rt = RuntimeBuilder::new()
        .with_storage(s.clone())
        .build()
        .await
        .unwrap();
    sql(&rt, "CREATE TABLE t (a BIGINT)").await;
    drop(rt);
    let rt2 = RuntimeBuilder::new().with_storage(s).build().await;
    assert!(rt2.is_ok(), "reopen after CREATE TABLE failed: {:?}", rt2.err());
}

/// After only DEFINING a crawler (no run).
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_create_crawler_only() {
    let root = tempfile::tempdir().unwrap();
    let s = storage(root.path());
    std::fs::create_dir_all(s.datasets_dir.join("src")).unwrap();
    std::fs::write(s.datasets_dir.join("src/a.csv"), "v,name\n1,a\n").unwrap();
    let rt = RuntimeBuilder::new().with_storage(s.clone()).build().await.unwrap();
    sql(&rt, "CREATE CRAWLER c ON 'src/'").await;
    drop(rt);
    let rt2 = RuntimeBuilder::new().with_storage(s).build().await;
    assert!(rt2.is_ok(), "reopen after CREATE CRAWLER only failed: {:?}", rt2.err());
}

/// Runtime with the crawler subsystem entirely disabled.
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_with_crawler_disabled() {
    use beacon_core::crawler::CrawlerConfig;
    let root = tempfile::tempdir().unwrap();
    let s = storage(root.path());
    let rt = RuntimeBuilder::new()
        .with_storage(s.clone())
        .with_crawler(CrawlerConfig { enable: false, default_interval_secs: 900 })
        .build()
        .await
        .unwrap();
    drop(rt);
    let rt2 = RuntimeBuilder::new()
        .with_storage(s)
        .with_crawler(CrawlerConfig { enable: false, default_interval_secs: 900 })
        .build()
        .await;
    assert!(rt2.is_ok(), "reopen with crawler disabled failed: {:?}", rt2.err());
}

/// After defining + running a crawler.
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_crawler() {
    let root = tempfile::tempdir().unwrap();
    let s = storage(root.path());
    std::fs::create_dir_all(s.datasets_dir.join("src")).unwrap();
    std::fs::write(s.datasets_dir.join("src/a.csv"), "v,name\n1,a\n").unwrap();

    let rt = RuntimeBuilder::new()
        .with_storage(s.clone())
        .build()
        .await
        .unwrap();
    sql(&rt, "CREATE CRAWLER c ON 'src/'").await;
    sql(&rt, "RUN CRAWLER c").await;
    drop(rt);
    let rt2 = RuntimeBuilder::new().with_storage(s).build().await;
    assert!(rt2.is_ok(), "reopen after crawler failed: {:?}", rt2.err());
}

/// Is the leak the external-table registration path (which the crawler reuses)?
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_create_external_table() {
    let root = tempfile::tempdir().unwrap();
    let s = storage(root.path());
    std::fs::create_dir_all(s.datasets_dir.join("src")).unwrap();
    std::fs::write(s.datasets_dir.join("src/a.csv"), "v,name\n1,a\n").unwrap();

    let rt = RuntimeBuilder::new().with_storage(s.clone()).build().await.unwrap();
    sql(&rt, "CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION 'src/'").await;
    drop(rt);
    let rt2 = RuntimeBuilder::new().with_storage(s).build().await;
    assert!(rt2.is_ok(), "reopen after CREATE EXTERNAL TABLE failed: {:?}", rt2.err());
}

/// Does merely QUERYING an external table leak it?
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_query_external_table() {
    let root = tempfile::tempdir().unwrap();
    let s = storage(root.path());
    std::fs::create_dir_all(s.datasets_dir.join("src")).unwrap();
    std::fs::write(s.datasets_dir.join("src/a.csv"), "v,name\n1,a\n").unwrap();

    let rt = RuntimeBuilder::new().with_storage(s.clone()).build().await.unwrap();
    sql(&rt, "CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION 'src/'").await;
    sql(&rt, "SELECT count(*) FROM ext").await;
    drop(rt);
    let rt2 = RuntimeBuilder::new().with_storage(s).build().await;
    assert!(rt2.is_ok(), "reopen after querying external table failed: {:?}", rt2.err());
}
