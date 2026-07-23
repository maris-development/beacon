//! Dropping a runtime must release the redb tables store's exclusive lock on
//! `beacon.db`, so the file can be reopened (a restart, or a second instance).
//!
//! Regression test. Anything registered *in* the session's catalog that retains the
//! session — e.g. an `ExternalTable` holding a `SessionState` clone, which shares the
//! session's `catalog_list` — forms a cycle that keeps the object-store registry, and
//! hence the lock, alive for the process lifetime. That is invisible with a plain
//! filesystem tables store and fatal with a single-file redb one, so each way of
//! reaching a table is covered here.

use std::path::Path;

use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use beacon_datafusion_ext::listing_factory::RootStore;
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::TryStreamExt;

/// A builder over `root`: a redb tables store at `beacon.db`, a local datasets
/// store rooted at `root/datasets` (so a relative `LOCATION 'src/'` resolves
/// there), and a tmp dir. Called twice per test (build, drop, reopen) so both
/// runtimes share identical storage.
fn builder(root: &Path) -> RuntimeBuilder {
    let datasets = root.join("datasets");
    std::fs::create_dir_all(&datasets).unwrap();
    std::fs::create_dir_all(root.join("tmp")).unwrap();
    RuntimeBuilder::new()
        .with_db_path(root.join("beacon.db"))
        .with_default_store(
            ObjectStoreUrl::parse("datasets://").unwrap(),
            RootStore::FileSystem(datasets),
        )
        .with_tmp_dir_path(root.join("tmp"))
}

/// Writes a one-row CSV at `root/datasets/src/a.csv` for the crawler/external-table cases.
fn seed_src_csv(root: &Path) {
    let src = root.join("datasets/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("a.csv"), "v,name\n1,a\n").unwrap();
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
    let rt = builder(root.path()).build().await.expect("first build");
    drop(rt);
    let rt2 = builder(root.path()).build().await;
    assert!(rt2.is_ok(), "reopen after drop failed: {:?}", rt2.err());
}

/// After creating a managed table.
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_create_table() {
    let root = tempfile::tempdir().unwrap();
    let rt = builder(root.path()).build().await.unwrap();
    sql(&rt, "CREATE TABLE t (a BIGINT)").await;
    drop(rt);
    let rt2 = builder(root.path()).build().await;
    assert!(
        rt2.is_ok(),
        "reopen after CREATE TABLE failed: {:?}",
        rt2.err()
    );
}

/// After only DEFINING a crawler (no run).
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_create_crawler_only() {
    let root = tempfile::tempdir().unwrap();
    seed_src_csv(root.path());
    let rt = builder(root.path()).build().await.unwrap();
    sql(&rt, "CREATE CRAWLER c ON 'src/'").await;
    drop(rt);
    let rt2 = builder(root.path()).build().await;
    assert!(
        rt2.is_ok(),
        "reopen after CREATE CRAWLER only failed: {:?}",
        rt2.err()
    );
}

/// Runtime with the crawler subsystem entirely disabled.
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_with_crawler_disabled() {
    use beacon_core::crawler::CrawlerConfig;
    let root = tempfile::tempdir().unwrap();
    let disabled = || CrawlerConfig {
        enable: false,
        default_interval_secs: 900,
    };
    let rt = builder(root.path())
        .with_crawler(disabled())
        .build()
        .await
        .unwrap();
    drop(rt);
    let rt2 = builder(root.path()).with_crawler(disabled()).build().await;
    assert!(
        rt2.is_ok(),
        "reopen with crawler disabled failed: {:?}",
        rt2.err()
    );
}

/// After defining + running a crawler.
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_crawler() {
    let root = tempfile::tempdir().unwrap();
    seed_src_csv(root.path());
    let rt = builder(root.path()).build().await.unwrap();
    sql(&rt, "CREATE CRAWLER c ON 'src/'").await;
    sql(&rt, "RUN CRAWLER c").await;
    drop(rt);
    let rt2 = builder(root.path()).build().await;
    assert!(rt2.is_ok(), "reopen after crawler failed: {:?}", rt2.err());
}

/// Is the leak the external-table registration path (which the crawler reuses)?
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_create_external_table() {
    let root = tempfile::tempdir().unwrap();
    seed_src_csv(root.path());
    let rt = builder(root.path()).build().await.unwrap();
    sql(
        &rt,
        "CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION 'src/'",
    )
    .await;
    drop(rt);
    let rt2 = builder(root.path()).build().await;
    assert!(
        rt2.is_ok(),
        "reopen after CREATE EXTERNAL TABLE failed: {:?}",
        rt2.err()
    );
}

/// Does merely QUERYING an external table leak it?
#[tokio::test(flavor = "multi_thread")]
async fn releases_lock_after_query_external_table() {
    let root = tempfile::tempdir().unwrap();
    seed_src_csv(root.path());
    let rt = builder(root.path()).build().await.unwrap();
    sql(
        &rt,
        "CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION 'src/'",
    )
    .await;
    sql(&rt, "SELECT count(*) FROM ext").await;
    drop(rt);
    let rt2 = builder(root.path()).build().await;
    assert!(
        rt2.is_ok(),
        "reopen after querying external table failed: {:?}",
        rt2.err()
    );
}
