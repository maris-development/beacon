//! The embedder owns storage: the whole `ObjectStores` set can be injected, and the
//! URLs the datasets / tables / tmp stores are registered under are builder-provided
//! (defaulting to `datasets://` / `db://` / `tmp://`).
//!
//! Each test proves a store's *registration* URL agrees with the URL its consumers
//! read back — the "one store per URL per runtime" invariant — by round-tripping data
//! through it (and, where persistence is involved, across a restart, which also proves
//! the injected/relabelled redb tables store releases its file lock on drop).

mod common;

use std::path::Path;

use arrow::record_batch::RecordBatch;
use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use beacon_object_storage::{ObjectStores, StorageConfig};
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::TryStreamExt;
use serde_json::json;
use tempfile::TempDir;

/// A persistent storage layout (redb tables store on disk, so a restart is meaningful).
fn persistent_storage(root: &Path) -> StorageConfig {
    let mut storage = common::storage_in(root);
    storage.db_path = Some(root.join("beacon.db"));
    for dir in [&storage.data_dir, &storage.datasets_dir, &storage.tmp_dir] {
        std::fs::create_dir_all(dir).expect("create storage dir");
    }
    storage
}

/// Build a runtime over `storage`, applying `customize` to the builder.
async fn build_with(
    storage: &StorageConfig,
    customize: impl FnOnce(RuntimeBuilder) -> RuntimeBuilder,
) -> Runtime {
    let builder = RuntimeBuilder::new()
        .with_storage(storage.clone())
        .with_tmp_dir_path(storage.tmp_dir.clone());
    customize(builder)
        .build()
        .await
        .expect("runtime should build")
}

/// Run SQL as the system super-user and collect the result batches.
async fn run(rt: &Runtime, sql: &str) -> Vec<RecordBatch> {
    rt.run_query(Query::sql(sql.to_string()), AuthIdentity::system())
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
        .into_record_stream()
        .expect("record stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect rows")
}

/// Injecting the whole `ObjectStores` set wires storage end to end: a managed table
/// created through the injected stores round-trips, survives a restart, and — because
/// reopening the same redb file succeeds — proves the injected tables store released
/// its exclusive lock when the first runtime dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn injected_object_stores_round_trip_and_release_the_lock() {
    let root = TempDir::new().expect("temp root");
    let storage = persistent_storage(root.path());

    // First runtime: build the stores ourselves and hand them to the builder whole.
    let stores = ObjectStores::new(&storage)
        .await
        .expect("build object stores");
    let rt = build_with(&storage, |b| b.with_object_stores(stores)).await;

    run(&rt, "CREATE TABLE t (id BIGINT, name VARCHAR)").await;
    run(&rt, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
    assert_eq!(
        common::scalar_i64(&run(&rt, "SELECT count(*) FROM t").await),
        3,
        "the injected stores should back a working managed table"
    );

    // Drop the runtime; the injected redb tables store must release its file lock.
    drop(rt);

    // Reopening the same file only succeeds if the lock was released; the table must
    // have persisted into the injected tables store.
    let stores = ObjectStores::new(&storage)
        .await
        .expect("reopen the tables store — proves the injected store released its lock");
    let rt = build_with(&storage, |b| b.with_object_stores(stores)).await;
    assert_eq!(
        common::scalar_i64(&run(&rt, "SELECT count(*) FROM t").await),
        3,
        "the managed table should persist across a restart in the injected tables store"
    );
}

/// A non-default *tables* URL is threaded consistently: `register_object_stores`, the
/// schema provider and `init_tables` all use it, so a managed table created under it
/// round-trips and reloads across a restart. If the registration URL and the read-back
/// URL diverged, the table would be written to one store and read from another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_default_tables_store_url_is_threaded() {
    let custom_tables = ObjectStoreUrl::parse("customdb://").expect("a valid URL");

    let rt = common::restartable_runtime("custom-tables-url", move |b| {
        b.with_tables_store_url(custom_tables.clone())
    })
    .await;

    rt.sql_as("CREATE TABLE t (id BIGINT, name VARCHAR)", AuthIdentity::system())
        .await;
    rt.sql_as(
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        AuthIdentity::system(),
    )
    .await;
    assert_eq!(
        common::scalar_i64(
            &rt.sql_as("SELECT count(*) FROM t", AuthIdentity::system())
                .await
        ),
        3,
        "a managed table under the custom tables URL should round-trip"
    );

    // Restart re-applies the same custom tables URL; the table must reload from it.
    let rt = rt.restart().await;
    assert_eq!(
        common::scalar_i64(
            &rt.sql_as("SELECT count(*) FROM t", AuthIdentity::system())
                .await
        ),
        3,
        "the table should reload from the custom tables store URL after a restart"
    );
}

/// A non-default *tmp* URL is threaded through the query-output path: the COPY target
/// is built from the tmp URL read off the session, and the object-store writer resolves
/// it against the store registered under that same URL. A mismatch would fail to
/// resolve the store, so a non-empty output file proves they agree.
#[tokio::test(flavor = "multi_thread")]
async fn non_default_tmp_store_url_backs_query_output() {
    let custom_tmp = ObjectStoreUrl::parse("customtmp://").expect("a valid URL");
    let rt = common::runtime_with("custom-tmp-url", move |b| {
        b.with_tmp_store_url(custom_tmp.clone())
    })
    .await;

    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1), (2)").await;

    let mut query = Query::sql("SELECT a FROM t".to_string());
    query.output = Some(serde_json::from_value(json!({ "format": "csv" })).expect("valid output"));

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("query with an output format should run under a custom tmp URL");

    match result.query_output {
        QueryOutput::File(file) => {
            assert!(
                file.size().expect("output file size") > 0,
                "the output file should contain rows, proving the custom tmp URL resolved its store"
            );
        }
        QueryOutput::Stream(_) => panic!("an output format should yield a file, not a stream"),
    }
}
