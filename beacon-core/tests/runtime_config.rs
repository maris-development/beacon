//! Proves the runtime takes its configuration as an argument (rather than from a
//! process-global) and that two runtimes in one process honor their own config.

mod common;

use std::sync::Arc;

use beacon_core::query::Query;
use beacon_core::settings::SqlSettings;
use common::TestRuntime;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::local::LocalFileSystem;

/// Builds a runtime whose `sql.default_table` is `default_table`, on its own temp
/// root with its own (in-memory) tables store. Config is passed explicitly to the
/// builder, so nothing is read from a process-global.
async fn runtime_with_default_table(default_table: &str, tag: &str) -> TestRuntime {
    let default_table = default_table.to_string();
    common::runtime_with(tag, move |builder| {
        builder.with_sql_settings(SqlSettings {
            default_table,
            ..Default::default()
        })
    })
    .await
}

/// A JSON (non-SQL) query with no `from`, which the compiler resolves against the
/// runtime's configured `sql.default_table`. `QueryBody`'s fields are private, so
/// the query is built through its `Deserialize` impl.
fn json_query_without_from() -> Query {
    serde_json::from_str(r#"{"select": [{"column": "id"}]}"#).expect("a valid JSON query body")
}

/// Reads back the default table a runtime resolves a `from`-less JSON query against.
///
/// `Runtime` exposes no config getter — the setting is observed through behavior:
/// the table is never created, so planning fails with an error naming the exact
/// table the runtime resolved to.
async fn resolved_default_table_error(rt: &TestRuntime) -> String {
    match rt
        .runtime
        .run_query(json_query_without_from(), beacon_core::AuthIdentity::system())
        .await
    {
        Ok(_) => panic!("expected the (never-created) default table to be missing"),
        Err(error) => error.to_string(),
    }
}

/// Two runtimes built from different configs in the same process each reflect
/// their own configuration — confirming the config is owned per-runtime and not
/// read from a process-global.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_runtimes_honor_their_own_config() {
    let rt_alpha = runtime_with_default_table("alpha_table", "alpha").await;
    let rt_bravo = runtime_with_default_table("bravo_table", "bravo").await;

    let alpha = resolved_default_table_error(&rt_alpha).await;
    assert!(
        alpha.contains("alpha_table") && !alpha.contains("bravo_table"),
        "alpha runtime should resolve its own default table: {alpha}"
    );

    let bravo = resolved_default_table_error(&rt_bravo).await;
    assert!(
        bravo.contains("bravo_table") && !bravo.contains("alpha_table"),
        "bravo runtime should resolve its own default table: {bravo}"
    );

    // The first runtime is unaffected by the second's construction.
    let alpha_again = resolved_default_table_error(&rt_alpha).await;
    assert!(
        alpha_again.contains("alpha_table") && !alpha_again.contains("bravo_table"),
        "alpha runtime should still resolve its own default table: {alpha_again}"
    );
}

/// An embedder-supplied store and URL replace the datasets store wholesale: beacon
/// has no opinion about storage, and `datasets://` is only a default.
///
/// The store is injected under a non-default scheme and pointed at a directory that
/// is *not* `storage.datasets_dir`; the latter holds decoys at the same relative
/// paths with different row counts. Reading the injected store's rows — rather than
/// a decoy's, and rather than nothing — is what proves the configured URL and the
/// store registered under it agree.
///
/// Both a `CREATE EXTERNAL TABLE` and a crawler are exercised, because they reach the
/// store by different routes: the former through the configured URL at plan time, the
/// latter by resolving that URL against the session's registry. There must be exactly
/// one datasets store per runtime, so the two can never disagree about which files
/// exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_injected_store_replaces_the_datasets_store() {
    let injected_dir = tempfile::Builder::new()
        .prefix("beacon-core-test-injected-store-")
        .tempdir()
        .expect("create injected store root");
    common::write_file(
        &injected_dir.path().join("obs/a.csv"),
        "v,name\n1,a\n2,b\n3,c\n",
    );
    common::write_file(
        &injected_dir.path().join("crawled/a.csv"),
        "v,name\n1,a\n2,b\n3,c\n4,d\n",
    );

    let store =
        Arc::new(LocalFileSystem::new_with_prefix(injected_dir.path()).expect("local store"));
    let rt = common::runtime_with("injected-store", move |b| {
        b.with_default_store(
            ObjectStoreUrl::parse("injected://").expect("a valid URL"),
            store,
        )
    })
    .await;

    // Decoys under the storage config's datasets_dir, at the same relative paths but
    // with different row counts. A runtime still reading the store built from
    // StorageConfig would report 1 for both counts below.
    common::write_file(&rt.datasets_dir().join("obs/a.csv"), "v,name\n9,z\n");
    common::write_file(&rt.datasets_dir().join("crawled/a.csv"), "v,name\n9,z\n");

    rt.sql("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'obs/'")
        .await;
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM obs").await),
        3,
        "a relative LOCATION should resolve against the injected store"
    );

    rt.sql("CREATE CRAWLER c ON 'crawled/'").await;
    rt.sql("RUN CRAWLER c").await;
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM crawled").await),
        4,
        "the crawler should scan the injected store, not the one built from StorageConfig"
    );
}
