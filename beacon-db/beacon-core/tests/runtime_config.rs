//! Proves the runtime takes its configuration as an argument (rather than from a
//! process-global) and that two runtimes in one process honor their own config.

mod common;

use beacon_core::query::Query;
use beacon_core::settings::SqlSettings;
use beacon_datafusion_ext::listing_factory::RootStore;
use common::TestRuntime;
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::TryStreamExt;

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

/// Reads back how many rows a runtime's `from`-less JSON query returns.
///
/// `Runtime` exposes no config getter, so the setting is observed through
/// behavior: each runtime holds a distinct number of rows under its own
/// configured default-table name, and the row count identifies the table the
/// runtime resolved to.
async fn rows_from_default_table(rt: &TestRuntime) -> usize {
    let batches = rt
        .runtime
        .run_query(json_query_without_from(), beacon_core::AuthIdentity::system())
        .await
        .expect("a from-less query should resolve the configured default table")
        .into_record_stream()
        .expect("the result should be a record stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("the stream should run");
    common::total_rows(&batches)
}

/// Fills a runtime's configured default table with `row_count` rows of `id`.
/// The startup stand-in holds the name, so drop it before the create.
async fn fill_default_table(rt: &TestRuntime, table: &str, row_count: usize) {
    rt.sql(&format!("DROP TABLE {table}")).await;
    rt.sql(&format!("CREATE TABLE {table} (id BIGINT)")).await;
    let values = (1..=row_count)
        .map(|row| format!("({row})"))
        .collect::<Vec<_>>()
        .join(", ");
    rt.sql(&format!("INSERT INTO {table} VALUES {values}"))
        .await;
}

/// Two runtimes built from different configs in the same process each reflect
/// their own configuration — confirming the config is owned per-runtime and not
/// read from a process-global.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_runtimes_honor_their_own_config() {
    let rt_alpha = runtime_with_default_table("alpha_table", "alpha").await;
    let rt_bravo = runtime_with_default_table("bravo_table", "bravo").await;

    // Distinct row counts, so a `from`-less query names the table it read.
    fill_default_table(&rt_alpha, "alpha_table", 1).await;
    fill_default_table(&rt_bravo, "bravo_table", 2).await;

    assert_eq!(
        rows_from_default_table(&rt_alpha).await,
        1,
        "alpha runtime should resolve its own default table"
    );
    assert_eq!(
        rows_from_default_table(&rt_bravo).await,
        2,
        "bravo runtime should resolve its own default table"
    );

    // The first runtime is unaffected by the second's construction.
    assert_eq!(
        rows_from_default_table(&rt_alpha).await,
        1,
        "alpha runtime should still resolve its own default table"
    );
}

/// An embedder-supplied store and URL replace the datasets store wholesale: beacon
/// has no opinion about storage, and `datasets://` is only a default.
///
/// The store is injected under a non-default scheme and pointed at a directory that
/// is *not* the harness's datasets dir; the latter holds decoys at the same relative
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

    let injected_root = injected_dir.path().to_path_buf();
    let rt = common::runtime_with("injected-store", move |b| {
        b.with_default_store(
            ObjectStoreUrl::parse("injected://").expect("a valid URL"),
            RootStore::FileSystem(injected_root),
        )
    })
    .await;

    // Decoys under the harness's default datasets dir, at the same relative paths but
    // with different row counts. A runtime still reading the harness's default store
    // (rather than the injected one) would report 1 for both counts below.
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
        "the crawler should scan the injected store, not the harness's default store"
    );
}
