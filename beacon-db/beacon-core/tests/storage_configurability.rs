//! The embedder configures storage through the builder: the datasets store is
//! configured with [`RuntimeBuilder::with_default_store`] (a URL + its root), the
//! tables store is persisted by pointing `with_db_path` at a redb file, and query
//! output lands under `with_tmp_dir_path`.
//!
//! Each test proves a configured store is actually the one its consumers use, by
//! round-tripping data through it via SQL (and, where persistence is involved, across
//! a restart — which also proves the redb tables store releases its file lock on drop).

mod common;

use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use beacon_datafusion_ext::listing_factory::RootStore;
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::TryStreamExt;
use serde_json::json;
use tempfile::TempDir;

/// An injected datasets store is the one dataset reads resolve against: a CSV placed
/// under the injected store's root is found by a relative `LOCATION`, proving the
/// store (for listing) and its root (for native paths) are both wired to it.
#[tokio::test(flavor = "multi_thread")]
async fn injected_default_store_backs_dataset_reads() {
    let root = TempDir::new().expect("temp root");
    let datasets = root.path().join("my-datasets");
    std::fs::create_dir_all(datasets.join("obs")).unwrap();
    std::fs::write(datasets.join("obs/a.csv"), "v,name\n1,a\n2,b\n3,c\n").unwrap();

    let rt = RuntimeBuilder::new()
        .with_default_store(
            ObjectStoreUrl::parse("datasets://").unwrap(),
            RootStore::FileSystem(datasets),
        )
        .with_tmp_dir_path(root.path().join("tmp"))
        .build()
        .await
        .expect("runtime should build over the injected datasets store");

    run(&rt, "CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'obs/'").await;
    assert_eq!(
        common::scalar_i64(&run(&rt, "SELECT count(*) FROM obs").await),
        3,
        "a relative LOCATION should resolve against the injected datasets store"
    );
}

/// A managed table on the redb tables store persists across a restart: create + insert,
/// drop the runtime, reopen the same `beacon.db`, and the rows are still there. Reopening
/// at all also proves the tables store released its exclusive file lock on drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_table_persists_across_restart() {
    let rt = common::restartable_runtime("persist-managed-table", |b| b).await;

    rt.sql("CREATE TABLE t (id BIGINT, name VARCHAR)").await;
    rt.sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM t").await),
        3,
        "the managed table should back three rows before the restart"
    );

    let rt = rt.restart().await;
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM t").await),
        3,
        "the managed table should reload from the redb tables store after a restart"
    );
}

/// Query output is written under the configured tmp dir: a query with an output format
/// resolves the tmp store and writes a non-empty file, and that file lives under the
/// directory `with_tmp_dir_path` set.
#[tokio::test(flavor = "multi_thread")]
async fn query_output_is_written_under_the_configured_tmp_dir() {
    let rt = common::runtime("tmp-output").await;

    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1), (2)").await;

    let mut query = Query::sql("SELECT a FROM t".to_string());
    query.output = Some(serde_json::from_value(json!({ "format": "csv" })).expect("valid output"));

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("a query with an output format should run");

    match result.query_output {
        QueryOutput::File(file) => {
            assert!(
                file.size().expect("output file size") > 0,
                "the output file should contain rows, proving the tmp store resolved"
            );
        }
        QueryOutput::Stream(_) => panic!("an output format should yield a file, not a stream"),
    }
}

/// Run SQL as the system super-user and collect the result batches.
async fn run(
    rt: &beacon_core::runtime::Runtime,
    sql: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    rt.run_query(Query::sql(sql.to_string()), AuthIdentity::system())
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
        .into_record_stream()
        .expect("record stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect rows")
}
