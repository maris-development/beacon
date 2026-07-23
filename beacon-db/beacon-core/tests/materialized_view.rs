//! End-to-end test for materialized views driven through the SQL endpoint:
//! CREATE -> query -> REFRESH -> DROP, plus a restart proving the persisted
//! result now lives in the redb tables store (`db://`, under the reserved
//! `__materialized__/` prefix) and reloads.

mod common;

use beacon_core::AuthIdentity;
use common::{total_rows, TestRuntime};
use datafusion::arrow::record_batch::RecordBatch;

async fn run(rt: &TestRuntime, sql: &str) -> Vec<RecordBatch> {
    rt.sql_as(sql, AuthIdentity::system()).await
}

#[tokio::test(flavor = "multi_thread")]
async fn materialized_view_create_query_refresh_drop_and_restart() {
    // Materialized-view data is persisted in the tables store, so use a
    // persistent single-file redb store to prove it survives a restart.
    let rt = common::restartable_runtime("materialized_view", |b| b).await;

    // CREATE persists the query result as Parquet under `__materialized__/` in the
    // tables store and registers a provider that reads it back.
    run(&rt, "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a, 2 AS b").await;

    // QUERY serves the persisted result rather than recomputing.
    let batches = run(&rt, "SELECT * FROM mv").await;
    assert_eq!(total_rows(&batches), 1);
    assert_eq!(batches[0].num_columns(), 2);

    // REFRESH recomputes and atomically swaps in a fresh versioned file.
    run(&rt, "REFRESH mv").await;
    assert_eq!(total_rows(&run(&rt, "SELECT * FROM mv").await), 1);

    // REFRESH of a plain view is rejected clearly (only MVs and external tables
    // are refreshable).
    run(&rt, "CREATE VIEW rv AS SELECT 1 AS a").await;
    let err = rt
        .try_sql_as("REFRESH rv", AuthIdentity::system())
        .await
        .expect_err("refresh of a regular view should fail");
    assert!(
        err.to_string().contains("is not a materialized view"),
        "unexpected error: {err}"
    );

    // A restart reloads the catalog from the persistent redb store; the MV must
    // rebuild from its persisted Parquet and still return the same data — proof
    // the data lives in `db://`, not the ephemeral datasets store.
    let rt = rt.restart().await;
    let batches = run(&rt, "SELECT * FROM mv").await;
    assert_eq!(total_rows(&batches), 1);
    assert_eq!(batches[0].num_columns(), 2);

    // DROP removes the view and reclaims its `__materialized__/mv` storage; it is
    // no longer queryable.
    run(&rt, "DROP TABLE mv").await;
    assert!(
        rt.try_sql_as("SELECT * FROM mv", AuthIdentity::system())
            .await
            .is_err(),
        "materialized view should not be queryable after drop"
    );

    // The drop is persisted, so it stays gone after a restart.
    let rt = rt.restart().await;
    assert!(
        rt.try_sql_as("SELECT * FROM mv", AuthIdentity::system())
            .await
            .is_err(),
        "dropped materialized view should stay gone after restart"
    );
}
