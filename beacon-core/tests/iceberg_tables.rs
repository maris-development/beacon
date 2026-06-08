//! End-to-end test for Iceberg-backed managed tables driven through the SQL
//! endpoint (`Runtime::run_sql`): CREATE, INSERT, SELECT, CTAS and DROP.

use beacon_core::runtime::Runtime;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

/// Run SQL as a super-user (DDL/DML allowed) and collect the result batches.
async fn run(runtime: &Runtime, sql: &str) -> Vec<RecordBatch> {
    let stream = runtime
        .run_sql(sql.to_string(), true)
        .await
        .unwrap_or_else(|error| panic!("SQL failed to plan/execute: {sql}\n{error}"));
    stream
        .try_collect()
        .await
        .unwrap_or_else(|error| panic!("SQL stream failed: {sql}\n{error}"))
}

/// `SELECT count(*)`-style helper returning the single i64 value.
fn scalar_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column should be Int64")
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_create_insert_select_ctas_drop() {
    let runtime = Runtime::new().await.expect("runtime should boot");

    // Unique names so reruns/parallel suites do not collide in the shared
    // warehouse, and clean any leftovers from a previous aborted run.
    let table = format!("ice_e2e_{}", std::process::id());
    let copy = format!("{table}_copy");
    let _ = runtime.run_sql(format!("DROP TABLE IF EXISTS {table}"), true).await;
    let _ = runtime.run_sql(format!("DROP TABLE IF EXISTS {copy}"), true).await;

    // CREATE + INSERT + SELECT.
    run(&runtime, &format!("CREATE TABLE {table} (id BIGINT, name VARCHAR)")).await;
    run(&runtime, &format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")).await;
    let count = scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await);
    assert_eq!(count, 2, "two inserted rows should be visible");

    // The warehouse must live in the datasets store's internal area
    // (`<datasets>/__beacon__/iceberg/...`), not a separate folder.
    let warehouse_table_dir = beacon_config::DATASETS_DIR_PATH
        .join("__beacon__")
        .join("iceberg")
        .join("beacon")
        .join(&table);
    assert!(
        warehouse_table_dir.exists(),
        "Iceberg table should be stored under the datasets internal prefix: {}",
        warehouse_table_dir.display()
    );

    // CREATE TABLE AS SELECT.
    run(&runtime, &format!("CREATE TABLE {copy} AS SELECT * FROM {table}")).await;
    let copy_count = scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {copy}")).await);
    assert_eq!(copy_count, 2, "CTAS should copy all rows");

    // DELETE WHERE: remove one row, the other survives unchanged.
    run(&runtime, &format!("DELETE FROM {table} WHERE id = 1")).await;
    let after_delete = scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await);
    assert_eq!(after_delete, 1, "DELETE WHERE id = 1 should remove one row");
    let surviving_id = scalar_count(&run(&runtime, &format!("SELECT id FROM {table}")).await);
    assert_eq!(surviving_id, 2, "the surviving row should be id = 2");

    // DELETE all rows.
    run(&runtime, &format!("DELETE FROM {table}")).await;
    let after_delete_all = scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await);
    assert_eq!(after_delete_all, 0, "DELETE without WHERE should empty the table");

    // DELETE on a non-Iceberg relation must be rejected.
    let view_name = format!("{table}_view");
    run(&runtime, &format!("CREATE VIEW {view_name} AS SELECT 1 AS id")).await;
    let delete_view = runtime
        .run_sql(format!("DELETE FROM {view_name} WHERE id = 1"), true)
        .await;
    assert!(
        delete_view.is_err(),
        "DELETE on a non-Iceberg table should error"
    );
    run(&runtime, &format!("DROP TABLE {view_name}")).await;

    // DROP both tables.
    run(&runtime, &format!("DROP TABLE {table}")).await;
    run(&runtime, &format!("DROP TABLE {copy}")).await;

    // Dropped tables should no longer be queryable.
    let err = runtime
        .run_sql(format!("SELECT count(*) FROM {table}"), true)
        .await;
    assert!(err.is_err(), "querying a dropped table should error");
}
