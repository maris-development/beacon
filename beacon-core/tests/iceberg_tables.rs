//! End-to-end test for Iceberg-backed managed tables driven through the SQL
//! endpoint (`Runtime::run_sql`): CREATE, INSERT, SELECT, CTAS and DROP.

use beacon_core::runtime::Runtime;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

/// Run SQL as a super-user (DDL/DML allowed) and collect the result batches.
async fn run(runtime: &Runtime, sql: &str) -> Vec<RecordBatch> {
    runtime
        .run_query(beacon_core::query::Query::sql(sql.to_string()), beacon_core::AuthIdentity::system())
        .await
        .unwrap_or_else(|error| panic!("SQL failed to plan/execute: {sql}\n{error}"))
        .into_record_stream()
        .unwrap_or_else(|error| panic!("expected a streamed result: {sql}\n{error}"))
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

/// First-column single-row Utf8 value.
fn scalar_string(batches: &[RecordBatch]) -> String {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column should be Utf8")
        .value(0)
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_create_insert_select_ctas_drop() {
    let runtime = Runtime::new(std::sync::Arc::new(beacon_config::Config::load().unwrap())).await.expect("runtime should boot");

    // The default managed-table engine is Lance; this test exercises the Iceberg
    // path, so select it explicitly for this session.
    run(&runtime, "SET beacon.table_engine = 'iceberg'").await;

    // Unique names so reruns/parallel suites do not collide in the shared
    // warehouse, and clean any leftovers from a previous aborted run.
    let table = format!("ice_e2e_{}", std::process::id());
    let copy = format!("{table}_copy");
    let _ = runtime.run_query(beacon_core::query::Query::sql(format!("DROP TABLE IF EXISTS {table}")), beacon_core::AuthIdentity::system()).await;
    let _ = runtime.run_query(beacon_core::query::Query::sql(format!("DROP TABLE IF EXISTS {copy}")), beacon_core::AuthIdentity::system()).await;

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

    // UPDATE WHERE: only the matching row changes; the other is untouched.
    run(&runtime, &format!("UPDATE {table} SET name = 'Z' WHERE id = 1")).await;
    let updated = scalar_string(&run(&runtime, &format!("SELECT name FROM {table} WHERE id = 1")).await);
    assert_eq!(updated, "Z", "UPDATE WHERE id = 1 should set name to 'Z'");
    let untouched = scalar_string(&run(&runtime, &format!("SELECT name FROM {table} WHERE id = 2")).await);
    assert_eq!(untouched, "b", "non-matching row should be unchanged");
    let row_count = scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await);
    assert_eq!(row_count, 2, "UPDATE must not change the row count");

    // UPDATE all rows (no WHERE).
    run(&runtime, &format!("UPDATE {table} SET name = 'all'")).await;
    let distinct_names =
        scalar_count(&run(&runtime, &format!("SELECT count(DISTINCT name) FROM {table}")).await);
    assert_eq!(distinct_names, 1, "UPDATE without WHERE should set every row");
    let any_name = scalar_string(&run(&runtime, &format!("SELECT name FROM {table} LIMIT 1")).await);
    assert_eq!(any_name, "all", "every row should have the updated value");

    // ALTER TABLE schema evolution (on the independent `copy` table, which still
    // holds the original rows (1,'a'),(2,'b')).
    // ADD COLUMN: existing rows read NULL.
    run(&runtime, &format!("ALTER TABLE {copy} ADD COLUMN age INT")).await;
    let non_null_age = scalar_count(&run(&runtime, &format!("SELECT count(age) FROM {copy}")).await);
    assert_eq!(non_null_age, 0, "existing rows must read NULL for a new column");
    // New rows can populate the new column.
    run(&runtime, &format!("INSERT INTO {copy} VALUES (3, 'c', 30)")).await;
    // ALTER COLUMN TYPE: int -> bigint (a safe promotion).
    run(&runtime, &format!("ALTER TABLE {copy} ALTER COLUMN age TYPE BIGINT")).await;
    let age = scalar_count(&run(&runtime, &format!("SELECT age FROM {copy} WHERE id = 3")).await);
    assert_eq!(age, 30, "inserted value survives the type promotion");
    // RENAME COLUMN: values preserved under the new name.
    run(&runtime, &format!("ALTER TABLE {copy} RENAME COLUMN name TO label")).await;
    let label = scalar_string(&run(&runtime, &format!("SELECT label FROM {copy} WHERE id = 1")).await);
    assert_eq!(label, "a", "renamed column keeps its values");
    // DROP COLUMN.
    run(&runtime, &format!("ALTER TABLE {copy} DROP COLUMN label")).await;
    let cols = run(&runtime, &format!("SELECT * FROM {copy} ORDER BY id")).await;
    let schema = cols[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "age"], "dropped column should be gone");
    // A narrowing type change is rejected.
    let narrow = runtime
        .run_query(beacon_core::query::Query::sql(format!("ALTER TABLE {copy} ALTER COLUMN age TYPE INT")), beacon_core::AuthIdentity::system())
        .await;
    assert!(narrow.is_err(), "narrowing type change should be rejected");

    // Restore a known state for the DELETE cases below.
    run(&runtime, &format!("UPDATE {table} SET name = 'a' WHERE id = 1")).await;
    run(&runtime, &format!("UPDATE {table} SET name = 'b' WHERE id = 2")).await;

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
        .run_query(beacon_core::query::Query::sql(format!("DELETE FROM {view_name} WHERE id = 1")), beacon_core::AuthIdentity::system())
        .await;
    assert!(
        delete_view.is_err(),
        "DELETE on a non-Iceberg table should error"
    );
    let update_view = runtime
        .run_query(beacon_core::query::Query::sql(format!("UPDATE {view_name} SET id = 2 WHERE id = 1")), beacon_core::AuthIdentity::system())
        .await;
    assert!(
        update_view.is_err(),
        "UPDATE on a non-Iceberg table should error"
    );
    let alter_view = runtime
        .run_query(beacon_core::query::Query::sql(format!("ALTER TABLE {view_name} ADD COLUMN x INT")), beacon_core::AuthIdentity::system())
        .await;
    assert!(
        alter_view.is_err(),
        "ALTER on a non-Iceberg table should error"
    );
    run(&runtime, &format!("DROP TABLE {view_name}")).await;

    // DROP both tables.
    run(&runtime, &format!("DROP TABLE {table}")).await;
    run(&runtime, &format!("DROP TABLE {copy}")).await;

    // Dropped tables should no longer be queryable.
    let err = runtime
        .run_query(beacon_core::query::Query::sql(format!("SELECT count(*) FROM {table}")), beacon_core::AuthIdentity::system())
        .await;
    assert!(err.is_err(), "querying a dropped table should error");
}
