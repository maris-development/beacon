//! End-to-end test for Lance-backed managed tables (the default engine) driven
//! through the SQL endpoint: CREATE, INSERT, UPDATE and DELETE — exercising the
//! native row-mutation path (predicate/SET unparsed and applied via Lance).

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

fn scalar_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column should be Int64")
        .value(0)
}

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
async fn lance_create_insert_update_delete() {
    let runtime = Runtime::new_with_in_memory_auth(std::sync::Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");

    // Lance is the only managed-table engine, so no engine selection is needed.
    let table = format!("lance_e2e_{}", std::process::id());
    let _ = runtime
        .run_query(
            beacon_core::query::Query::sql(format!("DROP TABLE IF EXISTS {table}")),
            beacon_core::AuthIdentity::system(),
        )
        .await;

    run(&runtime, &format!("CREATE TABLE {table} (id BIGINT, name VARCHAR)")).await;
    run(&runtime, &format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")).await;
    assert_eq!(
        scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await),
        3
    );

    // UPDATE WHERE: only the matching row changes; the others are untouched.
    run(&runtime, &format!("UPDATE {table} SET name = 'Z' WHERE id = 2")).await;
    assert_eq!(
        scalar_string(&run(&runtime, &format!("SELECT name FROM {table} WHERE id = 2")).await),
        "Z"
    );
    assert_eq!(
        scalar_string(&run(&runtime, &format!("SELECT name FROM {table} WHERE id = 1")).await),
        "a",
        "non-matching row should be unchanged"
    );
    assert_eq!(
        scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await),
        3,
        "UPDATE must not change the row count"
    );

    // UPDATE all rows (no WHERE).
    run(&runtime, &format!("UPDATE {table} SET name = 'all'")).await;
    assert_eq!(
        scalar_count(
            &run(&runtime, &format!("SELECT count(DISTINCT name) FROM {table}")).await
        ),
        1,
        "UPDATE without WHERE should set every row"
    );

    // DELETE WHERE: remove one row, the other survives.
    run(&runtime, &format!("DELETE FROM {table} WHERE id = 1")).await;
    assert_eq!(
        scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await),
        2,
        "DELETE WHERE id = 1 should remove one row"
    );
    assert_eq!(
        scalar_count(
            &run(&runtime, &format!("SELECT id FROM {table} ORDER BY id LIMIT 1")).await
        ),
        2,
        "the smallest surviving id should be 2"
    );

    // DELETE all rows.
    run(&runtime, &format!("DELETE FROM {table}")).await;
    assert_eq!(
        scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await),
        0,
        "DELETE without WHERE should empty the table"
    );

    run(&runtime, &format!("DROP TABLE {table}")).await;
}
