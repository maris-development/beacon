//! End-to-end test for Lance-backed managed tables (the default engine) driven
//! through the SQL endpoint: CREATE, INSERT, UPDATE and DELETE — exercising the
//! native row-mutation path (predicate/SET unparsed and applied via Lance).

mod common;

use common::TestRuntime;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;

/// Run SQL as a super-user (DDL/DML allowed) and collect the result batches.
async fn run(rt: &TestRuntime, sql: &str) -> Vec<RecordBatch> {
    rt.sql_as(sql, beacon_core::AuthIdentity::system()).await
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
    // Managed Lance data lives in the tables store (`db://`), so this runtime gets a
    // persistent single-file redb store rather than the default in-memory one.
    let rt = common::runtime_with("lance", |b| {
        b.with_db_path(std::env::temp_dir().join(format!(
            "beacon-lance-e2e-{}-{}.db",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        )))
    })
    .await;

    // Lance is the only managed-table engine, so no engine selection is needed.
    let table = format!("lance_e2e_{}", std::process::id());
    let _ = rt
        .try_sql_as(
            &format!("DROP TABLE IF EXISTS {table}"),
            beacon_core::AuthIdentity::system(),
        )
        .await;

    run(&rt, &format!("CREATE TABLE {table} (id BIGINT, name VARCHAR)")).await;
    run(
        &rt,
        &format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
    )
    .await;
    assert_eq!(
        scalar_count(&run(&rt, &format!("SELECT count(*) FROM {table}")).await),
        3
    );

    // UPDATE WHERE: only the matching row changes; the others are untouched.
    run(&rt, &format!("UPDATE {table} SET name = 'Z' WHERE id = 2")).await;
    assert_eq!(
        scalar_string(&run(&rt, &format!("SELECT name FROM {table} WHERE id = 2")).await),
        "Z"
    );
    assert_eq!(
        scalar_string(&run(&rt, &format!("SELECT name FROM {table} WHERE id = 1")).await),
        "a",
        "non-matching row should be unchanged"
    );
    assert_eq!(
        scalar_count(&run(&rt, &format!("SELECT count(*) FROM {table}")).await),
        3,
        "UPDATE must not change the row count"
    );

    // UPDATE all rows (no WHERE).
    run(&rt, &format!("UPDATE {table} SET name = 'all'")).await;
    assert_eq!(
        scalar_count(&run(&rt, &format!("SELECT count(DISTINCT name) FROM {table}")).await),
        1,
        "UPDATE without WHERE should set every row"
    );

    // DELETE WHERE: remove one row, the other survives.
    run(&rt, &format!("DELETE FROM {table} WHERE id = 1")).await;
    assert_eq!(
        scalar_count(&run(&rt, &format!("SELECT count(*) FROM {table}")).await),
        2,
        "DELETE WHERE id = 1 should remove one row"
    );
    assert_eq!(
        scalar_count(&run(&rt, &format!("SELECT id FROM {table} ORDER BY id LIMIT 1")).await),
        2,
        "the smallest surviving id should be 2"
    );

    // DELETE all rows.
    run(&rt, &format!("DELETE FROM {table}")).await;
    assert_eq!(
        scalar_count(&run(&rt, &format!("SELECT count(*) FROM {table}")).await),
        0,
        "DELETE without WHERE should empty the table"
    );

    run(&rt, &format!("DROP TABLE {table}")).await;
}
