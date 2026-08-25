//! End-to-end test for Lance-backed managed tables (the default engine) driven
//! through the SQL endpoint: CREATE, INSERT, UPDATE and DELETE — exercising the
//! native row-mutation path (predicate/SET unparsed and applied via Lance) — plus
//! `COMPACT TABLE`.

mod common;

use common::TestRuntime;
use datafusion::arrow::array::{Int64Array, StringArray, UInt64Array};
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

/// Read a named `UInt64` column out of a single-row report batch (`COMPACT TABLE`).
fn report_value(batches: &[RecordBatch], column: &str) -> u64 {
    let batch = &batches[0];
    let index = batch
        .schema()
        .index_of(column)
        .unwrap_or_else(|_| panic!("report should have a '{column}' column"));
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("report columns should be UInt64")
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

/// `COMPACT TABLE` merges the fragments a series of small inserts left behind and
/// materializes the deletions, without changing what the table contains. The
/// cleanup pass is what frees the disk space, and only for versions older than
/// the retention window — so the default run reclaims nothing and
/// `cleanup_older_than '0s'` reclaims immediately.
#[tokio::test(flavor = "multi_thread")]
async fn lance_compact_table_merges_fragments_and_reclaims_versions() {
    let rt = common::runtime_with("lance-compact", |b| {
        b.with_db_path(std::env::temp_dir().join(format!(
            "beacon-lance-compact-{}-{}.db",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        )))
    })
    .await;

    let table = format!("lance_compact_{}", std::process::id());
    let _ = rt
        .try_sql_as(
            &format!("DROP TABLE IF EXISTS {table}"),
            beacon_core::AuthIdentity::system(),
        )
        .await;

    run(&rt, &format!("CREATE TABLE {table} (id BIGINT, name VARCHAR)")).await;
    // Three commits of two rows each, so three fragments well under the target
    // size. The deleted row leaves its fragment alive (a fragment whose rows are
    // *all* deleted is dropped by the DELETE itself), so compaction is what
    // materializes the deletion.
    for pair in 0..3 {
        let (first, second) = (pair * 2 + 1, pair * 2 + 2);
        run(
            &rt,
            &format!(
                "INSERT INTO {table} VALUES ({first}, 'row-{first}'), ({second}, 'row-{second}')"
            ),
        )
        .await;
    }
    run(&rt, &format!("DELETE FROM {table} WHERE id = 1")).await;

    let report = run(&rt, &format!("COMPACT TABLE {table}")).await;
    assert_eq!(
        report_value(&report, "fragments_removed"),
        3,
        "every two-row fragment is under the target size, so all three are merged"
    );
    assert_eq!(
        report_value(&report, "fragments_added"),
        1,
        "the five surviving rows fit in a single fragment"
    );
    assert_eq!(
        report_value(&report, "versions_removed"),
        0,
        "the default 7-day retention window keeps every version just written"
    );
    assert_eq!(
        scalar_count(&run(&rt, &format!("SELECT count(*) FROM {table}")).await),
        5,
        "compaction must not change the rows"
    );
    assert_eq!(
        scalar_string(&run(&rt, &format!("SELECT name FROM {table} WHERE id = 3")).await),
        "row-3",
        "values survive the fragment rewrite"
    );

    // Nothing left to merge, but the superseded versions are now removable.
    let report = run(
        &rt,
        &format!("COMPACT TABLE {table} WITH ('cleanup_older_than' '0s')"),
    )
    .await;
    assert_eq!(
        report_value(&report, "fragments_removed"),
        0,
        "a single target-sized fragment is not a compaction candidate"
    );
    assert!(
        report_value(&report, "versions_removed") > 0,
        "with a zero retention window the superseded versions go"
    );
    assert!(
        report_value(&report, "bytes_removed") > 0,
        "removing those versions frees the files only they referenced"
    );
    assert_eq!(
        scalar_count(&run(&rt, &format!("SELECT count(*) FROM {table}")).await),
        5,
        "the surviving version is untouched by cleanup"
    );

    // A mistyped option must fail loudly rather than look like it took effect.
    let error = rt
        .try_sql_as(
            &format!("COMPACT TABLE {table} WITH ('target_rows' '10')"),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .expect_err("an unknown option should be rejected");
    assert!(
        error.to_string().contains("target_rows_per_fragment"),
        "the error should name the accepted option: {error}"
    );

    run(&rt, &format!("DROP TABLE {table}")).await;
}
