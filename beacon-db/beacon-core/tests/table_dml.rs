//! Managed-table DML beyond the basic Lance lifecycle: CTAS and
//! `INSERT INTO ... SELECT` over multi-batch inputs, `ALTER TABLE ADD COLUMN`,
//! and string-predicate `UPDATE`/`DELETE`.
//!
//! The multi-batch and string-predicate cases are regression tests: CTAS/INSERT
//! used to truncate to the first record batch, string predicates used to
//! mismatch on Utf8View, and `ALTER TABLE ... ADD COLUMN <VARCHAR>` used to fail
//! outright.

mod common;

use common::{runtime, scalar_i64, scalar_string, write_file, TestRuntime};

/// Writes a CSV with `rows` data rows (`v` = 0..rows) — large enough that the
/// scan yields multiple record batches at the default 8192 batch size.
fn write_big_csv(rt: &TestRuntime, rel: &str, rows: usize) {
    let mut contents = String::from("v\n");
    for i in 0..rows {
        contents.push_str(&format!("{i}\n"));
    }
    write_file(&rt.datasets_dir().join(rel), &contents);
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_and_insert_select_preserve_every_batch() {
    let rt = runtime("ctas-multibatch").await;
    const ROWS: usize = 20_000;
    write_big_csv(&rt, "big.csv", ROWS);

    rt.sql("CREATE TABLE big AS SELECT * FROM read_csv('big.csv')")
        .await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM big").await),
        ROWS as i64,
        "CTAS must materialize every input batch, not just the first"
    );
    // Values, not just counts: the sum of 0..N is a fingerprint of all rows.
    let expected_sum = (ROWS as i64 - 1) * ROWS as i64 / 2;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT sum(v) FROM big").await),
        expected_sum
    );

    rt.sql("INSERT INTO big SELECT * FROM read_csv('big.csv')")
        .await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM big").await),
        2 * ROWS as i64,
        "INSERT ... SELECT must append every input batch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_table_add_varchar_column() {
    let rt = runtime("alter-add-varchar").await;

    rt.sql("CREATE TABLE notes (id BIGINT)").await;
    rt.sql("INSERT INTO notes VALUES (1), (2)").await;

    rt.sql("ALTER TABLE notes ADD COLUMN note VARCHAR").await;

    // Existing rows read back NULL for the new column.
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM notes WHERE note IS NULL").await),
        2,
        "existing rows should have NULL in the added column"
    );

    // The new column is writable.
    rt.sql("UPDATE notes SET note = 'first' WHERE id = 1").await;
    assert_eq!(
        scalar_string(&rt.sql("SELECT note FROM notes WHERE id = 1").await),
        "first"
    );
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM notes WHERE note IS NULL").await),
        1,
        "only the updated row should be non-NULL"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn update_and_delete_with_string_predicates() {
    let rt = runtime("string-predicates").await;

    rt.sql("CREATE TABLE s (id BIGINT, name VARCHAR)").await;
    rt.sql("INSERT INTO s VALUES (1, 'a'), (2, 'b'), (3, 'b')")
        .await;

    rt.sql("UPDATE s SET name = 'z' WHERE name = 'b'").await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM s WHERE name = 'z'").await),
        2,
        "both 'b' rows should have matched the string predicate"
    );
    assert_eq!(
        scalar_string(&rt.sql("SELECT name FROM s WHERE id = 1").await),
        "a",
        "the non-matching row must be untouched"
    );

    rt.sql("DELETE FROM s WHERE name = 'z'").await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM s").await),
        1,
        "DELETE with a string predicate should remove exactly the matches"
    );
    assert_eq!(scalar_i64(&rt.sql("SELECT id FROM s").await), 1);
}
