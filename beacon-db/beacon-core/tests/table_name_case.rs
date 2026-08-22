//! Beacon keeps the case of every identifier.
//!
//! The session sets `enable_ident_normalization = false`, so the catalog holds a
//! table under the exact name the statement writes. Several statements used to
//! rebuild a `TableReference` from a string with `TableReference::parse_str`,
//! which lowercases every unquoted part. `CREATE TABLE MyTable` then answered
//! `No table named 'mytable'` to the very next statement.
//!
//! These tests pin both halves: the case-sensitive rule itself, and each
//! statement that used to break it.

mod common;

use common::{
    TestRuntime, column_strings, restartable_runtime, runtime, scalar_i64, total_rows, write_file,
};

/// A two-row CSV whose column names are not lowercase either.
fn write_obs(rt: &TestRuntime) {
    write_file(
        &rt.datasets_dir().join("obs.csv"),
        "Depth,TEMP,value\n1,2.0,3\n4,5.0,6\n",
    );
}

/// The user-facing table names, in catalog order.
async fn table_names(rt: &TestRuntime) -> Vec<String> {
    let batches = rt.sql("SHOW TABLES").await;
    column_strings(&batches, 2)
}

fn assert_missing(result: anyhow::Result<Vec<arrow::record_batch::RecordBatch>>, name: &str) {
    let error = format!(
        "{:#}",
        result.expect_err("the lowercase name must not resolve")
    );
    assert!(
        error.contains(name),
        "the error must name what was looked up, got: {error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_name_keeps_its_case() {
    let rt = runtime("case-table-name").await;
    write_obs(&rt);
    rt.sql("CREATE EXTERNAL TABLE MyTable STORED AS CSV LOCATION 'obs.csv'")
        .await;

    assert_eq!(total_rows(&rt.sql("SELECT * FROM MyTable").await), 2);
    assert_eq!(total_rows(&rt.sql("SELECT * FROM \"MyTable\"").await), 2);
    assert!(table_names(&rt).await.contains(&"MyTable".to_string()));

    // The mirror image holds too: a lowercase spelling is a different name.
    assert_missing(rt.try_sql("SELECT * FROM mytable").await, "mytable");
    assert_missing(rt.try_sql("SELECT * FROM MYTABLE").await, "MYTABLE");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_column_name_keeps_its_case() {
    let rt = runtime("case-column-name").await;
    write_obs(&rt);
    rt.sql("CREATE EXTERNAL TABLE MyTable STORED AS CSV LOCATION 'obs.csv'")
        .await;

    assert_eq!(
        total_rows(&rt.sql("SELECT TEMP, Depth FROM MyTable").await),
        2
    );
    assert!(rt.try_sql("SELECT temp FROM MyTable").await.is_err());
    assert!(rt.try_sql("SELECT depth FROM MyTable").await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_matches_the_case() {
    let rt = runtime("case-drop-table").await;
    write_obs(&rt);
    rt.sql("CREATE EXTERNAL TABLE MyTable STORED AS CSV LOCATION 'obs.csv'")
        .await;

    assert!(rt.try_sql("DROP TABLE mytable").await.is_err());
    rt.sql("DROP TABLE MyTable").await;
    assert!(!table_names(&rt).await.contains(&"MyTable".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_name_keeps_its_case_over_a_restart() {
    let rt = restartable_runtime("case-restart", |b| b).await;
    write_obs(&rt);
    rt.sql("CREATE EXTERNAL TABLE MyTable STORED AS CSV LOCATION 'obs.csv'")
        .await;

    let rt = rt.restart().await;
    assert!(table_names(&rt).await.contains(&"MyTable".to_string()));
    assert_eq!(total_rows(&rt.sql("SELECT * FROM MyTable").await), 2);
    assert_missing(rt.try_sql("SELECT * FROM mytable").await, "mytable");
}

/// `INSERT INTO` used to look up the lowercased name and fail.
#[tokio::test(flavor = "multi_thread")]
async fn insert_into_reaches_a_mixed_case_table() {
    let rt = runtime("case-insert").await;
    rt.sql("CREATE TABLE MyManaged (Id BIGINT, Name VARCHAR)")
        .await;

    rt.sql("INSERT INTO MyManaged VALUES (1, 'a'), (2, 'b')")
        .await;

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM MyManaged").await),
        2
    );
}

/// `CREATE TABLE AS SELECT` used to register the table and then fail its insert,
/// which left an empty table behind.
#[tokio::test(flavor = "multi_thread")]
async fn create_table_as_select_fills_a_mixed_case_table() {
    let rt = runtime("case-ctas").await;

    rt.sql("CREATE TABLE MyManaged AS SELECT 1 AS Id, 'a' AS Name")
        .await;

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM MyManaged").await),
        1
    );
}

/// `ALTER TABLE` used to look up the lowercased name and fail.
#[tokio::test(flavor = "multi_thread")]
async fn alter_table_reaches_a_mixed_case_table() {
    let rt = runtime("case-alter").await;
    rt.sql("CREATE TABLE MyManaged (Id BIGINT)").await;

    rt.sql("ALTER TABLE MyManaged ADD COLUMN Extra BIGINT")
        .await;

    let columns = column_strings(&rt.sql("DESCRIBE MyManaged").await, 0);
    assert!(columns.contains(&"Extra".to_string()), "got {columns:?}");
}

/// `CREATE INDEX`, `SHOW INDEXES` and `DROP INDEX` share one lookup, which used
/// to lowercase the table name.
#[tokio::test(flavor = "multi_thread")]
async fn the_index_statements_reach_a_mixed_case_table() {
    let rt = runtime("case-index").await;
    rt.sql("CREATE TABLE MyManaged AS SELECT 1 AS Id").await;

    rt.sql("CREATE INDEX id_idx ON MyManaged (Id)").await;
    let indexes = column_strings(&rt.sql("SHOW INDEXES ON MyManaged").await, 0);
    assert!(indexes.contains(&"id_idx".to_string()), "got {indexes:?}");

    rt.sql("DROP INDEX id_idx ON MyManaged").await;
    // `CREATE TABLE AS SELECT` builds a default zone map per column, so the
    // listing keeps `zm_Id` after the named index goes.
    let indexes = column_strings(&rt.sql("SHOW INDEXES ON MyManaged").await, 0);
    assert!(!indexes.contains(&"id_idx".to_string()), "got {indexes:?}");
}

/// `REFRESH` used to look up the lowercased name and fail for every name that
/// was not already lowercase.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_reaches_a_mixed_case_table() {
    let rt = runtime("case-refresh").await;
    write_obs(&rt);
    rt.sql("CREATE EXTERNAL TABLE MyTable STORED AS CSV LOCATION 'obs.csv'")
        .await;

    rt.sql("REFRESH TABLE MyTable").await;

    assert_eq!(total_rows(&rt.sql("SELECT * FROM MyTable").await), 2);
    assert!(rt.try_sql("REFRESH TABLE mytable").await.is_err());
}

/// The worst of the set: the view registered under the lowercased name, while
/// its persisted definition kept the written one, so a restart renamed it.
#[tokio::test(flavor = "multi_thread")]
async fn a_materialized_view_keeps_one_name_over_a_restart() {
    let rt = restartable_runtime("case-materialized-view", |b| b).await;
    write_obs(&rt);
    rt.sql("CREATE EXTERNAL TABLE MyTable STORED AS CSV LOCATION 'obs.csv'")
        .await;

    rt.sql("CREATE MATERIALIZED VIEW MyView AS SELECT * FROM MyTable")
        .await;

    assert!(table_names(&rt).await.contains(&"MyView".to_string()));
    assert_eq!(total_rows(&rt.sql("SELECT * FROM MyView").await), 2);
    assert_missing(rt.try_sql("SELECT * FROM myview").await, "myview");

    rt.sql("REFRESH TABLE MyView").await;
    assert_eq!(total_rows(&rt.sql("SELECT * FROM MyView").await), 2);

    let rt = rt.restart().await;
    assert!(table_names(&rt).await.contains(&"MyView".to_string()));
    assert_eq!(total_rows(&rt.sql("SELECT * FROM MyView").await), 2);
    assert_missing(rt.try_sql("SELECT * FROM myview").await, "myview");
}

/// The table-extension statements share the lookup that the admin `table-config`
/// endpoint uses, and it lowercased the name too.
#[tokio::test(flavor = "multi_thread")]
async fn the_extension_statements_reach_a_mixed_case_table() {
    let rt = runtime("case-extensions").await;
    write_obs(&rt);
    rt.sql("CREATE EXTERNAL TABLE MyTable STORED AS CSV LOCATION 'obs.csv'")
        .await;

    rt.sql("SET EXTENSION 'preset' FOR MyTable TO '{\"presets\":[]}'")
        .await;
    assert_eq!(total_rows(&rt.sql("SHOW EXTENSIONS FOR MyTable").await), 1);

    rt.sql("DROP EXTENSION 'preset' FOR MyTable").await;
    assert!(rt.try_sql("SHOW EXTENSIONS FOR mytable").await.is_err());
}
