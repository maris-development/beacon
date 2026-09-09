//! The default table (`sql.default_table`) is a name Beacon fills only when it is free.
//!
//! At startup Beacon registers an empty stand-in under that name, so a `from`-less
//! JSON query plans on a fresh database. The stand-in is an ordinary table: a
//! `CREATE` on that name fails, and you run `DROP TABLE` first to take it. Once a
//! real table holds the name, Beacon leaves it alone, restart included.

mod common;

use beacon_core::query::Query;
use beacon_core::settings::SqlSettings;
use beacon_core::AuthIdentity;
use futures::TryStreamExt;

/// A JSON (non-SQL) query with no `from`, which the compiler resolves against the
/// runtime's configured `sql.default_table`.
fn json_query_without_from() -> Query {
    serde_json::from_str(r#"{"select": [{"column": "id"}]}"#).expect("a valid JSON query body")
}

/// Rows a `from`-less JSON query returns, so a test can prove which table the
/// default-table name resolves to.
async fn rows_without_from(rt: &common::TestRuntime) -> usize {
    let batches = rt
        .runtime
        .run_query(json_query_without_from(), AuthIdentity::system())
        .await
        .expect("a from-less query should plan against the default table")
        .into_record_stream()
        .expect("the result should be a record stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("the stream should run");
    common::total_rows(&batches)
}

/// The stand-in is an ordinary table: `CREATE TABLE` on its name fails. The error
/// names the stand-in, because no user made that table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_table_fails_while_the_stand_in_holds_the_name() {
    let rt = common::runtime("default-create-blocked").await;

    let error = rt
        .try_sql(r#"CREATE TABLE "default" (id BIGINT)"#)
        .await
        .expect_err("the name is taken, so the create should fail");

    let message = error.to_string();
    assert!(
        message.contains("already exists"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("DROP TABLE"),
        "the error should say how to free the name: {message}"
    );
}

/// Every `CREATE` statement refuses the name the stand-in holds, and each one
/// says how to free it. No statement replaces the stand-in quietly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_create_statement_refuses_the_stand_in_name() {
    let rt = common::runtime("default-create-blocked-all").await;
    common::write_file(&rt.datasets_dir().join("obs/a.csv"), "id\n1\n");

    for statement in [
        r#"CREATE TABLE "default" (id BIGINT)"#,
        r#"CREATE EXTERNAL TABLE "default" STORED AS CSV LOCATION 'obs/'"#,
        r#"CREATE VIEW "default" AS SELECT 1 AS id"#,
        r#"CREATE MATERIALIZED VIEW "default" AS SELECT 1 AS id"#,
    ] {
        let message = rt
            .try_sql(statement)
            .await
            .err()
            .unwrap_or_else(|| panic!("should fail while the stand-in holds the name: {statement}"))
            .to_string();

        assert!(
            message.contains("already exists"),
            "`{statement}` gave an unexpected error: {message}"
        );
        assert!(
            message.contains("DROP TABLE"),
            "`{statement}` should say how to free the name: {message}"
        );
    }

    // The stand-in is still there, and still empty.
    assert_eq!(
        common::total_rows(&rt.sql(r#"SELECT * FROM "default""#).await),
        0
    );
}

/// The cycle an operator runs: `DROP` the stand-in, `CREATE TABLE` under the same
/// name, restart. The table and its rows come back, and Beacon adds no stand-in
/// over them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_managed_default_table_survives_a_restart() {
    let rt = common::restartable_runtime("default-managed", |b| b).await;

    rt.sql(r#"DROP TABLE "default""#).await;
    rt.sql(r#"CREATE TABLE "default" (id BIGINT)"#).await;
    rt.sql(r#"INSERT INTO "default" VALUES (1), (2), (3)"#).await;

    let rt = rt.restart().await;

    assert_eq!(
        common::scalar_i64(&rt.sql(r#"SELECT count(*) FROM "default""#).await),
        3,
        "the managed table and its rows should survive the restart"
    );
    assert_eq!(rows_without_from(&rt).await, 3);
    // The stand-in is column-less; the surviving table reports its own column.
    assert_eq!(
        common::column_strings(&rt.sql(r#"SHOW COLUMNS FROM "default""#).await, 3),
        vec!["id"],
        "the resolved table should be the user's, not a fresh stand-in"
    );
}

/// The same cycle with `CREATE EXTERNAL TABLE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_external_default_table_survives_a_restart() {
    let rt = common::restartable_runtime("default-external", |b| b).await;
    common::write_file(&rt.datasets_dir().join("obs/a.csv"), "id,v\n1,2\n3,4\n");

    rt.sql(r#"DROP TABLE "default""#).await;
    rt.sql(r#"CREATE EXTERNAL TABLE "default" STORED AS CSV LOCATION 'obs/'"#)
        .await;
    assert_eq!(
        common::scalar_i64(&rt.sql(r#"SELECT count(*) FROM "default""#).await),
        2
    );

    let rt = rt.restart().await;

    assert_eq!(
        common::scalar_i64(&rt.sql(r#"SELECT count(*) FROM "default""#).await),
        2,
        "Beacon must not put a stand-in over the user's table after a restart"
    );
    assert_eq!(rows_without_from(&rt).await, 2);
}

/// `CREATE MATERIALIZED VIEW` refuses the name for the same reason, and its error
/// names the stand-in too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_materialized_view_fails_while_the_stand_in_holds_the_name() {
    let rt = common::runtime("default-materialized-view").await;

    let error = rt
        .try_sql(r#"CREATE MATERIALIZED VIEW "default" AS SELECT 1 AS id"#)
        .await
        .expect_err("the name is taken, so the create should fail");

    let message = error.to_string();
    assert!(
        message.contains("already exists") && message.contains("DROP TABLE"),
        "unexpected error: {message}"
    );

    // After the drop the name is free.
    rt.sql(r#"DROP TABLE "default""#).await;
    rt.sql(r#"CREATE MATERIALIZED VIEW "default" AS SELECT 1 AS id"#)
        .await;
    assert_eq!(rows_without_from(&rt).await, 1);
}

/// A dropped default table comes back as an empty stand-in on the next start,
/// because the name is free again. This is what keeps a `from`-less query planning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_free_name_gets_a_stand_in_on_the_next_start() {
    let rt = common::restartable_runtime("default-dropped", |b| b).await;

    rt.sql(r#"DROP TABLE "default""#).await;
    assert!(
        !common::column_strings(&rt.sql("SHOW TABLES").await, 2)
            .iter()
            .any(|name| name == "default"),
        "the drop should hold for this run"
    );

    let rt = rt.restart().await;

    assert!(
        common::column_strings(&rt.sql("SHOW TABLES").await, 2)
            .iter()
            .any(|name| name == "default"),
        "a free default-table name gets a stand-in again"
    );
    assert_eq!(
        common::total_rows(&rt.sql(r#"SELECT * FROM "default""#).await),
        0,
        "the fresh stand-in is empty"
    );
}

/// The stand-in uses the configured name. A deployment that sets
/// `sql.default_table` to `observations` gets `observations`, not `default`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stand_in_uses_the_configured_name() {
    let rt = common::runtime_with("default-configured", |builder| {
        builder.with_sql_settings(SqlSettings {
            default_table: "observations".to_string(),
            ..Default::default()
        })
    })
    .await;

    let names = common::column_strings(&rt.sql("SHOW TABLES").await, 2);
    assert!(
        names.iter().any(|name| name == "observations"),
        "the stand-in should hold the configured name: {names:?}"
    );
    assert!(
        !names.iter().any(|name| name == "default"),
        "no table should hold the literal name 'default': {names:?}"
    );
    assert_eq!(
        common::total_rows(&rt.sql("SELECT * FROM observations").await),
        0,
        "the stand-in should be queryable under the configured name"
    );

    // The configured name behaves like any other: drop, then create.
    rt.sql("DROP TABLE observations").await;
    rt.sql("CREATE TABLE observations (id BIGINT)").await;
    rt.sql("INSERT INTO observations VALUES (7)").await;
    assert_eq!(rows_without_from(&rt).await, 1);
}
