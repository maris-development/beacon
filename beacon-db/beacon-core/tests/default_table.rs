//! The default table (`sql.default_table`) is a name, not a fixed table.
//!
//! Beacon registers an empty stand-in under that name so a `from`-less JSON query
//! plans on a fresh database. These tests prove the stand-in yields: any `CREATE`
//! statement takes the name, the resulting table persists, and a real table under
//! that name is never replaced.

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

/// `CREATE TABLE` takes the default-table name without a `DROP` first: the
/// stand-in is a placeholder, not a table the user has to clear out of the way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_table_takes_the_default_table_name() {
    let rt = common::runtime("default-create-table").await;

    rt.sql(r#"CREATE TABLE "default" (id BIGINT)"#).await;
    rt.sql(r#"INSERT INTO "default" VALUES (1), (2)"#).await;

    let rows = rt.sql(r#"SELECT count(*) FROM "default""#).await;
    assert_eq!(common::scalar_i64(&rows), 2);
    assert_eq!(
        rows_without_from(&rt).await,
        2,
        "a from-less JSON query should read the table the user created"
    );
}

/// A real table under the default-table name still blocks a second `CREATE TABLE`.
/// Only the stand-in yields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_real_default_table_blocks_a_second_create() {
    let rt = common::runtime("default-create-twice").await;
    rt.sql(r#"CREATE TABLE "default" (id BIGINT)"#).await;

    let error = rt
        .try_sql(r#"CREATE TABLE "default" (id BIGINT)"#)
        .await
        .expect_err("a real table must not be overwritten");

    assert!(
        error.to_string().contains("already exists"),
        "unexpected error: {error}"
    );
}

/// `CREATE VIEW` takes the default-table name too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_view_takes_the_default_table_name() {
    let rt = common::runtime("default-view").await;

    rt.sql(r#"CREATE VIEW "default" AS SELECT 1 AS id"#).await;

    assert_eq!(rows_without_from(&rt).await, 1);
}

/// `DROP` then `CREATE EXTERNAL TABLE` leaves a table called `default`, and the
/// startup stand-in does not take the name back on the next start.
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
        "the stand-in must not replace the user's table after a restart"
    );
    assert_eq!(rows_without_from(&rt).await, 2);
}

/// The whole cycle an operator runs: start, `DROP` the stand-in, `CREATE TABLE`
/// under the same name, restart. The managed table and its rows come back, and
/// startup registers no stand-in over them.
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

/// `CREATE MATERIALIZED VIEW` takes the default-table name too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_materialized_view_takes_the_default_table_name() {
    let rt = common::runtime("default-materialized-view").await;

    rt.sql(r#"CREATE MATERIALIZED VIEW "default" AS SELECT 1 AS id"#)
        .await;

    assert_eq!(rows_without_from(&rt).await, 1);
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

    // The stand-in yields to a real table under the configured name too.
    rt.sql("CREATE TABLE observations (id BIGINT)").await;
    rt.sql("INSERT INTO observations VALUES (7)").await;
    assert_eq!(rows_without_from(&rt).await, 1);
}
