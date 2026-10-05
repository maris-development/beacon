//! `COMMENT ON TABLE | COLUMN`: free-text comments that every consumer reads
//! from the Arrow schema of `Runtime::table_arrow_schema`.
//!
//! Comments live in a `db://<table>/comments.json` sidecar. These tests pin the
//! SQL surface, the schema metadata, the lifecycle (`ALTER`, `DROP`, restart)
//! and the `beacon.system.comments` listing.

mod common;

use arrow::array::{Array, AsArray};
use arrow::datatypes::SchemaRef;
use beacon_core::{AuthIdentity, Credential};
use common::{restartable_runtime, TestRuntime};
use datafusion::sql::TableReference;

async fn schema(rt: &TestRuntime, table: &str) -> SchemaRef {
    rt.runtime
        .table_arrow_schema(TableReference::bare(table), &AuthIdentity::system())
        .await
        .expect("table schema")
}

fn table_comment(schema: &SchemaRef) -> Option<&str> {
    schema.metadata().get("comment").map(String::as_str)
}

fn column_comment<'a>(schema: &'a SchemaRef, column: &str) -> Option<&'a str> {
    schema
        .field_with_name(column)
        .expect("column exists")
        .metadata()
        .get("comment")
        .map(String::as_str)
}

async fn run(rt: &TestRuntime, statements: &[&str]) {
    for sql in statements {
        rt.sql_as(sql, AuthIdentity::system()).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn comments_show_in_the_arrow_schema() {
    let rt = restartable_runtime("tc-show", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE obs (depth DOUBLE, temp DOUBLE)",
            "COMMENT ON TABLE obs IS 'Argo float profiles'",
            "COMMENT ON COLUMN obs.depth IS 'Measurement depth in meters'",
        ],
    )
    .await;

    let schema = schema(&rt, "obs").await;

    assert_eq!(table_comment(&schema), Some("Argo float profiles"));
    assert_eq!(
        column_comment(&schema, "depth"),
        Some("Measurement depth in meters")
    );
    assert_eq!(column_comment(&schema, "temp"), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_comment_replaces_the_old_one_and_null_removes_it() {
    let rt = restartable_runtime("tc-replace", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE obs (depth DOUBLE)",
            "COMMENT ON TABLE obs IS 'first'",
            "COMMENT ON TABLE obs IS 'second'",
            "COMMENT ON COLUMN obs.depth IS 'meters'",
        ],
    )
    .await;
    assert_eq!(table_comment(&schema(&rt, "obs").await), Some("second"));

    run(
        &rt,
        &[
            "COMMENT ON TABLE obs IS NULL",
            "COMMENT ON COLUMN obs.depth IS NULL",
        ],
    )
    .await;

    let schema = schema(&rt, "obs").await;
    assert_eq!(table_comment(&schema), None);
    assert_eq!(column_comment(&schema, "depth"), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn comments_survive_a_restart() {
    let rt = restartable_runtime("tc-restart", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE obs (depth DOUBLE)",
            "COMMENT ON TABLE obs IS 'kept'",
            "COMMENT ON COLUMN obs.depth IS 'meters'",
        ],
    )
    .await;

    let rt = rt.restart().await;

    let schema = schema(&rt, "obs").await;
    assert_eq!(table_comment(&schema), Some("kept"));
    assert_eq!(column_comment(&schema, "depth"), Some("meters"));
}

#[tokio::test(flavor = "multi_thread")]
async fn names_keep_their_case() {
    let rt = restartable_runtime("tc-case", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE MyObs (Depth DOUBLE, temp DOUBLE)",
            "COMMENT ON TABLE MyObs IS 'mixed case'",
            "COMMENT ON COLUMN MyObs.Depth IS 'unquoted'",
            r#"COMMENT ON COLUMN "MyObs"."temp" IS 'quoted'"#,
        ],
    )
    .await;

    let wrong_case = rt
        .try_sql_as(
            "COMMENT ON COLUMN MyObs.depth IS 'x'",
            AuthIdentity::system(),
        )
        .await;

    let schema = schema(&rt, "MyObs").await;
    assert_eq!(table_comment(&schema), Some("mixed case"));
    assert_eq!(column_comment(&schema, "Depth"), Some("unquoted"));
    assert_eq!(column_comment(&schema, "temp"), Some("quoted"));
    assert!(wrong_case.is_err(), "depth is not the column Depth");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_column_is_rejected_but_null_removes_a_stale_key() {
    let rt = restartable_runtime("tc-unknown-col", |b| b).await;
    run(&rt, &["CREATE TABLE obs (depth DOUBLE)"]).await;

    let unknown = rt
        .try_sql_as("COMMENT ON COLUMN obs.ghost IS 'x'", AuthIdentity::system())
        .await;
    let remove = rt
        .try_sql_as(
            "COMMENT ON COLUMN obs.ghost IS NULL",
            AuthIdentity::system(),
        )
        .await;

    let message = unknown.expect_err("unknown column").to_string();
    assert!(message.contains("ghost"), "unexpected error: {message}");
    remove.expect("IS NULL skips the column check, so a stale key can be removed");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_table_errors_unless_if_exists() {
    let rt = restartable_runtime("tc-unknown-table", |b| b).await;

    let missing = rt
        .try_sql_as("COMMENT ON TABLE nope IS 'x'", AuthIdentity::system())
        .await;
    let if_exists = rt
        .try_sql_as(
            "COMMENT IF EXISTS ON TABLE nope IS 'x'",
            AuthIdentity::system(),
        )
        .await;

    assert!(missing.is_err(), "an unknown table must error");
    if_exists.expect("IF EXISTS on an unknown table is a no-op");
}

#[tokio::test(flavor = "multi_thread")]
async fn only_tables_of_the_default_schema_take_comments() {
    let rt = restartable_runtime("tc-schema", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE tables (a BIGINT)",
            "COMMENT ON TABLE tables IS 'a user table'",
        ],
    )
    .await;

    let metadata_table = rt
        .try_sql_as(
            "COMMENT ON TABLE information_schema.tables IS 'x'",
            AuthIdentity::system(),
        )
        .await;
    let catalog_view = rt
        .runtime
        .table_arrow_schema(
            TableReference::partial("information_schema", "tables"),
            &AuthIdentity::system(),
        )
        .await
        .expect("information_schema.tables schema");

    assert!(
        metadata_table.is_err(),
        "a metadata table takes no comments"
    );
    assert_eq!(
        table_comment(&catalog_view),
        None,
        "the comment of user table 'tables' must not leak to information_schema.tables"
    );
    assert_eq!(
        table_comment(&schema(&rt, "tables").await),
        Some("a user table")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn other_object_types_are_unsupported() {
    let rt = restartable_runtime("tc-object", |b| b).await;

    let database = rt
        .try_sql_as("COMMENT ON DATABASE beacon IS 'x'", AuthIdentity::system())
        .await;

    let message = database.expect_err("COMMENT ON DATABASE").to_string();
    assert!(
        message.to_lowercase().contains("not supported"),
        "unexpected error: {message}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_super_user_may_comment() {
    let rt = restartable_runtime("tc-authz", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE obs (depth DOUBLE)",
            "CREATE USER alice WITH PASSWORD 'pw'",
        ],
    )
    .await;
    let alice = rt
        .runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice authenticates");

    let denied = rt.try_sql_as("COMMENT ON TABLE obs IS 'x'", alice).await;

    assert!(denied.is_err(), "a regular user must not set comments");
    assert_eq!(table_comment(&schema(&rt, "obs").await), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn rename_column_moves_the_comment_and_drop_column_removes_it() {
    let rt = restartable_runtime("tc-alter", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE obs (depth DOUBLE, temp DOUBLE)",
            "COMMENT ON COLUMN obs.depth IS 'meters'",
            "COMMENT ON COLUMN obs.temp IS 'celsius'",
            "ALTER TABLE obs RENAME COLUMN depth TO pressure",
            "ALTER TABLE obs DROP COLUMN temp",
            "ALTER TABLE obs ADD COLUMN temp DOUBLE",
        ],
    )
    .await;

    let schema = schema(&rt, "obs").await;
    assert_eq!(column_comment(&schema, "pressure"), Some("meters"));
    assert_eq!(
        column_comment(&schema, "temp"),
        None,
        "a re-added column must not inherit the comment of the dropped one"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_deletes_the_comments() {
    let rt = restartable_runtime("tc-drop", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE obs (depth DOUBLE)",
            "COMMENT ON TABLE obs IS 'old'",
            "COMMENT ON COLUMN obs.depth IS 'old'",
            "DROP TABLE obs",
            "CREATE TABLE obs (depth DOUBLE)",
        ],
    )
    .await;

    let schema = schema(&rt, "obs").await;
    assert_eq!(table_comment(&schema), None);
    assert_eq!(column_comment(&schema, "depth"), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_view_takes_comments() {
    let rt = restartable_runtime("tc-view", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE obs (depth DOUBLE, temp DOUBLE)",
            "CREATE VIEW shallow AS SELECT depth FROM obs WHERE depth < 10",
            "COMMENT ON TABLE shallow IS 'Surface layer only'",
            "COMMENT ON COLUMN shallow.depth IS 'meters'",
        ],
    )
    .await;

    let schema = schema(&rt, "shallow").await;
    assert_eq!(table_comment(&schema), Some("Surface layer only"));
    assert_eq!(column_comment(&schema, "depth"), Some("meters"));
}

#[tokio::test(flavor = "multi_thread")]
async fn system_comments_lists_every_comment_for_the_super_user_only() {
    let rt = restartable_runtime("tc-system", |b| b).await;
    run(
        &rt,
        &[
            "CREATE TABLE obs (depth DOUBLE)",
            "CREATE TABLE plain (a BIGINT)",
            "COMMENT ON TABLE obs IS 'profiles'",
            "COMMENT ON COLUMN obs.depth IS 'meters'",
            "CREATE USER alice WITH PASSWORD 'pw'",
        ],
    )
    .await;
    let alice = rt
        .runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice authenticates");

    let batches = rt
        .sql(
            "SELECT table_name, column_name, comment FROM beacon.system.comments \
             ORDER BY table_name, column_name NULLS FIRST",
        )
        .await;
    let denied = rt
        .try_sql_as("SELECT * FROM beacon.system.comments", alice)
        .await;

    let mut rows = Vec::new();
    for batch in &batches {
        let tables = batch.column(0).as_string::<i32>();
        let columns = batch.column(1).as_string::<i32>();
        let comments = batch.column(2).as_string::<i32>();
        for row in 0..batch.num_rows() {
            let column = (!columns.is_null(row)).then(|| columns.value(row).to_string());
            rows.push((
                tables.value(row).to_string(),
                column,
                comments.value(row).to_string(),
            ));
        }
    }
    assert_eq!(
        rows,
        vec![
            ("obs".to_string(), None, "profiles".to_string()),
            (
                "obs".to_string(),
                Some("depth".to_string()),
                "meters".to_string()
            ),
        ]
    );
    assert!(denied.is_err(), "beacon.system is super-user only");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_extension_statements_are_gone() {
    let rt = restartable_runtime("tc-no-ext", |b| b).await;
    run(&rt, &["CREATE TABLE obs (depth DOUBLE)"]).await;

    for sql in [
        r#"SET EXTENSION 'mcp' FOR obs TO '{"enabled": true}'"#,
        "DROP EXTENSION 'mcp' FOR obs",
        "SHOW EXTENSIONS FOR obs",
    ] {
        assert!(
            rt.try_sql_as(sql, AuthIdentity::system()).await.is_err(),
            "{sql} must no longer run"
        );
    }
}
