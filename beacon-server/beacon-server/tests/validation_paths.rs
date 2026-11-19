//! The paths from the server to the engine, and the check on each of them.
//!
//! The engine knows no users. These tests pin that each path gives the engine the validator
//! of the caller, and that the server alone applies the statements that manage users.

mod common;

use beacon_auth::{AuthIdentity, Credential};
use beacon_core::plan_validator::allow_all;
use beacon_core::query::output::{Output, OutputFormat};
use beacon_core::query::Query;
use common::{config, scalar_string, server_with, total_rows, TestServer, ADMIN_PASSWORD};
use common::{unique, ADMIN_USERNAME};

/// A server with enforcement on, the tables `open` and `secret`, and the user `alice` who
/// can read only `open`.
async fn server_with_reader() -> (TestServer, AuthIdentity) {
    let ts = server_with(config(true)).await;
    for sql in [
        "CREATE TABLE open (a BIGINT)",
        "CREATE TABLE secret (a BIGINT)",
        "INSERT INTO open VALUES (1), (2)",
        "CREATE ROLE reader",
        "GRANT SELECT ON TABLE open TO ROLE reader",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "GRANT ROLE reader TO USER alice",
    ] {
        ts.sql(sql).await;
    }
    let alice = ts.login("alice", "pw").await;
    (ts, alice)
}

// --------------------------------------------------------------------------------------------
// Authentication
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_configured_super_user_is_the_only_super_user() {
    let (ts, alice) = server_with_reader().await;

    let admin = ts
        .server
        .authenticate(&Credential::basic(ADMIN_USERNAME, ADMIN_PASSWORD))
        .await
        .expect("the configured credential authenticates");
    let wrong = ts
        .server
        .authenticate(&Credential::basic(ADMIN_USERNAME, "wrong"))
        .await;

    assert!(admin.is_super_user);
    assert!(!alice.is_super_user);
    assert_eq!(alice.roles, vec!["reader".to_string()]);
    assert!(wrong.is_err(), "a wrong password must not authenticate");
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_access_follows_the_configuration() {
    let on = server_with(config(false)).await;
    let mut off_config = config(false);
    off_config.auth.anonymous_enabled = false;
    let off = server_with(off_config).await;

    let anonymous = on.server.authenticate_anonymous().await;
    let refused = off.server.authenticate_anonymous().await;

    let anonymous = anonymous.expect("anonymous access is on");
    assert!(!anonymous.is_super_user);
    assert!(anonymous.roles.is_empty());
    assert!(refused.is_err(), "anonymous access is off");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_role_change_applies_to_the_next_login() {
    let (ts, _alice) = server_with_reader().await;

    ts.sql("REVOKE ROLE reader FROM USER alice").await;
    let alice = ts.login("alice", "pw").await;

    assert!(alice.roles.is_empty());
    assert!(ts.try_sql_as("SELECT * FROM open", alice).await.is_err());
}

// --------------------------------------------------------------------------------------------
// Plan validation
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_query_reads_only_what_the_roles_grant() {
    let (ts, alice) = server_with_reader().await;

    let granted = ts.try_sql_as("SELECT * FROM open", alice.clone()).await;
    let refused = ts.try_sql_as("SELECT * FROM secret", alice.clone()).await;
    let joined = ts
        .try_sql_as("SELECT * FROM open JOIN secret USING (a)", alice.clone())
        .await;
    let nested = ts
        .try_sql_as("SELECT * FROM (SELECT * FROM secret) AS s", alice)
        .await;

    assert_eq!(total_rows(&granted.expect("a granted table reads")), 2);
    let error = refused.expect_err("a table without a grant is refused");
    assert!(error.to_string().contains("permission denied"), "{error}");
    assert!(joined.is_err(), "one refused table refuses the query");
    assert!(nested.is_err(), "a subquery does not hide the table");
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_and_explain_analyze_ask_the_validator() {
    let (ts, alice) = server_with_reader().await;
    let runtime = ts.server.runtime();
    let query = |table: &str| Query::sql(format!("SELECT * FROM {table}"));
    let validator = || ts.server.validator(alice.clone());

    let explain_open = runtime.explain_query(query("open"), validator()).await;
    let explain_secret = runtime.explain_query(query("secret"), validator()).await;
    let analyze_open = runtime
        .explain_analyze_query(query("open"), validator())
        .await;
    let analyze_secret = runtime
        .explain_analyze_query(query("secret"), validator())
        .await;

    assert!(explain_open.is_ok(), "{explain_open:?}");
    assert!(
        explain_secret.is_err(),
        "a plan of a refused table must not show"
    );
    assert!(analyze_open.is_ok(), "{analyze_open:?}");
    assert!(analyze_secret.is_err(), "a refused table must not run");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_that_is_not_the_super_user_cannot_write() {
    let (ts, alice) = server_with_reader().await;
    let table = unique("t");

    for sql in [
        format!("CREATE TABLE {table} (a BIGINT)"),
        "INSERT INTO open VALUES (3)".to_string(),
        "DELETE FROM open".to_string(),
        "DROP TABLE open".to_string(),
    ] {
        let error = ts
            .try_sql_as(&sql, alice.clone())
            .await
            .expect_err("a write needs the super-user");
        assert!(error.to_string().contains("super-user"), "{sql}: {error}");
    }
    assert_eq!(total_rows(&ts.sql("SELECT * FROM open").await), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_validator_of_a_caller_gives_its_name_to_the_metrics() {
    let (ts, alice) = server_with_reader().await;

    ts.sql_as("SELECT a AS marker_column FROM open", alice)
        .await;
    let owner = ts
        .sql(
            "SELECT username FROM beacon.system.query_metrics \
             WHERE query LIKE '%marker_column FROM open%' AND username <> 'beacon-admin' \
             AND finished_at IS NOT NULL",
        )
        .await;

    assert_eq!(scalar_string(&owner), "alice");
}

// --------------------------------------------------------------------------------------------
// The statements that manage users
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_user_statement_gives_an_empty_result_and_no_metrics_row() {
    let ts = server_with(config(false)).await;

    let result = ts.sql("CREATE USER bob WITH PASSWORD 'topsecret-pw'").await;
    let recorded = ts
        .sql("SELECT query_id FROM beacon.system.query_metrics WHERE query LIKE '%topsecret-pw%'")
        .await;

    assert_eq!(total_rows(&result), 0);
    assert_eq!(
        total_rows(&recorded),
        0,
        "the metrics must not hold a password"
    );
    assert!(ts.server.auth().user_exists("bob").await);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_user_statement_refuses_an_output_format_and_a_second_statement() {
    let ts = server_with(config(false)).await;
    let mut with_output = Query::sql("CREATE ROLE exported".to_string());
    with_output.output = Some(Output {
        format: OutputFormat::Csv,
    });

    let output = ts.server.run_query(with_output, ts.admin().await).await;
    let second = ts.try_sql("CREATE ROLE first; CREATE ROLE second").await;

    let error = output
        .err()
        .expect("a user statement has no rows to export");
    assert!(error.to_string().contains("output format"), "{error}");
    let error = second.expect_err("a request holds one statement");
    assert!(error.to_string().contains("one SQL statement"), "{error}");
    let roles = ts.server.auth().list_roles();
    assert!(
        roles.is_empty(),
        "no refused statement creates a role: {roles:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_engine_alone_has_no_user_statements() {
    let ts = server_with(config(false)).await;
    let runtime = ts.server.runtime();

    for sql in [
        "CREATE USER eve WITH PASSWORD 'pw'",
        "CREATE ROLE engine_role",
        "GRANT SELECT ON TABLE t TO ROLE engine_role",
    ] {
        let result = runtime
            .run_query(Query::sql(sql.to_string()), allow_all())
            .await;
        assert!(result.is_err(), "the engine must refuse `{sql}`");
    }
    assert!(!ts.server.auth().user_exists("eve").await);
    assert!(ts.server.auth().list_roles().is_empty());
}
