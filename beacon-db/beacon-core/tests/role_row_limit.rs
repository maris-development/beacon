//! The `query_output_row_limit` role setting, end to end: SQL sets it, and the
//! runtime fails a query of the role's users that outputs more rows, for a
//! streamed result and for a file output. Without it, there is no row limit.

mod common;

use beacon_core::query::Query;
use beacon_core::{AuthIdentity, Credential};
use common::{runtime_with, total_rows, TestRuntime};
use serde_json::json;

/// A query with exactly `rows` result rows.
fn rows_sql(rows: u64) -> String {
    format!("SELECT v FROM generate_series(1, {rows}) AS t(v)")
}

async fn user(rt: &TestRuntime, name: &str, role: Option<&str>) -> AuthIdentity {
    rt.sql(&format!("CREATE USER {name} WITH PASSWORD 'pw'"))
        .await;
    if let Some(role) = role {
        rt.sql(&format!("GRANT ROLE {role} TO USER {name}")).await;
    }
    rt.runtime
        .authenticate(&Credential::basic(name, "pw"))
        .await
        .expect("the user should authenticate")
}

async fn csv_export(rt: &TestRuntime, sql: &str, identity: AuthIdentity) -> anyhow::Result<()> {
    let mut query = Query::sql(sql.to_string());
    query.output = Some(serde_json::from_value(json!({ "format": "csv" })).expect("valid output"));
    rt.runtime.run_query(query, identity).await.map(|_| ())
}

fn assert_over_limit(result: anyhow::Result<impl Sized>) {
    let error = result
        .err()
        .expect("the query should go over its row limit");
    assert!(format!("{error:#}").contains("row limit"), "{error:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_limits_the_rows_its_users_get() {
    let rt = runtime_with("role-row-limit", |builder| builder).await;
    rt.sql("CREATE ROLE reader").await;
    let alice = user(&rt, "alice", Some("reader")).await;
    let big = rows_sql(200_000);

    let batches = rt
        .try_sql_as(&big, alice.clone())
        .await
        .expect("no limit before the role sets one");
    assert_eq!(total_rows(&batches), 200_000);

    rt.sql("ALTER ROLE reader SET query_output_row_limit = 100000")
        .await;
    let batches = rt
        .try_sql_as(&rows_sql(100_000), alice.clone())
        .await
        .expect("exactly the limit passes");
    assert_eq!(total_rows(&batches), 100_000);
    assert_over_limit(rt.try_sql_as(&big, alice.clone()).await);
    assert_over_limit(csv_export(&rt, &big, alice.clone()).await);
    csv_export(&rt, &rows_sql(100_000), alice.clone())
        .await
        .expect("a file at the limit is written");

    // Aggregates count output rows, not the rows they read.
    rt.try_sql_as(
        "SELECT count(*) FROM generate_series(1, 5000000) AS t(v)",
        alice.clone(),
    )
    .await
    .expect("one output row");

    rt.try_sql(&big).await.expect("the super-user has no limit");
    rt.sql("ALTER ROLE reader RESET query_output_row_limit")
        .await;
    rt.try_sql_as(&big, alice)
        .await
        .expect("no limit after the reset");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_most_generous_role_wins_and_no_role_means_no_limit() {
    let rt = runtime_with("role-row-limit-roles", |builder| builder).await;
    rt.sql("CREATE ROLE tight").await;
    rt.sql("CREATE ROLE premium").await;
    rt.sql("ALTER ROLE tight SET query_output_row_limit = 10")
        .await;
    rt.sql("ALTER ROLE premium SET query_output_row_limit TO 0")
        .await;
    let limited = user(&rt, "limited", Some("tight")).await;
    user(&rt, "paying", Some("tight")).await;
    rt.sql("GRANT ROLE premium TO USER paying").await;
    let paying = rt
        .runtime
        .authenticate(&Credential::basic("paying", "pw"))
        .await
        .unwrap();
    let plain = user(&rt, "plain", None).await;

    assert_over_limit(rt.try_sql_as(&rows_sql(11), limited).await);
    rt.try_sql_as(&rows_sql(1000), paying)
        .await
        .expect("0 on one role beats the limit of the other");
    rt.try_sql_as(&rows_sql(1000), plain)
        .await
        .expect("a user without a limiting role has no limit");

    let error = rt
        .try_sql("ALTER ROLE tight SET query_output_row_limit = '1M'")
        .await
        .expect_err("the value must be whole rows");
    assert!(
        format!("{error:#}").contains("whole number of rows"),
        "{error:#}"
    );
}
