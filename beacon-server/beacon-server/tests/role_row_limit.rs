//! The `query_output_row_limit` role setting, end to end: SQL sets it, and the
//! server fails a query of the role's users that outputs more rows, for a
//! streamed result and for a file output. Without it, there is no row limit.

mod common;

use beacon_auth::AuthIdentity;
use beacon_core::query::Query;
use common::{test_server, total_rows, TestServer};
use serde_json::json;

/// A query with exactly `rows` result rows.
fn rows_sql(rows: u64) -> String {
    format!("SELECT v FROM generate_series(1, {rows}) AS t(v)")
}

async fn user(ts: &TestServer, name: &str, role: Option<&str>) -> AuthIdentity {
    ts.sql(&format!("CREATE USER {name} WITH PASSWORD 'pw'"))
        .await;
    if let Some(role) = role {
        ts.sql(&format!("GRANT ROLE {role} TO USER {name}")).await;
    }
    ts.login(name, "pw").await
}

async fn csv_export(ts: &TestServer, sql: &str, identity: AuthIdentity) -> anyhow::Result<()> {
    let mut query = Query::sql(sql.to_string());
    query.output = Some(serde_json::from_value(json!({ "format": "csv" })).expect("valid output"));
    ts.server.run_query(query, identity).await.map(|_| ())
}

fn assert_over_limit(result: anyhow::Result<impl Sized>) {
    let error = result
        .err()
        .expect("the query should go over its row limit");
    assert!(format!("{error:#}").contains("row limit"), "{error:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_limits_the_rows_its_users_get() {
    let ts = test_server().await;
    ts.sql("CREATE ROLE reader").await;
    let alice = user(&ts, "alice", Some("reader")).await;
    let big = rows_sql(200_000);

    let batches = ts
        .try_sql_as(&big, alice.clone())
        .await
        .expect("no limit before the role sets one");
    assert_eq!(total_rows(&batches), 200_000);

    ts.sql("ALTER ROLE reader SET query_output_row_limit = 100000")
        .await;
    let batches = ts
        .try_sql_as(&rows_sql(100_000), alice.clone())
        .await
        .expect("exactly the limit passes");
    assert_eq!(total_rows(&batches), 100_000);
    assert_over_limit(ts.try_sql_as(&big, alice.clone()).await);
    assert_over_limit(csv_export(&ts, &big, alice.clone()).await);
    csv_export(&ts, &rows_sql(100_000), alice.clone())
        .await
        .expect("a file at the limit is written");

    // Aggregates count output rows, not the rows they read.
    ts.try_sql_as(
        "SELECT count(*) FROM generate_series(1, 5000000) AS t(v)",
        alice.clone(),
    )
    .await
    .expect("one output row");

    ts.try_sql(&big).await.expect("the super-user has no limit");
    ts.sql("ALTER ROLE reader RESET query_output_row_limit")
        .await;
    ts.try_sql_as(&big, alice)
        .await
        .expect("no limit after the reset");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_most_generous_role_wins_and_no_role_means_no_limit() {
    let ts = test_server().await;
    ts.sql("CREATE ROLE tight").await;
    ts.sql("CREATE ROLE premium").await;
    ts.sql("ALTER ROLE tight SET query_output_row_limit = 10")
        .await;
    ts.sql("ALTER ROLE premium SET query_output_row_limit TO 0")
        .await;
    let limited = user(&ts, "limited", Some("tight")).await;
    user(&ts, "paying", Some("tight")).await;
    ts.sql("GRANT ROLE premium TO USER paying").await;
    let paying = ts.login("paying", "pw").await;
    let plain = user(&ts, "plain", None).await;

    assert_over_limit(ts.try_sql_as(&rows_sql(11), limited).await);
    ts.try_sql_as(&rows_sql(1000), paying)
        .await
        .expect("0 on one role beats the limit of the other");
    ts.try_sql_as(&rows_sql(1000), plain)
        .await
        .expect("a user without a limiting role has no limit");

    let error = ts
        .try_sql("ALTER ROLE tight SET query_output_row_limit = '1M'")
        .await
        .expect_err("the value must be whole rows");
    assert!(
        format!("{error:#}").contains("whole number of rows"),
        "{error:#}"
    );
}
