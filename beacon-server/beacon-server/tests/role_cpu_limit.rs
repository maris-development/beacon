//! The `query_cpu_limit_ms` role setting, end to end: SQL sets it, the server
//! applies it to the users of the role, and it survives a restart. Without it,
//! a query has no CPU limit.
//! The generic key-value behavior is in `role_settings.rs`.

mod common;

use beacon_auth::AuthIdentity;
use common::{test_server, TestServer};

/// A query that needs far more than 1 ms of CPU time.
const BUSY_SQL: &str = "SELECT count(*) FROM generate_series(1, 20000000) AS t(v) WHERE v % 7 = 3";

async fn user(ts: &TestServer, name: &str, role: Option<&str>) -> AuthIdentity {
    ts.sql(&format!("CREATE USER {name} WITH PASSWORD 'pw'"))
        .await;
    if let Some(role) = role {
        ts.sql(&format!("GRANT ROLE {role} TO USER {name}")).await;
    }
    ts.login(name, "pw").await
}

async fn assert_over_budget(ts: &TestServer, identity: AuthIdentity) {
    let error = ts
        .try_sql_as(BUSY_SQL, identity)
        .await
        .expect_err("the query should go over its CPU budget");
    assert!(format!("{error:#}").contains("CPU budget"), "{error:#}");
}

async fn role_settings(ts: &TestServer, role: &str) -> serde_json::Value {
    let batches = ts
        .sql(&format!(
            "SELECT settings FROM beacon.system.roles WHERE role_name = '{role}'"
        ))
        .await;
    serde_json::from_str(&common::scalar_string(&batches)).expect("settings is a JSON object")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_setting_limits_its_users_and_survives_a_restart() {
    let ts = test_server().await;
    ts.sql("CREATE ROLE reader").await;
    let alice = user(&ts, "alice", Some("reader")).await;
    ts.try_sql_as(BUSY_SQL, alice.clone())
        .await
        .expect("no limit before the role sets one");

    ts.sql("ALTER ROLE reader SET query_cpu_limit_ms = 1").await;
    assert_over_budget(&ts, alice).await;
    assert_eq!(
        role_settings(&ts, "reader").await,
        serde_json::json!({ "query_cpu_limit_ms": "1" })
    );

    let ts = ts.restart().await;
    let alice = ts.login("alice", "pw").await;
    assert_over_budget(&ts, alice.clone()).await;

    ts.sql("ALTER ROLE reader RESET query_cpu_limit_ms").await;
    ts.try_sql_as(BUSY_SQL, alice)
        .await
        .expect("no limit after the reset");
    assert_eq!(role_settings(&ts, "reader").await, serde_json::json!({}));

    let ts = ts.restart().await;
    let alice = ts.login("alice", "pw").await;
    ts.try_sql_as(BUSY_SQL, alice)
        .await
        .expect("the reset also survives a restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_most_generous_role_wins_and_no_role_means_no_limit() {
    let ts = test_server().await;
    ts.sql("CREATE ROLE tight").await;
    ts.sql("CREATE ROLE premium").await;
    ts.sql("ALTER ROLE tight SET query_cpu_limit_ms = 1").await;
    ts.sql("ALTER ROLE premium SET query_cpu_limit_ms TO 0")
        .await;
    let limited = user(&ts, "limited", Some("tight")).await;
    user(&ts, "paying", Some("tight")).await;
    ts.sql("GRANT ROLE premium TO USER paying").await;
    let paying = ts.login("paying", "pw").await;
    let plain = user(&ts, "plain", None).await;

    assert_over_budget(&ts, limited).await;
    ts.try_sql_as(BUSY_SQL, paying)
        .await
        .expect("0 on one role beats the limit of the other");
    ts.try_sql_as(BUSY_SQL, plain)
        .await
        .expect("a user without a limiting role has no limit");
    ts.try_sql(BUSY_SQL)
        .await
        .expect("the super-user has no limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_user_cannot_raise_its_own_limit() {
    let ts = test_server().await;
    ts.sql("CREATE ROLE reader").await;
    let alice = user(&ts, "alice", Some("reader")).await;

    ts.try_sql_as("ALTER ROLE reader SET query_cpu_limit_ms = 0", alice)
        .await
        .expect_err("a role user must not raise its own limit");
    let error = ts
        .try_sql("ALTER ROLE reader SET query_cpu_limit_ms = soon")
        .await
        .expect_err("the value must be whole milliseconds");
    assert!(format!("{error:#}").contains("whole number"), "{error:#}");
}
