//! The `query_cpu_limit_ms` role setting, end to end: SQL sets it, the default
//! CPU policy applies it to the users of the role, and it survives a restart.
//! The generic key-value behavior is in `role_settings.rs`.

mod common;

use beacon_core::query_cpu::CpuBudget;
use beacon_core::{AuthIdentity, Credential};
use common::{restartable_runtime, runtime_with, TestRuntime};
use std::time::Duration;

/// A query that needs far more than 1 ms of CPU time.
const BUSY_SQL: &str = "SELECT count(*) FROM generate_series(1, 20000000) AS t(v) WHERE v % 7 = 3";

async fn user(rt: &TestRuntime, name: &str, role: Option<&str>) -> AuthIdentity {
    rt.sql(&format!("CREATE USER {name} WITH PASSWORD 'pw'"))
        .await;
    if let Some(role) = role {
        rt.sql(&format!("GRANT ROLE {role} TO USER {name}")).await;
    }
    login(rt, name).await
}

async fn login(rt: &TestRuntime, name: &str) -> AuthIdentity {
    rt.runtime
        .authenticate(&Credential::basic(name, "pw"))
        .await
        .expect("the user should authenticate")
}

async fn assert_over_budget(rt: &TestRuntime, identity: AuthIdentity) {
    let error = rt
        .try_sql_as(BUSY_SQL, identity)
        .await
        .expect_err("the query should go over its CPU budget");
    assert!(format!("{error:#}").contains("CPU budget"), "{error:#}");
}

async fn role_settings(rt: &TestRuntime, role: &str) -> serde_json::Value {
    let batches = rt
        .sql(&format!(
            "SELECT settings FROM beacon.system.roles WHERE role_name = '{role}'"
        ))
        .await;
    serde_json::from_str(&common::scalar_string(&batches)).expect("settings is a JSON object")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_setting_limits_its_users_and_survives_a_restart() {
    let rt = restartable_runtime("role-cpu-limit", |builder| builder).await;
    rt.sql("CREATE ROLE reader").await;
    let alice = user(&rt, "alice", Some("reader")).await;
    rt.try_sql_as(BUSY_SQL, alice.clone())
        .await
        .expect("no limit before the role sets one");

    rt.sql("ALTER ROLE reader SET query_cpu_limit_ms = 1").await;
    assert_over_budget(&rt, alice).await;
    assert_eq!(
        role_settings(&rt, "reader").await,
        serde_json::json!({ "query_cpu_limit_ms": "1" })
    );

    let rt = rt.restart().await;
    let alice = login(&rt, "alice").await;
    assert_over_budget(&rt, alice.clone()).await;

    rt.sql("ALTER ROLE reader RESET query_cpu_limit_ms").await;
    rt.try_sql_as(BUSY_SQL, alice)
        .await
        .expect("no limit after the reset");
    assert_eq!(role_settings(&rt, "reader").await, serde_json::json!({}));

    let rt = rt.restart().await;
    let alice = login(&rt, "alice").await;
    rt.try_sql_as(BUSY_SQL, alice)
        .await
        .expect("the reset also survives a restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_setting_replaces_the_default_budget() {
    let tiny = CpuBudget::Limited(Duration::from_micros(1));
    let rt = runtime_with("role-cpu-default", move |builder| {
        builder.with_query_cpu_budget(tiny)
    })
    .await;
    rt.sql("CREATE ROLE premium").await;
    rt.sql("ALTER ROLE premium SET query_cpu_limit_ms TO 0")
        .await;
    let paying = user(&rt, "paying", Some("premium")).await;
    let plain = user(&rt, "plain", None).await;

    rt.try_sql_as(BUSY_SQL, paying)
        .await
        .expect("0 on the role means no limit");
    assert_over_budget(&rt, plain).await;
    rt.try_sql(BUSY_SQL)
        .await
        .expect("the super-user has no limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_user_cannot_raise_its_own_limit() {
    let rt = runtime_with("role-cpu-authz", |builder| builder).await;
    rt.sql("CREATE ROLE reader").await;
    let alice = user(&rt, "alice", Some("reader")).await;

    rt.try_sql_as("ALTER ROLE reader SET query_cpu_limit_ms = 0", alice)
        .await
        .expect_err("a role user must not raise its own limit");
    let error = rt
        .try_sql("ALTER ROLE reader SET query_cpu_limit_ms = soon")
        .await
        .expect_err("the value must be whole milliseconds");
    assert!(format!("{error:#}").contains("whole number"), "{error:#}");
}
