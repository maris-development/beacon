//! HTTP integration tests for the RBAC surface, driven through the real router
//! with `tower::ServiceExt::oneshot`:
//! - the admin auth list endpoints (`/api/admin/auth/{users,roles}`) and their
//!   super-user gate,
//! - the SQL-driven user/role/grant lifecycle (create → grant/deny → assign →
//!   revoke → drop) reflected in those endpoints,
//! - the model guards (read-only roles, reserved super-user, protected anonymous
//!   user, non-super users can't manage or enumerate auth),
//! - with enforcement on, that grants allow and denies block query results.

use std::sync::Arc;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use base64::{engine::general_purpose, Engine as _};
use crate::datalake::DataLake;
use futures::TryStreamExt;
use serde_json::Value;
use tower::ServiceExt;

use super::setup_router;

/// Config with explicit auth + SQL settings; everything else from the environment.
fn config(enforce: bool) -> Arc<beacon_datalake_config::Config> {
    let mut config = beacon_datalake_config::Config::load().expect("load config");
    config.auth.enforce = enforce;
    config.auth.anonymous_enabled = true;
    config.sql.enable = true;
    Arc::new(config)
}

async fn runtime(config: Arc<beacon_datalake_config::Config>) -> Arc<DataLake> {
    Arc::new(
        Runtime::new_with_in_memory_auth(config)
            .await
            .expect("runtime should start"),
    )
}

fn basic(username: &str, password: &str) -> String {
    format!(
        "Basic {}",
        general_purpose::STANDARD.encode(format!("{username}:{password}"))
    )
}

/// A unique name so managed tables don't collide with leftovers from other tests
/// (the tables store is shared across runtimes in this process).
fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

fn get(uri: &str, auth: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    builder.body(Body::empty()).unwrap()
}

fn post_query(sql: &str, auth: Option<&str>) -> Request<Body> {
    let body = serde_json::json!({ "sql": sql }).to_string();
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    builder.body(Body::from(body)).unwrap()
}

async fn send(router: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

/// Run a query as the super-user and assert it returns 200 (auth DDL yields no rows).
async fn admin_ok(router: &Router, admin: &str, sql: &str) {
    let (status, _) = send(router, post_query(sql, Some(admin))).await;
    assert_eq!(status, StatusCode::OK, "expected 200 for: {sql}");
}

fn json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("response should be JSON")
}

/// Seed managed tables (as the system identity) so enforcement tests have data.
async fn seed(runtime: &Runtime, sql: &str) {
    runtime
        .run_query(
            beacon_core::query::Query::sql(sql.to_string()),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .unwrap_or_else(|e| panic!("seed failed: {sql}: {e}"))
        .into_record_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_or_else(|e| panic!("seed drain failed: {sql}: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_endpoints_gated_and_list_default_principals() {
    let config = config(false);
    let runtime = runtime(config.clone()).await;
    let router = setup_router(runtime, config.clone()).unwrap();
    let admin = basic(&config.admin.username, &config.admin.password);

    // A stored, non-super user to exercise the 403 path.
    admin_ok(&router, &admin, "CREATE USER alice WITH PASSWORD 'pw'").await;
    let alice = basic("alice", "pw");

    // No credentials → 401; non-super → 403; super → 200.
    assert_eq!(
        send(&router, get("/api/admin/auth/users", None)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(&router, get("/api/admin/auth/users", Some(&alice))).await.0,
        StatusCode::FORBIDDEN
    );
    let (status, body) = send(&router, get("/api/admin/auth/users", Some(&admin))).await;
    assert_eq!(status, StatusCode::OK);

    let users = json(&body);
    let arr = users.as_array().unwrap();
    // The config super-user is flagged and present.
    assert!(arr.iter().any(|u| {
        u["username"] == config.admin.username.as_str() && u["is_super_user"] == Value::Bool(true)
    }));
    // The anonymous user is flagged.
    assert!(arr
        .iter()
        .any(|u| u["username"] == "anonymous" && u["is_anonymous"] == Value::Bool(true)));
    // alice is a plain user.
    assert!(arr.iter().any(|u| {
        u["username"] == "alice"
            && u["is_super_user"] == Value::Bool(false)
            && u["is_anonymous"] == Value::Bool(false)
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn rbac_lifecycle_reflected_in_endpoints() {
    let config = config(false);
    let runtime = runtime(config.clone()).await;
    let router = setup_router(runtime, config.clone()).unwrap();
    let admin = basic(&config.admin.username, &config.admin.password);

    // Create + grant + deny + assign.
    admin_ok(&router, &admin, "CREATE ROLE reader").await;
    admin_ok(&router, &admin, "GRANT SELECT ON TABLE observations TO ROLE reader").await;
    admin_ok(&router, &admin, "GRANT SELECT ON PATH 'argo/**/*.nc' TO ROLE reader").await;
    admin_ok(&router, &admin, "DENY SELECT ON TABLE secret TO ROLE reader").await;
    admin_ok(&router, &admin, "CREATE USER alice WITH PASSWORD 'pw'").await;
    admin_ok(&router, &admin, "GRANT ROLE reader TO USER alice").await;

    // Roles endpoint reflects the grants and denies.
    let roles = json(&send(&router, get("/api/admin/auth/roles", Some(&admin))).await.1);
    let reader = roles
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "reader")
        .expect("reader role present");
    let grants = reader["grants"].as_array().unwrap();
    assert!(grants.iter().any(|g| {
        g["privilege"] == "SELECT" && g["target_type"] == "table" && g["target_value"] == "observations"
    }));
    assert!(grants
        .iter()
        .any(|g| g["target_type"] == "path" && g["target_value"] == "argo/**/*.nc"));
    let denies = reader["denies"].as_array().unwrap();
    assert!(denies
        .iter()
        .any(|d| d["target_type"] == "table" && d["target_value"] == "secret"));

    // Users endpoint reflects the role assignment.
    let users = json(&send(&router, get("/api/admin/auth/users", Some(&admin))).await.1);
    let alice = users
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["username"] == "alice")
        .expect("alice present");
    assert_eq!(alice["roles"], serde_json::json!(["reader"]));

    // Revoke + drop, then confirm the state is gone.
    admin_ok(&router, &admin, "REVOKE SELECT ON TABLE observations FROM ROLE reader").await;
    admin_ok(&router, &admin, "REVOKE DENY SELECT ON TABLE secret FROM ROLE reader").await;
    admin_ok(&router, &admin, "REVOKE ROLE reader FROM USER alice").await;
    admin_ok(&router, &admin, "DROP USER alice").await;

    let users = json(&send(&router, get("/api/admin/auth/users", Some(&admin))).await.1);
    assert!(!users.as_array().unwrap().iter().any(|u| u["username"] == "alice"));

    let roles = json(&send(&router, get("/api/admin/auth/roles", Some(&admin))).await.1);
    let reader = roles
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "reader")
        .expect("reader still exists");
    assert!(reader["grants"]
        .as_array()
        .unwrap()
        .iter()
        .all(|g| g["target_value"] != "observations"));
    assert!(reader["denies"].as_array().unwrap().is_empty());

    admin_ok(&router, &admin, "DROP ROLE reader").await;
    let roles = json(&send(&router, get("/api/admin/auth/roles", Some(&admin))).await.1);
    assert!(!roles.as_array().unwrap().iter().any(|r| r["name"] == "reader"));
}

#[tokio::test(flavor = "multi_thread")]
async fn model_guards_are_enforced() {
    let config = config(false);
    let runtime = runtime(config.clone()).await;
    let router = setup_router(runtime, config.clone()).unwrap();
    let admin = basic(&config.admin.username, &config.admin.password);
    let admin_name = config.admin.username.clone();

    admin_ok(&router, &admin, "CREATE ROLE reader").await;

    // Roles are read-only: only SELECT may be granted.
    let bad = send(&router, post_query("GRANT INSERT ON TABLE t TO ROLE reader", Some(&admin))).await;
    assert_eq!(bad.0, StatusCode::BAD_REQUEST);

    // The super-user username is reserved (can't be created or dropped via SQL).
    assert_eq!(
        send(&router, post_query(&format!("DROP USER {admin_name}"), Some(&admin))).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &router,
            post_query(&format!("CREATE USER {admin_name} WITH PASSWORD 'x'"), Some(&admin))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );

    // The anonymous user can't be deleted while anonymous access is enabled.
    assert_eq!(
        send(&router, post_query("DROP USER anonymous", Some(&admin))).await.0,
        StatusCode::BAD_REQUEST
    );
    let users = json(&send(&router, get("/api/admin/auth/users", Some(&admin))).await.1);
    assert!(users.as_array().unwrap().iter().any(|u| u["username"] == "anonymous"));
}

#[tokio::test(flavor = "multi_thread")]
async fn non_super_user_cannot_manage_or_enumerate_auth() {
    let config = config(false);
    let runtime = runtime(config.clone()).await;
    let router = setup_router(runtime, config.clone()).unwrap();
    let admin = basic(&config.admin.username, &config.admin.password);

    admin_ok(&router, &admin, "CREATE USER bob WITH PASSWORD 'pw'").await;
    let bob = basic("bob", "pw");

    // Auth DDL requires the super-user → 400 for bob.
    assert_eq!(
        send(&router, post_query("CREATE ROLE hacker", Some(&bob))).await.0,
        StatusCode::BAD_REQUEST
    );
    // The admin enumeration endpoints reject non-super principals → 403.
    assert_eq!(
        send(&router, get("/api/admin/auth/roles", Some(&bob))).await.0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforcement_grants_allow_and_denies_block() {
    let config = config(true);
    let runtime = runtime(config.clone()).await;
    let admin = basic(&config.admin.username, &config.admin.password);

    // Two tables with a row each (unique names to avoid cross-test collisions).
    let t1 = unique("t1");
    let t2 = unique("t2");
    seed(&runtime, &format!("CREATE TABLE {t1} (a BIGINT)")).await;
    seed(&runtime, &format!("INSERT INTO {t1} VALUES (1)")).await;
    seed(&runtime, &format!("CREATE TABLE {t2} (a BIGINT)")).await;
    seed(&runtime, &format!("INSERT INTO {t2} VALUES (2)")).await;

    let router = setup_router(runtime, config.clone()).unwrap();

    // reader gets a global SELECT grant; alice gets the role.
    admin_ok(&router, &admin, "CREATE ROLE reader").await;
    admin_ok(&router, &admin, "GRANT SELECT TO ROLE reader").await;
    admin_ok(&router, &admin, "CREATE USER alice WITH PASSWORD 'pw'").await;
    admin_ok(&router, &admin, "GRANT ROLE reader TO USER alice").await;
    let alice = basic("alice", "pw");

    // The global grant lets alice read both tables.
    assert_eq!(
        send(&router, post_query(&format!("SELECT * FROM {t1}"), Some(&alice))).await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(&router, post_query(&format!("SELECT * FROM {t2}"), Some(&alice))).await.0,
        StatusCode::OK
    );

    // A deny on t1 wins over the grant; t2 is still readable.
    admin_ok(&router, &admin, &format!("DENY SELECT ON TABLE {t1} TO ROLE reader")).await;
    assert_eq!(
        send(&router, post_query(&format!("SELECT * FROM {t1}"), Some(&alice))).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(&router, post_query(&format!("SELECT * FROM {t2}"), Some(&alice))).await.0,
        StatusCode::OK
    );

    // A user with no roles can't read at all.
    admin_ok(&router, &admin, "CREATE USER mallory WITH PASSWORD 'pw'").await;
    let mallory = basic("mallory", "pw");
    assert_eq!(
        send(&router, post_query(&format!("SELECT * FROM {t2}"), Some(&mallory))).await.0,
        StatusCode::BAD_REQUEST
    );
}
