//! HTTP-transport integration tests for the auth middleware: the `resolve_identity` client
//! middleware and the super-user `basic_auth` admin gate, driven through the real router with
//! `tower::ServiceExt::oneshot`.

mod common;

use std::sync::Arc;

use ::axum::{
    body::Body,
    http::{header, Request, StatusCode},
    Router,
};
use beacon_core::runtime::Runtime;
use common::{basic, config, unique};
use futures::TryStreamExt;
use tower::ServiceExt;

use beacon_server::axum::setup_router;

/// Opens an ephemeral server from `config` and returns it with the `Arc<Config>`
/// the router should use — the server's own config, which carries the ephemeral
/// temp paths but keeps the auth/admin settings unchanged.
async fn app(
    config: beacon_server_config::Config,
) -> (common::TestServer, Arc<beacon_server_config::Config>) {
    let harness = common::server_with(config).await;
    let cfg = harness.server.config().clone();
    (harness, cfg)
}

/// Runs setup SQL as a super-user directly against the runtime (test fixture).
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

async fn status(router: &Router, req: Request<Body>) -> StatusCode {
    router.clone().oneshot(req).await.unwrap().status()
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_route_requires_a_super_user() {
    let (harness, config) = app(config(false)).await;
    // A non-super-user to exercise the 403 path.
    seed(harness.server.runtime(), "CREATE ROLE reader").await;
    seed(harness.server.runtime(), "CREATE USER alice WITH PASSWORD 'pw'").await;
    seed(harness.server.runtime(), "GRANT ROLE reader TO USER alice").await;

    let router = setup_router(harness.server.clone(), config.clone()).unwrap();

    // No credentials → 401.
    assert_eq!(
        status(&router, get("/api/admin/check", None)).await,
        StatusCode::UNAUTHORIZED
    );
    // Valid but non-super credentials → 403.
    assert_eq!(
        status(&router, get("/api/admin/check", Some(&basic("alice", "pw")))).await,
        StatusCode::FORBIDDEN
    );
    // Wrong password → 401.
    assert_eq!(
        status(
            &router,
            get(
                "/api/admin/check",
                Some(&basic(&config.admin.username, "wrong"))
            )
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    // Admin (super-user) → 200.
    assert_eq!(
        status(
            &router,
            get(
                "/api/admin/check",
                Some(&basic(&config.admin.username, &config.admin.password))
            )
        )
        .await,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn client_query_anonymous_reads_but_cannot_write() {
    let (harness, config) = app(config(false)).await;
    let router = setup_router(harness.server.clone(), config.clone()).unwrap();
    let table = unique("t");

    // Anonymous SELECT is allowed (read-only, enforcement off).
    assert_eq!(
        status(&router, post_query("SELECT 1", None)).await,
        StatusCode::OK
    );
    // Anonymous DDL is rejected (non-super) → handler maps the error to 400.
    assert_eq!(
        status(
            &router,
            post_query(&format!("CREATE TABLE {table} (a BIGINT)"), None)
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    // The admin can run the same DDL → 200.
    assert_eq!(
        status(
            &router,
            post_query(
                &format!("CREATE TABLE {table} (a BIGINT)"),
                Some(&basic(&config.admin.username, &config.admin.password))
            )
        )
        .await,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_http_query_respects_table_grants() {
    let (harness, config) = app(config(true)).await;
    let rt = harness.server.runtime();

    let t1 = unique("t1");
    let t2 = unique("t2");
    seed(rt, &format!("CREATE TABLE {t1} (a BIGINT)")).await;
    seed(rt, &format!("INSERT INTO {t1} VALUES (1)")).await;
    seed(rt, &format!("CREATE TABLE {t2} (a BIGINT)")).await;
    seed(rt, &format!("INSERT INTO {t2} VALUES (2)")).await;
    seed(rt, "CREATE ROLE reader").await;
    seed(rt, "CREATE USER alice WITH PASSWORD 'pw'").await;
    seed(rt, "GRANT ROLE reader TO USER alice").await;
    seed(rt, &format!("GRANT SELECT ON TABLE {t1} TO ROLE reader")).await;

    let router = setup_router(harness.server.clone(), config).unwrap();
    let auth = basic("alice", "pw");

    // Granted table → 200.
    assert_eq!(
        status(&router, post_query(&format!("SELECT * FROM {t1}"), Some(&auth))).await,
        StatusCode::OK
    );
    // Ungranted table → permission denied, surfaced as 400.
    assert_eq!(
        status(&router, post_query(&format!("SELECT * FROM {t2}"), Some(&auth))).await,
        StatusCode::BAD_REQUEST
    );
}
