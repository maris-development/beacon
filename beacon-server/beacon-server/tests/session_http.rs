//! HTTP tests for `/api/auth/session` and session tokens on the existing routes.

mod common;

use std::time::Instant;

use ::axum::{
    body::Body,
    http::{header, Request, StatusCode},
    Router,
};
use beacon_server::axum::setup_router;
use common::{ADMIN_PASSWORD, ADMIN_USERNAME};
use futures::TryStreamExt;
use serde_json::Value;
use tower::ServiceExt;

async fn router_for(config: beacon_server_config::Config) -> (common::TestServer, Router) {
    let harness = common::server_with(config).await;
    let router = setup_router(harness.server.clone(), harness.server.config().clone()).unwrap();
    (harness, router)
}

async fn seed(harness: &common::TestServer, sql: &str) {
    harness
        .server
        .runtime()
        .run_query(
            beacon_core::query::Query::sql(sql.to_string()),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .unwrap()
        .into_record_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
}

fn sign_in(username: &str, password: &str, remember: bool) -> Request<Body> {
    let body = serde_json::json!({ "username": username, "password": password, "remember": remember });
    Request::builder()
        .method("POST")
        .uri("/api/auth/session")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn with_token(method: &str, uri: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = ::axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn token_for(router: &Router, username: &str, password: &str) -> String {
    let (status, body) = send(router, sign_in(username, password, false)).await;
    assert_eq!(status, StatusCode::CREATED);
    body["token"].as_str().unwrap().to_string()
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test(flavor = "multi_thread")]
async fn sign_in_check_and_sign_out() {
    let (_harness, router) = router_for(common::config(false)).await;
    let (status, body) = send(&router, sign_in(ADMIN_USERNAME, ADMIN_PASSWORD, false)).await;
    assert_eq!(status, StatusCode::CREATED);
    let token = body["token"].as_str().unwrap().to_string();
    assert!(token.starts_with("bst_"));
    assert_eq!(body["username"], ADMIN_USERNAME);
    assert_eq!(body["is_super_user"], true);

    let (status, body) = send(&router, with_token("GET", "/api/auth/session", Some(&token))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["username"], ADMIN_USERNAME);

    let (status, _) = send(&router, with_token("DELETE", "/api/auth/session", Some(&token))).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(&router, with_token("GET", "/api/auth/session", Some(&token))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_super_user_token_opens_the_admin_api() {
    let (_harness, router) = router_for(common::config(false)).await;
    let token = token_for(&router, ADMIN_USERNAME, ADMIN_PASSWORD).await;
    let (status, _) = send(&router, with_token("GET", "/api/admin/check", Some(&token))).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_normal_user_token_gets_403_on_the_admin_api() {
    let (harness, router) = router_for(common::config(false)).await;
    seed(&harness, "CREATE USER alice WITH PASSWORD 'pw'").await;
    let token = token_for(&router, "alice", "pw").await;
    let (status, _) = send(&router, with_token("GET", "/api/admin/check", Some(&token))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(&router, with_token("GET", "/admin/api/info", Some(&token))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_normal_user_token_works_on_client_routes() {
    let (harness, router) = router_for(common::config(false)).await;
    seed(&harness, "CREATE USER alice WITH PASSWORD 'pw'").await;
    let token = token_for(&router, "alice", "pw").await;
    let (status, _) = send(&router, with_token("GET", "/api/info", Some(&token))).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn sign_in_works_with_anonymous_access_off() {
    let mut config = common::config(false);
    config.auth.anonymous_enabled = false;
    let (_harness, router) = router_for(config).await;
    token_for(&router, ADMIN_USERNAME, ADMIN_PASSWORD).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remember_sets_the_long_ttl() {
    let (_harness, router) = router_for(common::config(false)).await;
    let (_, body) = send(&router, sign_in(ADMIN_USERNAME, ADMIN_PASSWORD, true)).await;
    assert_eq!(body["remember"], true);
    let ttl = body["expires_at"].as_i64().unwrap() - now();
    assert!((2_592_000 - 60..=2_592_000).contains(&ttl), "ttl was {ttl}");
}

#[tokio::test(flavor = "multi_thread")]
async fn remember_is_ignored_when_its_ttl_is_zero() {
    let mut config = common::config(false);
    config.auth.session_remember_ttl_secs = 0;
    let (_harness, router) = router_for(config).await;
    let (_, body) = send(&router, sign_in(ADMIN_USERNAME, ADMIN_PASSWORD, true)).await;
    assert_eq!(body["remember"], false);
    let ttl = body["expires_at"].as_i64().unwrap() - now();
    assert!((28_800 - 60..=28_800).contains(&ttl), "ttl was {ttl}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_sign_in_is_delayed_and_a_good_one_is_not() {
    let (_harness, router) = router_for(common::config(false)).await;

    let started = Instant::now();
    let (status, body) = send(&router, sign_in(ADMIN_USERNAME, "wrong", false)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, Value::Null, "a 401 carries no detail");
    assert!(started.elapsed().as_millis() >= 500);

    let started = Instant::now();
    token_for(&router, ADMIN_USERNAME, ADMIN_PASSWORD).await;
    assert!(started.elapsed().as_millis() < 500);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_anonymous_user_cannot_sign_in() {
    let (_harness, router) = router_for(common::config(false)).await;
    let (status, _) = send(&router, sign_in("anonymous", "", false)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread")]
async fn session_routes_reject_a_missing_or_foreign_token() {
    let (_harness, router) = router_for(common::config(false)).await;
    let (status, _) = send(&router, with_token("GET", "/api/auth/session", None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = send(&router, with_token("DELETE", "/api/auth/session", Some("a.b.c"))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = send(&router, with_token("GET", "/api/admin/check", Some("bst_unknown"))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
