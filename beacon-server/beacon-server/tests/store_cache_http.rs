//! HTTP tests for the datasets store cache: stale until a sync, write-through
//! for uploads and deletes, and the admin endpoints.

mod common;

use std::sync::Arc;
use std::time::Duration;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use common::basic;
use serde_json::Value;
use tower::ServiceExt;

use beacon_server::axum::setup_router;

/// Server config with the store cache on. A long interval, so only a sync
/// request or a write through the server can change the cache.
fn cached_config() -> beacon_server_config::Config {
    let mut config = common::config(false);
    config.store_cache.enable = true;
    config.store_cache.sync_interval = Duration::from_secs(3600);
    config
}

async fn app(
    config: beacon_server_config::Config,
) -> (Router, common::TestServer, Arc<beacon_server_config::Config>, String) {
    let harness = common::server_with(config).await;
    let cfg = harness.server.config().clone();
    let router = setup_router(harness.server.clone(), cfg.clone()).unwrap();
    let admin = basic(&cfg.admin.username, &cfg.admin.password);
    (router, harness, cfg, admin)
}

async fn send(router: &Router, method: &str, uri: &str, auth: Option<&str>, body: Body) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    let request = builder
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn fixture() -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet");
    std::fs::read(path).expect("read parquet fixture")
}

/// Write a file into the datasets folder without the server, as a different program does.
fn place_external(cfg: &beacon_server_config::Config, rel: &str) {
    let path = cfg.data.datasets.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, fixture()).unwrap();
}

async fn total(router: &Router) -> u64 {
    let (status, body) = send(router, "GET", "/api/total-datasets", None, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    body.as_u64().expect("a dataset count")
}

#[tokio::test(flavor = "multi_thread")]
async fn external_files_show_after_a_sync_and_uploads_show_at_once() {
    let (router, _harness, cfg, admin) = app(cached_config()).await;
    place_external(&cfg, "obs/a.parquet");
    assert_eq!(total(&router).await, 1);

    // A file from a different program stays hidden until a sync finds it.
    place_external(&cfg, "obs/b.parquet");
    assert_eq!(total(&router).await, 1, "the cache answers the listing");
    let (status, report) = send(&router, "POST", "/api/admin/store-cache/sync", Some(&admin), Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(report["synced"], true);
    assert_eq!(total(&router).await, 2);

    // An upload through the server shows at once.
    let upload = "/api/admin/datasets/upload?path=obs/c.parquet";
    let (status, _) = send(&router, "POST", upload, Some(&admin), Body::from(fixture())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(total(&router).await, 3);

    // An upload must not replace a file that the cache does not know.
    place_external(&cfg, "obs/d.parquet");
    let hidden = "/api/admin/datasets/upload?path=obs/d.parquet";
    let (status, _) = send(&router, "POST", hidden, Some(&admin), Body::from(fixture())).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(total(&router).await, 4, "the store read for the check corrects the cache");

    // A delete through the server shows at once.
    let delete = "/api/admin/datasets?path=obs/c.parquet";
    let (status, _) = send(&router, "DELETE", delete, Some(&admin), Body::empty()).await;
    assert!(status.is_success(), "delete failed: {status}");
    assert_eq!(total(&router).await, 3);

    let (status, cache) = send(&router, "GET", "/api/admin/store-cache", Some(&admin), Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(cache["hits"].as_u64().unwrap() > 0, "status: {cache}");
    assert!(cache["entries"].as_u64().unwrap() >= 3, "status: {cache}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_background_sync_finds_external_files() {
    let mut config = cached_config();
    config.store_cache.sync_interval = Duration::from_secs(1);
    let (router, _harness, cfg, _admin) = app(config).await;
    place_external(&cfg, "obs/a.parquet");
    assert_eq!(total(&router).await, 1);

    place_external(&cfg, "obs/b.parquet");
    for _ in 0..200 {
        if total(&router).await == 2 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the background sync did not find the new file");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_endpoints_answer_conflict_when_the_cache_is_off() {
    let (router, _harness, _cfg, admin) = app(common::config(false)).await;
    for (method, uri) in [
        ("GET", "/api/admin/store-cache"),
        ("POST", "/api/admin/store-cache/sync"),
    ] {
        let (status, _) = send(&router, method, uri, Some(&admin), Body::empty()).await;
        assert_eq!(status, StatusCode::CONFLICT, "{method} {uri}");
    }
    let (status, _) = send(&router, "GET", "/api/admin/store-cache", None, Body::empty()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
