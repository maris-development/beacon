//! HTTP-transport integration tests for the bundled admin web UI, driven through
//! the real router with `tower::ServiceExt::oneshot`.
//!
//! The SPA asks for its assets with URLs relative to the document, because one
//! build cannot know the base path a deployment configures. These tests hold the
//! server to its half of that contract: the app root ends in a slash, and every
//! file below it answers under the configured prefix.

mod common;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use tempfile::TempDir;
use tower::ServiceExt;

use beacon_server::axum::setup_router;

const INDEX_HTML: &str = "<!doctype html><html><body>beacon admin</body></html>";
const ASSET_JS: &str = "console.log('beacon');";

/// A directory shaped like a Vite build: `index.html` plus one hashed asset.
fn web_build() -> TempDir {
    let dir = tempfile::tempdir().expect("create temp web dir");
    std::fs::write(dir.path().join("index.html"), INDEX_HTML).expect("write index.html");
    std::fs::create_dir(dir.path().join("assets")).expect("create assets dir");
    std::fs::write(dir.path().join("assets/index-abc123.js"), ASSET_JS).expect("write asset");
    dir
}

/// A router that serves `web` at `{base_path}/admin`, plus the server backing it.
///
/// The harness has to outlive the router: dropping it removes the temp root.
async fn app(base_path: &str, web: &TempDir) -> (common::TestServer, Router) {
    let mut config = common::config(false);
    config.server.base_path = base_path.to_string();
    config.server.web_ui_dir = web.path().to_string_lossy().into_owned();

    let harness = common::server_with(config).await;
    let router = setup_router(harness.server.clone(), harness.server.config().clone())
        .expect("router should build");
    (harness, router)
}

/// Sends a `GET` and returns the status, the `Location` header, and the body.
async fn get(router: &Router, uri: &str) -> (StatusCode, Option<String>, String) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("build request");
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("router responds");

    let status = response.status();
    let location = response
        .headers()
        .get(header::LOCATION)
        .map(|value| value.to_str().expect("Location is text").to_string());
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    (
        status,
        location,
        String::from_utf8_lossy(&body).into_owned(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_root_redirects_to_a_trailing_slash() {
    let web = web_build();

    for (base_path, admin, app_root) in [
        ("", "/admin", "/admin/"),
        ("/beacon", "/beacon/admin", "/beacon/admin/"),
        // A prefix that contains the word `admin` must not confuse the mount.
        (
            "/my-admin/deep",
            "/my-admin/deep/admin",
            "/my-admin/deep/admin/",
        ),
    ] {
        let (_harness, router) = app(base_path, &web).await;

        let (status, location, _) = get(&router, admin).await;
        assert_eq!(status, StatusCode::SEE_OTHER, "base path {base_path:?}");
        assert_eq!(
            location.as_deref(),
            Some(app_root),
            "base path {base_path:?}"
        );

        // The target itself serves the page, so the redirect terminates.
        let (status, _, body) = get(&router, app_root).await;
        assert_eq!(status, StatusCode::OK, "base path {base_path:?}");
        assert_eq!(body, INDEX_HTML, "base path {base_path:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_assets_and_client_routes_answer_under_the_base_path() {
    let web = web_build();
    let (_harness, router) = app("/beacon", &web).await;

    // An asset URL relative to the app root, as the built `index.html` writes it.
    let (status, _, body) = get(&router, "/beacon/admin/assets/index-abc123.js").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, ASSET_JS);

    // A deep client-side route on a hard reload falls back to the SPA shell.
    let (status, _, body) = get(&router, "/beacon/admin/tables").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, INDEX_HTML);

    // The same asset outside the prefix is not the SPA shell dressed as a script.
    let (status, _, _) = get(&router, "/admin/assets/index-abc123.js").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_api_stays_reachable_beside_the_web_ui() {
    let web = web_build();
    let (_harness, router) = app("/beacon", &web).await;

    // The UI derives its API base by dropping `/admin` from the app root, so the
    // API has to answer one level up from the SPA mount.
    let (status, _, _) = get(&router, "/beacon/api/health").await;
    assert_eq!(status, StatusCode::OK);

    // Admin endpoints still gate on credentials rather than 404 behind the mount.
    let (status, _, _) = get(&router, "/beacon/api/admin/check").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// The API alias and the SPA share the `/admin` prefix. The alias paths are exact
/// routes and the SPA is a catch-all below them, so `/admin/api/...` has to reach
/// the API. A `401` proves it: the app shell answers `200` to anything it catches.
#[tokio::test(flavor = "multi_thread")]
async fn the_api_alias_wins_over_the_spa_catch_all() {
    let web = web_build();

    for (base_path, alias, spa_route) in [
        ("", "/admin/api/info", "/admin/tables"),
        ("/beacon", "/beacon/admin/api/info", "/beacon/admin/tables"),
    ] {
        let (_harness, router) = app(base_path, &web).await;

        let (status, _, body) = get(&router, alias).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "base path {base_path:?}: {alias} should reach the gated API"
        );
        assert_ne!(body, INDEX_HTML, "base path {base_path:?}");

        // An unclaimed alias path is the API's 404, not the app shell. The gate
        // in front of the alias answers first, which is already not the shell.
        let (status, _, body) = get(&router, &format!("{alias}-nope")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "base path {base_path:?}");
        assert_ne!(body, INDEX_HTML, "base path {base_path:?}");

        // A client-side route beside it still falls back to the app shell.
        let (status, _, body) = get(&router, spa_route).await;
        assert_eq!(status, StatusCode::OK, "base path {base_path:?}");
        assert_eq!(body, INDEX_HTML, "base path {base_path:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn no_web_ui_directory_leaves_the_admin_path_unmounted() {
    let empty = tempfile::tempdir().expect("create temp dir");
    let mut config = common::config(false);
    config.server.web_ui_dir = empty.path().to_string_lossy().into_owned();

    let harness = common::server_with(config).await;
    let router = setup_router(harness.server.clone(), harness.server.config().clone())
        .expect("router should build");

    let (status, _, _) = get(&router, "/admin").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
