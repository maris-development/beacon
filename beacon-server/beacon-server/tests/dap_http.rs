//! HTTP tests for the DAP2 endpoint (`/dap/{table}.dds|.das|.dods|.csv|.nc`), driven
//! through the real router with `tower::ServiceExt::oneshot`.

mod common;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, HeaderMap, Request, StatusCode},
    Router,
};
use common::{basic, config};
use serde_json::json;
use tower::ServiceExt;

use beacon_server::axum::setup_router;

async fn router_with(config: beacon_server_config::Config) -> (Router, common::TestServer, String) {
    let harness = common::server_with(config).await;
    let cfg = harness.server.config().clone();
    let admin = basic(&cfg.admin.username, &cfg.admin.password);
    let router = setup_router(harness.server.clone(), cfg).unwrap();
    (router, harness, admin)
}

async fn admin_sql(router: &Router, admin: &str, sql: &str) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, admin)
        .body(Body::from(json!({ "sql": sql }).to_string()))
        .unwrap();
    let res = router.clone().oneshot(request).await.unwrap();
    let status = res.status();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{sql}: {}",
        String::from_utf8_lossy(&body)
    );
}

/// GET `uri`. Returns the status, the headers and the body.
async fn get(router: &Router, uri: &str, auth: Option<&str>) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    let res = router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let body = to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, headers, body)
}

/// The table `obs`: time, numeric, boolean and text columns, comments and one null row.
async fn obs_table(router: &Router, admin: &str) {
    for sql in [
        "CREATE TABLE obs (id BIGINT, t TIMESTAMP, depth DOUBLE, temp REAL, ok BOOLEAN, platform VARCHAR)",
        "INSERT INTO obs VALUES \
         (1, TIMESTAMP '2024-01-01 00:00:00', 5.0, 10.5, true, 'SHIP'), \
         (2, TIMESTAMP '2024-01-02 00:00:00', 50.0, 4.25, false, 'BUOY'), \
         (3, NULL, NULL, NULL, NULL, NULL)",
        "COMMENT ON TABLE obs IS 'test observations'",
        "COMMENT ON COLUMN obs.depth IS 'depth in meters'",
    ] {
        admin_sql(router, admin, sql).await;
    }
}

fn text(body: Vec<u8>) -> String {
    String::from_utf8(body).expect("UTF-8 body")
}

#[tokio::test(flavor = "multi_thread")]
async fn dds_describes_the_table_as_one_sequence() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs.dds", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-description"], "dods-dds");
    assert_eq!(headers["xdods-server"], "dods/3.7");
    assert_eq!(
        text(body),
        "Dataset {\n    Sequence {\n        Float64 id;\n        Float64 t;\n        Float64 depth;\n        Float32 temp;\n        Int32 ok;\n        String platform;\n    } s;\n} obs;\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dds_lists_only_the_projected_columns_and_ignores_selections() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, _, body) = get(
        &router,
        "/dap/obs.dds?s.platform,s.depth&s.depth%3E10",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        text(body),
        "Dataset {\n    Sequence {\n        Float64 depth;\n        String platform;\n    } s;\n} obs;\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn das_gives_fill_values_comments_and_time_units() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs.das", None).await;
    let body = text(body);

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-description"], "dods-das");
    for part in [
        "        depth {\n            Float64 _FillValue NaN;\n            String comment \"depth in meters\";\n        }\n",
        "        t {\n            Float64 _FillValue NaN;\n            String units \"seconds since 1970-01-01T00:00:00Z\";\n        }\n",
        "        ok {\n            Int32 _FillValue 2147483647;\n        }\n",
        "        String summary \"test observations\";\n",
    ] {
        assert!(body.contains(part), "missing {part:?} in:\n{body}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_table_is_a_dap2_404() {
    let (router, _harness, _admin) = router_with(config(false)).await;

    let (status, headers, body) = get(&router, "/dap/nope.dds", None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(headers["content-description"], "dods-error");
    assert!(text(body).starts_with("Error {\n    code = 404;"));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_column_is_a_dap2_400() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, _, body) = get(&router, "/dap/obs.dds?nope", None).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text(body).contains("unknown column"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_without_a_grant_is_a_404() {
    let (router, _harness, admin) = router_with(config(true)).await;
    obs_table(&router, &admin).await;

    // The anonymous user has no grant on `obs`.
    assert_eq!(
        get(&router, "/dap/obs.dds", None).await.0,
        StatusCode::NOT_FOUND
    );

    for sql in [
        "CREATE ROLE reader",
        "GRANT SELECT ON TABLE obs TO ROLE reader",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "GRANT ROLE reader TO USER alice",
    ] {
        admin_sql(&router, &admin, sql).await;
    }
    let alice = basic("alice", "pw");
    assert_eq!(
        get(&router, "/dap/obs.dds", Some(&alice)).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_credentials_without_anonymous_access_is_a_basic_challenge() {
    let mut config = config(false);
    config.auth.anonymous_enabled = false;
    let (router, _harness, _admin) = router_with(config).await;

    let (status, headers, body) = get(&router, "/dap/obs.dds", None).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Basic realm=\"beacon\"");
    assert_eq!(headers["content-description"], "dods-error");
    assert!(text(body).starts_with("Error {\n    code = 401;"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_index_page_links_the_responses() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, _, body) = get(&router, "/dap/obs", None).await;
    let body = text(body);

    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("obs.dds") && body.contains("obs.das"),
        "{body}"
    );
}
