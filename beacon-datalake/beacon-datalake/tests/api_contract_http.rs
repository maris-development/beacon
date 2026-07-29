//! Contract test: every endpoint the shipped clients call must be routed.
//!
//! The admin web UI and the TypeScript SDK address the API by hard-coded path.
//! Nothing links them to the Rust router, so deleting a handler compiles, passes
//! every other test, and only fails in the browser as a 404. That has already
//! happened three times on this branch (`/api/explain-query`,
//! `/api/explain-analyze-query`, `/api/query/metrics/{id}`, `/api/functions`,
//! `/api/table-functions`, `/api/admin/table-config` were all dropped by "wip"
//! commits while their callers stayed).
//!
//! This test pins the wire contract by asserting each client-called path is
//! present in the router's own OpenAPI document. Add a client call → add it
//! here; intentionally retire an endpoint → remove both, deliberately.

mod common;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use common::{basic, config, ADMIN_PASSWORD, ADMIN_USERNAME};
use serde_json::Value;
use tower::ServiceExt;

use beacon_datalake::axum::setup_router;

/// Every (method, path) the TypeScript SDK / admin UI calls, as written in
/// `beacon-datalake-clients/beacon-ts/src/client.ts`. Paths use the OpenAPI
/// `{param}` spelling.
const CLIENT_ENDPOINTS: &[(&str, &str)] = &[
    // -- query ---------------------------------------------------------------
    ("post", "/api/query"),
    ("post", "/api/parse-query"),
    ("post", "/api/explain-query"),
    ("post", "/api/explain-analyze-query"),
    ("get", "/api/query/metrics/{query_id}"),
    // -- tables --------------------------------------------------------------
    ("get", "/api/tables"),
    ("get", "/api/tables-with-schema"),
    ("get", "/api/table-schema"),
    ("get", "/api/default-table"),
    ("get", "/api/default-table-schema"),
    // -- datasets ------------------------------------------------------------
    ("get", "/api/list-datasets"),
    ("get", "/api/dataset-schema"),
    ("get", "/api/total-datasets"),
    // -- functions & info ----------------------------------------------------
    ("get", "/api/functions"),
    ("get", "/api/table-functions"),
    ("get", "/api/info"),
    // -- admin ---------------------------------------------------------------
    ("get", "/api/admin/check"),
    ("get", "/api/admin/table-config"),
    ("get", "/api/admin/auth/users"),
    ("get", "/api/admin/auth/roles"),
    ("get", "/api/admin/crawlers"),
    ("post", "/api/admin/crawlers"),
    ("get", "/api/admin/crawlers/{name}"),
    ("delete", "/api/admin/crawlers/{name}"),
    ("post", "/api/admin/crawlers/{name}/run"),
    ("post", "/api/admin/external-tables"),
    ("delete", "/api/admin/datasets"),
    ("get", "/api/admin/datasets/download"),
    ("post", "/api/admin/datasets/upload"),
    ("post", "/api/admin/datasets/upload/initiate"),
    ("put", "/api/admin/datasets/upload/part"),
    ("post", "/api/admin/datasets/upload/complete"),
    ("delete", "/api/admin/datasets/upload"),
];

async fn app() -> (Router, common::TestLake) {
    let lake = common::lake_with(config(false)).await;
    let cfg = lake.lake.config().clone();
    let router = setup_router(lake.lake.clone(), cfg).unwrap();
    (router, lake)
}

async fn send(router: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap().to_vec();
    (status, body)
}

/// The router's own OpenAPI document, as served to clients.
async fn openapi(router: &Router) -> Value {
    let (status, body) = send(
        router,
        Request::builder()
            .method("GET")
            .uri("/openapi.json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "/openapi.json should be served");
    serde_json::from_slice(&body).expect("openapi.json should be JSON")
}

#[tokio::test(flavor = "multi_thread")]
async fn every_client_called_endpoint_is_routed() {
    let (router, _lake) = app().await;
    let spec = openapi(&router).await;
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("the OpenAPI document should have a `paths` object");

    let mut missing = Vec::new();
    for (method, path) in CLIENT_ENDPOINTS {
        match paths.get(*path) {
            Some(item) if item.get(*method).is_some() => {}
            Some(_) => missing.push(format!("{} {path} (path exists, method not served)", method.to_uppercase())),
            None => missing.push(format!("{} {path}", method.to_uppercase())),
        }
    }

    assert!(
        missing.is_empty(),
        "the shipped clients call endpoints the router does not serve — these 404 in the \
         admin UI:\n  {}",
        missing.join("\n  ")
    );
}

/// `/api/health` is a plain axum route rather than a documented one, so it is
/// checked by calling it instead of via the OpenAPI document.
#[tokio::test(flavor = "multi_thread")]
async fn health_endpoint_responds() {
    let (router, _lake) = app().await;
    let (status, body) = send(
        &router,
        Request::builder()
            .method("GET")
            .uri("/api/health")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(String::from_utf8_lossy(&body), "Ok");
}

/// The read-only GET endpoints the admin UI loads on page render must actually
/// answer, not merely be routed: a handler that 500s is as broken as a missing
/// one, and these are the calls behind the Workbench, Tables and Server Info
/// pages.
#[tokio::test(flavor = "multi_thread")]
async fn client_get_endpoints_answer_successfully() {
    let (router, _lake) = app().await;

    // (uri, admin auth required)
    let cases: &[(&str, bool)] = &[
        ("/api/info", false),
        ("/api/tables", false),
        ("/api/tables-with-schema", false),
        ("/api/default-table", false),
        ("/api/list-datasets", false),
        ("/api/total-datasets", false),
        ("/api/functions", false),
        ("/api/table-functions", false),
        ("/api/admin/check", true),
        ("/api/admin/auth/users", true),
        ("/api/admin/auth/roles", true),
        ("/api/admin/crawlers", true),
    ];

    let auth = basic(ADMIN_USERNAME, ADMIN_PASSWORD);
    for (uri, needs_admin) in cases {
        let mut builder = Request::builder().method("GET").uri(*uri);
        if *needs_admin {
            builder = builder.header(header::AUTHORIZATION, &auth);
        }
        let (status, body) = send(&router, builder.body(Body::empty()).unwrap()).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "GET {uri} should succeed, got {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
}

/// `/api/query/metrics/{id}` is consumed field-by-field by the workbench's
/// Metrics panel, so its JSON is a wire contract: every key must stay present
/// with its type. Renaming or dropping one silently blanks a tile.
#[tokio::test(flavor = "multi_thread")]
async fn query_metrics_json_keeps_its_documented_shape() {
    let (router, _lake) = app().await;

    let res = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"sql":"SELECT 1 AS v"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let query_id = res
        .headers()
        .get("x-beacon-query-id")
        .and_then(|v| v.to_str().ok())
        .expect("a successful query returns its id")
        .to_string();
    // Metrics consolidate as the stream ends, so it must be drained first.
    let _ = to_bytes(res.into_body(), usize::MAX).await.unwrap();

    let (status, body) = send(
        &router,
        Request::builder()
            .method("GET")
            .uri(format!("/api/query/metrics/{query_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let m: Value = serde_json::from_slice(&body).expect("metrics should be JSON");

    // (key, predicate, expected-type label)
    let contract: &[(&str, fn(&Value) -> bool, &str)] = &[
        ("input_rows", |v| v.is_u64(), "number"),
        ("input_bytes", |v| v.is_u64(), "number"),
        ("result_num_rows", |v| v.is_u64(), "number"),
        ("result_size_in_bytes", |v| v.is_u64(), "number"),
        ("execution_time_ms", |v| v.is_u64(), "number"),
        ("file_paths", |v| v.is_array(), "array"),
        ("query", |v| v.is_object(), "object"),
        ("query_id", |v| v.is_string(), "string"),
        ("parsed_logical_plan", |v| !v.is_null(), "non-null"),
        ("optimized_logical_plan", |v| !v.is_null(), "non-null"),
        ("node_metrics", |v| v.is_object(), "object"),
    ];
    for (key, ok, label) in contract {
        let value = m
            .get(*key)
            .unwrap_or_else(|| panic!("metrics must carry `{key}`, got: {m}"));
        assert!(ok(value), "metrics.{key} should be a {label}, got: {value}");
    }

    // The two counters the panel headlines must reflect the query that ran.
    assert_eq!(m["result_num_rows"], 1, "one row was selected");
    assert_eq!(m["query"]["sql"], "SELECT 1 AS v");

    // The plan fields are the pgjson `[{Plan: …}]` shape, and `node_metrics`
    // names the executed operator. Both were left empty by the pre-rewrite
    // runtime (nothing called `set_logical_plan` / `set_physical_plan`), so
    // this pins that they are actually populated now.
    assert!(
        m["parsed_logical_plan"][0]["Plan"]["Node Type"].is_string(),
        "parsed_logical_plan should be a pgjson plan, got: {}",
        m["parsed_logical_plan"]
    );
    assert!(
        m["node_metrics"]["operator"]
            .as_str()
            .is_some_and(|op| !op.is_empty()),
        "node_metrics should name the executed operator, got: {}",
        m["node_metrics"]
    );
}

/// `table-config` backs the Tables page's config panel. It reads the table's
/// *persisted definition*, so it needs a real managed table to answer for.
#[tokio::test(flavor = "multi_thread")]
async fn table_config_reports_a_created_table_and_404s_for_an_unknown_one() {
    let (router, lake) = app().await;
    lake.lake
        .runtime()
        .run_query(
            beacon_core::query::Query::sql("CREATE TABLE cfg (a BIGINT)".to_string()),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .expect("create table");

    let auth = basic(ADMIN_USERNAME, ADMIN_PASSWORD);
    let (status, body) = send(
        &router,
        Request::builder()
            .method("GET")
            .uri("/api/admin/table-config?table_name=cfg")
            .header(header::AUTHORIZATION, &auth)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "table-config should describe a created table, got {status}: {}",
        String::from_utf8_lossy(&body)
    );
    let config: Value = serde_json::from_slice(&body).expect("config should be JSON");
    assert!(
        config.as_object().is_some_and(|o| !o.is_empty()),
        "the config should be a non-empty object, got: {config}"
    );

    let (status, _) = send(
        &router,
        Request::builder()
            .method("GET")
            .uri("/api/admin/table-config?table_name=nope")
            .header(header::AUTHORIZATION, &auth)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "an unknown table should 404");
}

/// The two function listings back the SQL editor's autocomplete, which reads
/// `function_name` and a `params` array off each entry. A shape change silently
/// empties the completion popup, so the contract is pinned here.
#[tokio::test(flavor = "multi_thread")]
async fn function_listings_carry_the_shape_autocomplete_reads() {
    let (router, _lake) = app().await;

    for uri in ["/api/functions", "/api/table-functions"] {
        let (status, body) = send(
            &router,
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "GET {uri} should succeed");

        let listing: Value = serde_json::from_slice(&body).expect("listing should be JSON");
        let entries = listing
            .as_array()
            .unwrap_or_else(|| panic!("{uri} should return an array, got: {listing}"));
        assert!(!entries.is_empty(), "{uri} should list at least one function");

        let first = &entries[0];
        assert!(
            first.get("function_name").and_then(Value::as_str).is_some(),
            "{uri} entries need a `function_name` string, got: {first}"
        );
        assert!(
            first.get("params").map(Value::is_array).unwrap_or(false),
            "{uri} entries need a `params` array (autocomplete renders signatures \
             from it), got: {first}"
        );
    }
}
