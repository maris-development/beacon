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
//! `/api/table-functions` and `/api/admin/table-config` are deprecated but still
//! routed: nothing catalogs table-valued functions any more, and a table's
//! persisted definition is no longer served over HTTP, so they answer with an
//! empty list and a notice rather than disappearing out from under a client that
//! calls them.
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

use beacon_server::axum::setup_router;

/// Every (method, path) the TypeScript SDK / admin UI calls, as written in
/// `beacon-clients/beacon-ts/src/client.ts`. Paths use the OpenAPI
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
    // Deprecated, kept routed and answering `[]` — see the module docs.
    ("get", "/api/table-functions"),
    ("get", "/api/info"),
    // -- admin ---------------------------------------------------------------
    ("get", "/api/admin/check"),
    // Deprecated, kept routed and answering a notice — see the module docs.
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

async fn app() -> (Router, common::TestServer) {
    let harness = common::server_with(config(false)).await;
    let cfg = harness.server.config().clone();
    let router = setup_router(harness.server.clone(), cfg).unwrap();
    (router, harness)
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
        ("username", |v| v.is_string(), "string"),
        ("finished_at", |v| v.is_string(), "string"),
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

    // Who ran it and when. The request carried no credentials, so the caller is
    // the anonymous principal; the timestamp is the RFC 3339 rendering of the
    // table's `finished_at`.
    assert_eq!(m["username"], "anonymous");
    let finished_at = m["finished_at"].as_str().unwrap_or_default();
    assert!(
        // e.g. `2026-07-30T18:21:03.114`: a date, `T`, and a time — enough to
        // catch an empty or unformatted cell without pulling in a date parser.
        finished_at.len() >= 19 && finished_at.contains('T') && finished_at.starts_with("20"),
        "finished_at should be a timestamp, got: {finished_at:?}"
    );

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

/// `table-config` is retired: it answers every caller with a deprecation notice
/// instead of a definition, stays admin-only, and never 404s.
#[tokio::test(flavor = "multi_thread")]
async fn table_config_answers_with_a_deprecation_notice() {
    let (router, harness) = app().await;
    harness.server
        .runtime()
        .run_query(
            beacon_core::query::Query::sql("CREATE TABLE cfg (a BIGINT)".to_string()),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .expect("create table");

    let auth = basic(ADMIN_USERNAME, ADMIN_PASSWORD);
    // Same answer for a real table and an unknown one — there is nothing to look up.
    for table in ["cfg", "nope"] {
        let (status, body) = send(
            &router,
            Request::builder()
                .method("GET")
                .uri(&format!("/api/admin/table-config?table_name={table}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "table-config should answer, not 404");
        let notice: Value = serde_json::from_slice(&body).expect("notice should be JSON");
        assert!(
            notice["message"]
                .as_str()
                .is_some_and(|m| m.contains("no longer supported")),
            "expected a deprecation notice, got: {notice}"
        );
        // Whatever it says, it must not carry a definition any more.
        assert!(notice.get("definition_type").is_none(), "got: {notice}");
    }

    // Still the super-user's endpoint.
    let (status, _) = send(
        &router,
        Request::builder()
            .method("GET")
            .uri("/api/admin/table-config?table_name=cfg")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "admin auth is still required");

    // And the OpenAPI document says it is deprecated.
    let (_, spec) = send(
        &router,
        Request::builder()
            .method("GET")
            .uri("/openapi.json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let spec: Value = serde_json::from_slice(&spec).expect("openapi should be JSON");
    assert_eq!(
        spec.pointer("/paths/~1api~1admin~1table-config/get/deprecated"),
        Some(&Value::Bool(true))
    );
}

/// The function listing backs the SQL editor's autocomplete, which reads
/// `function_name` and a `params` array off each entry. A shape change silently
/// empties the completion popup, so the contract is pinned here.
///
/// Its deprecated sibling `/api/table-functions` is checked separately: it is
/// contractually empty, so there is no entry to read a shape off.
#[tokio::test(flavor = "multi_thread")]
async fn function_listing_carries_the_shape_autocomplete_reads() {
    let (router, _lake) = app().await;

    for uri in ["/api/functions"] {
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

/// The deprecated table-function listing answers with an empty array — routed, so
/// a client that still calls it gets a listing rather than a 404.
#[tokio::test(flavor = "multi_thread")]
async fn deprecated_table_function_listing_is_empty_not_missing() {
    let (router, _lake) = app().await;

    let (status, body) = send(
        &router,
        Request::builder()
            .method("GET")
            .uri("/api/table-functions")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let listing: Value = serde_json::from_slice(&body).expect("listing should be JSON");
    assert_eq!(
        listing.as_array().map(Vec::len),
        Some(0),
        "the deprecated listing is contractually empty, got: {listing}"
    );

    // And the OpenAPI document says so.
    let (_, spec) = send(
        &router,
        Request::builder()
            .method("GET")
            .uri("/openapi.json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let spec: Value = serde_json::from_slice(&spec).expect("openapi should be JSON");
    assert_eq!(
        spec.pointer("/paths/~1api~1table-functions/get/deprecated"),
        Some(&Value::Bool(true)),
        "the operation should be marked deprecated in the OpenAPI document"
    );
}
