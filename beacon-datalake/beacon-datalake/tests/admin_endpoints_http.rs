//! HTTP integration tests for the admin endpoints that manage runtime objects
//! (as opposed to dataset files, which `admin_datasets_http` covers), driven
//! through the real router with `tower::ServiceExt::oneshot`:
//! - crawlers (`create` / `list` / `get` / `run` / `drop`),
//! - external tables (`/api/admin/external-tables`),
//! - table extensions (`PUT`/`DELETE /api/admin/table-extensions/{table}`),
//! - the chunked-upload abort path.
//!
//! Every one of these is super-user-only; the gate is spot-checked here and
//! exhaustively in `auth_http`/`rbac_http`.

mod common;

use std::sync::Arc;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use common::{basic, config};
use serde_json::{json, Value};
use tower::ServiceExt;

use beacon_datalake::axum::setup_router;

async fn app(
    config: beacon_datalake_config::Config,
) -> (Router, common::TestLake, Arc<beacon_datalake_config::Config>) {
    let lake = common::lake_with(config).await;
    let cfg = lake.lake.config().clone();
    let router = setup_router(lake.lake.clone(), cfg.clone()).unwrap();
    (router, lake, cfg)
}

fn req(method: &str, uri: &str, auth: Option<&str>, body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    builder.body(body).unwrap()
}

fn json_req(method: &str, uri: &str, body: Value, auth: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

struct Res {
    status: StatusCode,
    body: Vec<u8>,
}

async fn send(router: &Router, request: Request<Body>) -> Res {
    let res = router.clone().oneshot(request).await.unwrap();
    let status = res.status();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap().to_vec();
    Res { status, body }
}

fn json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("response should be JSON")
}

fn place_dataset(cfg: &beacon_datalake_config::Config, rel: &str, contents: &str) {
    let path = cfg.data.datasets.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

fn admin(cfg: &beacon_datalake_config::Config) -> String {
    basic(&cfg.admin.username, &cfg.admin.password)
}

/// Runs a SELECT count over `table` through the public query endpoint and returns
/// the row count reported by decoding the CSV output (avoids parsing Arrow IPC).
async fn count_rows(router: &Router, admin_auth: &str, table: &str) -> i64 {
    let res = send(
        router,
        json_req(
            "POST",
            "/api/query",
            json!({ "sql": format!("SELECT count(*) AS c FROM {table}"), "output": { "format": "csv" } }),
            Some(admin_auth),
        ),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "count query should succeed");
    let text = String::from_utf8(res.body).expect("csv");
    // `c\n<n>\n`
    text.lines()
        .nth(1)
        .and_then(|l| l.trim().parse().ok())
        .unwrap_or_else(|| panic!("could not parse count from CSV: {text:?}"))
}

// --------------------------------------------------------------------------
// Crawlers
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn crawler_lifecycle_create_list_get_run_drop() {
    let (router, _lake, cfg) = app(config(false)).await;
    let admin = admin(&cfg);
    place_dataset(&cfg, "obs/a.csv", "v,name\n1,a\n2,b\n");
    place_dataset(&cfg, "obs/b.csv", "v,name\n3,c\n");

    // Create.
    let created = send(
        &router,
        json_req(
            "POST",
            "/api/admin/crawlers",
            json!({ "name": "obsc", "target_prefix": "obs/" }),
            Some(&admin),
        ),
    )
    .await;
    assert_eq!(created.status, StatusCode::OK, "crawler should be created");

    // List reflects it, with the bare (unquoted) name.
    let list = json(&send(&router, req("GET", "/api/admin/crawlers", Some(&admin), Body::empty())).await.body);
    let names: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["obsc"], "the crawler should be listed by its bare name");

    // Get by name.
    let one = send(&router, req("GET", "/api/admin/crawlers/obsc", Some(&admin), Body::empty())).await;
    assert_eq!(one.status, StatusCode::OK);
    assert_eq!(json(&one.body)["target_prefix"], "obs/");

    // Get unknown → 404.
    let missing = send(&router, req("GET", "/api/admin/crawlers/nope", Some(&admin), Body::empty())).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);

    // Run → report.
    let run = send(&router, req("POST", "/api/admin/crawlers/obsc/run", Some(&admin), Body::empty())).await;
    assert_eq!(run.status, StatusCode::OK);
    let report = json(&run.body);
    assert_eq!(report["crawler"], "obsc");
    assert!(
        report["discovered"].as_u64().unwrap() >= 1,
        "the run should discover at least one table, got: {report}"
    );
    let created_tables = report["created"].as_array().unwrap();
    assert!(!created_tables.is_empty(), "the crawl should create a table");
    let table = created_tables[0].as_str().unwrap().to_string();

    // The discovered table is queryable and has all three rows.
    assert_eq!(count_rows(&router, &admin, &table).await, 3);

    // Drop, then it is gone from the listing and running it fails.
    let dropped = send(&router, req("DELETE", "/api/admin/crawlers/obsc", Some(&admin), Body::empty())).await;
    assert_eq!(dropped.status, StatusCode::OK);
    let list = json(&send(&router, req("GET", "/api/admin/crawlers", Some(&admin), Body::empty())).await.body);
    assert!(list.as_array().unwrap().is_empty(), "the crawler should be gone");
    let run_missing = send(&router, req("POST", "/api/admin/crawlers/obsc/run", Some(&admin), Body::empty())).await;
    assert_eq!(run_missing.status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn crawler_endpoints_are_super_user_only() {
    let (router, _lake, _cfg) = app(config(false)).await;

    // No credentials → 401 on both a read and a write.
    assert_eq!(
        send(&router, req("GET", "/api/admin/crawlers", None, Body::empty())).await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(
            &router,
            json_req(
                "POST",
                "/api/admin/crawlers",
                json!({ "name": "x", "target_prefix": "obs/" }),
                None,
            ),
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
}

// --------------------------------------------------------------------------
// External tables
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn create_external_table_from_fields() {
    let (router, _lake, cfg) = app(config(false)).await;
    let admin = admin(&cfg);
    place_dataset(&cfg, "ext/a.csv", "v\n1\n2\n");
    place_dataset(&cfg, "ext/b.csv", "v\n3\n");

    let created = send(
        &router,
        json_req(
            "POST",
            "/api/admin/external-tables",
            json!({ "name": "ext_obs", "location": "ext/", "file_type": "CSV" }),
            Some(&admin),
        ),
    )
    .await;
    assert_eq!(created.status, StatusCode::OK, "external table should be created");

    // It appears in the catalog and is queryable.
    let tables = json(&send(&router, req("GET", "/api/tables", Some(&admin), Body::empty())).await.body);
    assert!(
        tables.as_array().unwrap().iter().any(|t| t == "ext_obs"),
        "the external table should be listed, got: {tables}"
    );
    assert_eq!(count_rows(&router, &admin, "ext_obs").await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_external_table_rejects_an_unknown_format() {
    let (router, _lake, cfg) = app(config(false)).await;
    let admin = admin(&cfg);

    // An unknown `STORED AS` format fails registration → 400. (A merely-empty
    // location is accepted lazily, so it is the format, not the path, that is
    // rejected up front.)
    let res = send(
        &router,
        json_req(
            "POST",
            "/api/admin/external-tables",
            json!({ "name": "broken", "location": "ext/", "file_type": "BOGUSFORMAT" }),
            Some(&admin),
        ),
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

// --------------------------------------------------------------------------
// Table extensions
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn table_extensions_set_get_and_clear() {
    let (router, lake, cfg) = app(config(false)).await;
    let admin = admin(&cfg);
    // A managed table to attach the extensions to.
    lake.lake
        .runtime()
        .run_query(
            beacon_core::query::Query::sql("CREATE TABLE obs (id BIGINT, name VARCHAR)".to_string()),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .expect("create table");

    // Initially, no extensions: an empty document.
    let empty = json(&send(&router, req("GET", "/api/table-extensions?table_name=obs", Some(&admin), Body::empty())).await.body);
    assert!(empty.get("mcp").is_none(), "no mcp extension initially, got: {empty}");

    // Set an MCP descriptor exposing an existing column.
    let set = send(
        &router,
        json_req(
            "PUT",
            "/api/admin/table-extensions/obs",
            json!({ "mcp": { "enabled": true, "tool_name": "obs_tool", "description": "obs", "exposed_columns": ["id"] } }),
            Some(&admin),
        ),
    )
    .await;
    assert_eq!(set.status, StatusCode::OK, "setting extensions should succeed");

    // GET reflects it.
    let got = json(&send(&router, req("GET", "/api/table-extensions?table_name=obs", Some(&admin), Body::empty())).await.body);
    assert_eq!(got["mcp"]["tool_name"], "obs_tool");
    assert_eq!(got["mcp"]["enabled"], true);

    // DELETE clears everything.
    let cleared = send(&router, req("DELETE", "/api/admin/table-extensions/obs", Some(&admin), Body::empty())).await;
    assert_eq!(cleared.status, StatusCode::OK);
    let after = json(&send(&router, req("GET", "/api/table-extensions?table_name=obs", Some(&admin), Body::empty())).await.body);
    assert!(after.get("mcp").is_none(), "extensions should be cleared, got: {after}");
}

// --------------------------------------------------------------------------
// Chunked-upload abort
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn aborting_a_chunked_upload_invalidates_the_session() {
    let (router, _lake, cfg) = app(config(false)).await;
    let admin = admin(&cfg);

    // Initiate.
    let initiated = send(
        &router,
        req(
            "POST",
            "/api/admin/datasets/upload/initiate?path=big/multi.parquet",
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(initiated.status, StatusCode::OK);
    let upload_id = json(&initiated.body)["upload_id"].as_str().unwrap().to_string();

    // Abort → 204.
    let aborted = send(
        &router,
        req(
            "DELETE",
            &format!("/api/admin/datasets/upload?upload_id={upload_id}"),
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(aborted.status, StatusCode::NO_CONTENT);

    // A part against the aborted session is rejected as an unknown session.
    let part = send(
        &router,
        req(
            "PUT",
            &format!("/api/admin/datasets/upload/part?upload_id={upload_id}&part_number=1"),
            Some(&admin),
            Body::from("some bytes"),
        ),
    )
    .await;
    assert_eq!(
        part.status,
        StatusCode::NOT_FOUND,
        "a part for an aborted upload should be rejected"
    );
}
