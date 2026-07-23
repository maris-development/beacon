//! HTTP integration tests for the client (non-admin) API surface, driven through
//! the real router with `tower::ServiceExt::oneshot`:
//! - `/api/info` (system info, anonymous),
//! - `/api/query` (SQL + JSON, stream and file output, the `sql.enable` gate),
//! - `/api/parse-query`, `/api/query/available-columns`,
//! - the table-discovery endpoints (`/api/tables`, `/api/tables-with-schema`,
//!   `/api/table-schema`, `/api/table-extensions`, `/api/default-table[-schema]`),
//! - the dataset-discovery endpoints (`/api/datasets`, `/api/list-datasets`,
//!   `/api/dataset-schema`, `/api/total-datasets`).

mod common;

use std::sync::Arc;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use beacon_core::runtime::Runtime;
use common::{basic, config};
use futures::TryStreamExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use beacon_datalake::axum::setup_router;

/// Opens an ephemeral lake from `config` and returns the router plus the lake
/// (held so its temp root outlives the router) and the resolved `Arc<Config>`.
async fn app(
    config: beacon_datalake_config::Config,
) -> (Router, common::TestLake, Arc<beacon_datalake_config::Config>) {
    let lake = common::lake_with(config).await;
    let cfg = lake.lake.config().clone();
    let router = setup_router(lake.lake.clone(), cfg.clone()).unwrap();
    (router, lake, cfg)
}

/// Runs setup SQL directly against the runtime as the system identity.
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

fn post_json(uri: &str, body: Value, auth: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

struct Res {
    status: StatusCode,
    headers: ::axum::http::HeaderMap,
    body: Vec<u8>,
}

async fn send(router: &Router, req: Request<Body>) -> Res {
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap().to_vec();
    Res { status, headers, body }
}

fn json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("response should be JSON")
}

// --------------------------------------------------------------------------
// /api/info
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn info_is_public_and_reports_a_version() {
    let (router, _lake, _cfg) = app(config(false)).await;

    // `/api/info` is unauthenticated.
    let res = send(&router, get("/api/info", None)).await;
    assert_eq!(res.status, StatusCode::OK);
    let info = json(&res.body);
    assert!(
        info.get("beacon_version").and_then(Value::as_str).is_some(),
        "info should carry a beacon_version string, got: {info}"
    );
}

// --------------------------------------------------------------------------
// /api/query — SQL + JSON, stream + file, the sql.enable gate
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn query_sql_select_streams_arrow_with_a_query_id() {
    let (router, _lake, _cfg) = app(config(false)).await;

    let res = send(
        &router,
        post_json("/api/query", json!({ "sql": "SELECT 1 AS v" }), None),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(
        res.headers.contains_key("x-beacon-query-id"),
        "a successful query should return its id in the x-beacon-query-id header"
    );
    assert!(!res.body.is_empty(), "the Arrow IPC stream should be non-empty");
}

#[tokio::test(flavor = "multi_thread")]
async fn query_json_body_runs() {
    let (router, lake, _cfg) = app(config(false)).await;
    seed(lake.lake.runtime(), "CREATE TABLE jq (a BIGINT)").await;
    seed(lake.lake.runtime(), "INSERT INTO jq VALUES (1), (2), (3)").await;

    let res = send(
        &router,
        post_json(
            "/api/query",
            json!({ "select": ["a"], "from": "jq", "filter": { "column": "a", "gt_eq": 2 } }),
            None,
        ),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "a JSON query body should run");
    assert!(res.headers.contains_key("x-beacon-query-id"));
}

#[tokio::test(flavor = "multi_thread")]
async fn query_with_csv_output_is_a_file_download() {
    let (router, lake, _cfg) = app(config(false)).await;
    seed(lake.lake.runtime(), "CREATE TABLE t (a BIGINT)").await;
    seed(lake.lake.runtime(), "INSERT INTO t VALUES (10), (20)").await;

    let res = send(
        &router,
        post_json(
            "/api/query",
            json!({ "sql": "SELECT a FROM t ORDER BY a", "output": { "format": "csv" } }),
            None,
        ),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    let disposition = res
        .headers
        .get(header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        disposition.contains("attachment"),
        "a file output format should be returned as an attachment, got: {disposition:?}"
    );
    let text = String::from_utf8(res.body).expect("CSV output is UTF-8");
    assert!(
        text.contains("10") && text.contains("20"),
        "the CSV download should contain the exported rows, got: {text:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn query_rejects_malformed_body() {
    let (router, _lake, _cfg) = app(config(false)).await;

    // Neither a `sql` key nor a valid JSON-query shape.
    let res = send(
        &router,
        post_json("/api/query", json!({ "not_a_query": true }), None),
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_over_http_is_gated_by_sql_enable() {
    let mut cfg = config(false);
    cfg.sql.enable = false; // JSON stays allowed; SQL is turned off.
    let (router, _lake, _cfg) = app(cfg).await;

    // SQL → 400.
    let sql = send(
        &router,
        post_json("/api/query", json!({ "sql": "SELECT 1" }), None),
    )
    .await;
    assert_eq!(sql.status, StatusCode::BAD_REQUEST, "SQL should be disabled");

    // JSON is always allowed, even with SQL off.
    let json_query = send(
        &router,
        post_json("/api/query", json!({ "select": [{ "value": 1 }] }), None),
    )
    .await;
    assert_eq!(json_query.status, StatusCode::OK, "JSON queries stay enabled");
}

#[tokio::test(flavor = "multi_thread")]
async fn parse_query_validates_without_executing() {
    let (router, _lake, _cfg) = app(config(false)).await;

    // A well-formed body over a table that does not exist: parse-query does not
    // execute, so it is still 200.
    let ok = send(
        &router,
        post_json("/api/parse-query", json!({ "sql": "SELECT * FROM missing" }), None),
    )
    .await;
    assert_eq!(ok.status, StatusCode::OK);

    // A body that is not even a query object (a bare JSON string) fails to
    // deserialize into the request type → a client error.
    let bad = send(
        &router,
        post_json("/api/parse-query", json!("not a query object"), None),
    )
    .await;
    assert!(
        bad.status.is_client_error(),
        "a non-object body should be rejected, got: {}",
        bad.status
    );
}

// --------------------------------------------------------------------------
// Table discovery
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn table_discovery_endpoints_reflect_a_created_table() {
    let (router, lake, _cfg) = app(config(false)).await;
    seed(lake.lake.runtime(), "CREATE TABLE obs (id BIGINT, name VARCHAR)").await;

    // /api/tables lists it.
    let tables = json(&send(&router, get("/api/tables", None)).await.body);
    assert!(
        tables.as_array().unwrap().iter().any(|t| t == "obs"),
        "the created table should be listed, got: {tables}"
    );

    // /api/table-schema returns its two fields.
    let schema = send(&router, get("/api/table-schema?table_name=obs", None)).await;
    assert_eq!(schema.status, StatusCode::OK);
    let fields = json(&schema.body);
    let names: Vec<&str> = fields["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["id", "name"]);

    // /api/tables-with-schema carries the columns.
    let with_schema = json(&send(&router, get("/api/tables-with-schema", None)).await.body);
    let obs = with_schema
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["table_name"] == "obs")
        .expect("obs present in tables-with-schema");
    assert_eq!(obs["columns"].as_array().unwrap().len(), 2);

    // Unknown table → 404.
    let missing = send(&router, get("/api/table-schema?table_name=nope", None)).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    // (`/api/table-extensions` reads via `SHOW EXTENSIONS`, which is super-user
    // gated, so it is exercised with admin auth in `admin_endpoints_http`.)
}

#[tokio::test(flavor = "multi_thread")]
async fn default_table_endpoints_answer() {
    let (router, _lake, _cfg) = app(config(false)).await;

    // The default table name is a string (its schema may be empty when the table
    // does not exist — the endpoint tolerates that rather than 500ing).
    let name = send(&router, get("/api/default-table", None)).await;
    assert_eq!(name.status, StatusCode::OK);
    assert!(json(&name.body).is_string());

    let schema = send(&router, get("/api/default-table-schema", None)).await;
    assert_eq!(schema.status, StatusCode::OK);
    assert!(json(&schema.body).get("fields").is_some());
}

// --------------------------------------------------------------------------
// Dataset discovery
// --------------------------------------------------------------------------

/// Writes bytes directly into the lake's datasets store so the listing endpoints
/// have something to discover.
fn place_dataset(cfg: &beacon_datalake_config::Config, rel: &str, contents: &str) {
    let path = cfg.data.datasets.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

/// Copies the parquet fixture into the datasets store at `rel`.
fn place_parquet(cfg: &beacon_datalake_config::Config, rel: &str) {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet");
    let dst = cfg.data.datasets.join(rel);
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
    std::fs::copy(src, dst).expect("copy parquet fixture");
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_discovery_endpoints_reflect_stored_files() {
    let (router, _lake, cfg) = app(config(false)).await;
    place_parquet(&cfg, "obs/a.parquet");
    place_parquet(&cfg, "obs/b.parquet");

    // /api/total-datasets counts them.
    let total = send(&router, get("/api/total-datasets", None)).await;
    assert_eq!(total.status, StatusCode::OK);
    assert_eq!(json(&total.body).as_u64().unwrap(), 2, "two datasets stored");

    // /api/list-datasets returns metadata entries (path + detected format).
    let listed = json(&send(&router, get("/api/list-datasets", None)).await.body);
    let paths: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["file_path"].as_str().unwrap())
        .collect();
    assert!(paths.contains(&"obs/a.parquet") && paths.contains(&"obs/b.parquet"));
    assert!(
        listed.as_array().unwrap().iter().all(|d| d["format"] == "parquet"),
        "the detected format should be parquet, got: {listed}"
    );

    // The deprecated /api/datasets returns bare path strings.
    let bare = json(&send(&router, get("/api/datasets", None)).await.body);
    assert_eq!(bare.as_array().unwrap().len(), 2);

    // A pattern narrows the listing.
    let filtered = json(
        &send(&router, get("/api/list-datasets?pattern=obs/a.parquet", None))
            .await
            .body,
    );
    assert_eq!(filtered.as_array().unwrap().len(), 1);

    // /api/dataset-schema returns the file's real (non-empty) Arrow schema.
    let schema = send(&router, get("/api/dataset-schema?file=obs/a.parquet", None)).await;
    assert_eq!(schema.status, StatusCode::OK);
    let schema_json = json(&schema.body);
    assert!(
        !schema_json["fields"].as_array().unwrap().is_empty(),
        "the parquet schema should have fields"
    );
}
