//! HTTP tests for the Croissant endpoint (`/api/croissant`, `/api/croissant/{table}` and
//! `/api/croissant/{table}/data.parquet`), driven through the real router with
//! `tower::ServiceExt::oneshot`.

mod common;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, HeaderMap, Request, StatusCode},
    Router,
};
use common::{basic, config};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use tower::ServiceExt;

use beacon_server::axum::setup_router;

async fn router_with(config: beacon_server_config::Config) -> (Router, common::TestServer, String) {
    let harness = common::server_with(config).await;
    let cfg = harness.server.config().clone();
    let admin = basic(&cfg.admin.username, &cfg.admin.password);
    let router = setup_router(harness.server.clone(), cfg).unwrap();
    (router, harness, admin)
}

/// Runs `sql` as the admin on the server at `prefix` (the base path).
async fn admin_sql_at(router: &Router, admin: &str, prefix: &str, sql: &str) {
    let request = Request::builder()
        .method("POST")
        .uri(format!("{prefix}/api/query"))
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

async fn admin_sql(router: &Router, admin: &str, sql: &str) {
    admin_sql_at(router, admin, "", sql).await;
}

/// GET `uri` with `Host: localhost`. Returns the status, the headers and the body.
async fn get(router: &Router, uri: &str, auth: Option<&str>) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut builder = Request::builder()
        .method("GET")
        .uri(uri)
        .header(header::HOST, "localhost");
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

/// The SQL of the table `obs`: time, numeric, boolean and text columns, comments and one
/// null row.
const OBS_SQL: [&str; 4] = [
    "CREATE TABLE obs (id BIGINT, t TIMESTAMP, depth DOUBLE, temp REAL, ok BOOLEAN, platform VARCHAR)",
    "INSERT INTO obs VALUES \
     (1, TIMESTAMP '2024-01-01 00:00:00', 5.0, 10.5, true, 'SHIP'), \
     (2, TIMESTAMP '2024-01-02 00:00:00', 50.0, 4.25, false, 'BUOY'), \
     (3, NULL, NULL, NULL, NULL, NULL)",
    "COMMENT ON TABLE obs IS 'test observations'",
    "COMMENT ON COLUMN obs.depth IS 'depth in meters'",
];

async fn obs_table(router: &Router, admin: &str) {
    for sql in OBS_SQL {
        admin_sql(router, admin, sql).await;
    }
}

/// The table `obs2`, with one row.
async fn obs2_table(router: &Router, admin: &str) {
    for sql in [
        "CREATE TABLE obs2 (id BIGINT)",
        "INSERT INTO obs2 VALUES (1)",
    ] {
        admin_sql(router, admin, sql).await;
    }
}

fn json_body(body: &[u8]) -> Value {
    serde_json::from_slice(body).expect("JSON body")
}

/// The `name` of each entry of a catalog document.
fn catalog_names(catalog: &Value) -> Vec<String> {
    catalog["dataset"]
        .as_array()
        .expect("dataset array")
        .iter()
        .map(|entry| entry["name"].as_str().unwrap().to_string())
        .collect()
}

/// The field named `name` in the first record set of a dataset document.
fn field<'a>(document: &'a Value, name: &str) -> &'a Value {
    document["recordSet"][0]["field"]
        .as_array()
        .expect("field array")
        .iter()
        .find(|field| field["name"] == name)
        .unwrap_or_else(|| panic!("no field {name}"))
}

/// The row count and the column names of a Parquet file.
fn parquet_shape(body: Vec<u8>) -> (usize, Vec<String>) {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(body)).expect("Parquet file");
    let names = builder
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let rows = builder
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum();
    (rows, names)
}

/// The grant setup of the anonymous tests: the anonymous user reads `obs`, `alice` reads
/// `obs2`.
async fn anonymous_reads_obs(router: &Router, admin: &str) {
    obs_table(router, admin).await;
    obs2_table(router, admin).await;
    for sql in [
        "CREATE ROLE public_reader",
        "GRANT SELECT ON TABLE obs TO ROLE public_reader",
        "GRANT ROLE public_reader TO USER anonymous",
        "CREATE ROLE obs2_reader",
        "GRANT SELECT ON TABLE obs2 TO ROLE obs2_reader",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "GRANT ROLE obs2_reader TO USER alice",
    ] {
        admin_sql(router, admin, sql).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_document_describes_the_table() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/api/croissant/obs", None).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(headers[header::CONTENT_TYPE], "application/ld+json");
    let document = json_body(&body);
    assert_eq!(document["name"], "obs");
    assert_eq!(document["description"], "test observations");
    let depth = field(&document, "depth");
    assert_eq!(depth["dataType"], "sc:Float");
    assert_eq!(depth["description"], "depth in meters");
    assert_eq!(field(&document, "t")["dataType"], "sc:DateTime");
    assert_eq!(field(&document, "ok")["dataType"], "sc:Boolean");
    assert_eq!(field(&document, "id")["dataType"], "sc:Integer");
    assert_eq!(field(&document, "platform")["dataType"], "sc:Text");
    assert_eq!(
        document["distribution"][0]["contentUrl"],
        "http://localhost/api/croissant/obs/data.parquet"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn data_parquet_holds_the_rows() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/api/croissant/obs/data.parquet", None).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        headers[header::CONTENT_DISPOSITION],
        "attachment; filename=\"obs.parquet\""
    );
    let (rows, names) = parquet_shape(body);
    assert_eq!(rows, 3);
    assert_eq!(names, ["id", "t", "depth", "temp", "ok", "platform"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_lists_readable_tables() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs2_table(&router, &admin).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/api/croissant", None).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(headers[header::CONTENT_TYPE], "application/ld+json");
    let catalog = json_body(&body);
    // The server also has its own `default` table.
    let names = catalog_names(&catalog);
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);
    let entry = |name: &str| {
        catalog["dataset"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == name)
            .unwrap_or_else(|| panic!("no entry {name} in {names:?}"))
            .clone()
    };
    assert_eq!(entry("obs")["url"], "http://localhost/api/croissant/obs");
    assert_eq!(entry("obs")["description"], "test observations");
    assert_eq!(entry("obs2")["url"], "http://localhost/api/croissant/obs2");
    assert!(entry("obs2").get("description").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_table_is_404() {
    let (router, _harness, _admin) = router_with(config(false)).await;

    for uri in ["/api/croissant/nope", "/api/croissant/nope/data.parquet"] {
        let (status, _, body) = get(&router, uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert!(json_body(&body)["error"].is_string(), "{uri}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unreadable_table_is_hidden() {
    let (router, _harness, admin) = router_with(config(true)).await;
    obs_table(&router, &admin).await;
    admin_sql(&router, &admin, "CREATE USER bob WITH PASSWORD 'pw'").await;
    let bob = basic("bob", "pw");

    for uri in ["/api/croissant/obs", "/api/croissant/obs/data.parquet"] {
        assert_eq!(
            get(&router, uri, Some(&bob)).await.0,
            StatusCode::NOT_FOUND,
            "{uri}"
        );
    }
    let (status, _, body) = get(&router, "/api/croissant", Some(&bob)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!catalog_names(&json_body(&body)).contains(&"obs".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn no_credentials_reads_as_anonymous() {
    let (router, _harness, admin) = router_with(config(true)).await;
    anonymous_reads_obs(&router, &admin).await;

    let (status, _, body) = get(&router, "/api/croissant", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(catalog_names(&json_body(&body)), ["obs"]);
    assert_eq!(
        get(&router, "/api/croissant/obs", None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        get(&router, "/api/croissant/obs2", None).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_read_as_that_user() {
    let (router, _harness, admin) = router_with(config(true)).await;
    anonymous_reads_obs(&router, &admin).await;
    let alice = basic("alice", "pw");

    let (status, _, body) = get(&router, "/api/croissant", Some(&alice)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(catalog_names(&json_body(&body)).contains(&"obs2".to_string()));
    assert_eq!(
        get(&router, "/api/croissant/obs2", Some(&alice)).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_credentials_give_401() {
    let (router, _harness, admin) = router_with(config(true)).await;
    anonymous_reads_obs(&router, &admin).await;

    let wrong = basic("alice", "wrong");
    assert_eq!(
        get(&router, "/api/croissant", Some(&wrong)).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_without_served_columns() {
    let (router, _harness, admin) = router_with(config(false)).await;
    admin_sql(&router, &admin, "CREATE TABLE blobs (b BYTEA)").await;

    let (status, _, body) = get(&router, "/api/croissant/blobs", None).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(json_body(&body)["recordSet"][0]["field"], json!([]));

    let (status, _, body) = get(&router, "/api/croissant/blobs/data.parquet", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(json_body(&body)["error"].is_string());
}

#[tokio::test(flavor = "multi_thread")]
async fn odd_names_round_trip() {
    let (router, _harness, admin) = router_with(config(false)).await;
    for sql in [
        "CREATE TABLE \"odd name\" (\"a#b\" BIGINT)",
        "INSERT INTO \"odd name\" VALUES (7)",
    ] {
        admin_sql(&router, &admin, sql).await;
    }

    let (status, _, body) = get(&router, "/api/croissant/odd%20name", None).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(json_body(&body)["name"], "odd name");

    let (status, _, body) = get(&router, "/api/croissant/odd%20name/data.parquet", None).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let (rows, names) = parquet_shape(body);
    assert_eq!(rows, 1);
    assert_eq!(names, ["a#b"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn urls_follow_the_base_path() {
    let mut config = config(false);
    config.server.base_path = "/beacon".to_string();
    let (router, _harness, admin) = router_with(config).await;
    for sql in OBS_SQL {
        admin_sql_at(&router, &admin, "/beacon", sql).await;
    }

    let (status, _, body) = get(&router, "/beacon/api/croissant/obs", None).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        json_body(&body)["url"],
        "http://localhost/beacon/api/croissant/obs"
    );
}
