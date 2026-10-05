//! HTTP integration tests for the MCP endpoint (`/mcp`), driven through the real
//! router with `tower::ServiceExt::oneshot`:
//! - the `Host` check: loopback always, plus `BEACON_MCP_ALLOWED_HOSTS`,
//! - the fixed tool list, and the table and column comments that `list_tables`
//!   and `describe_table` show to a caller who is not the super-user.

mod common;

use std::time::Duration;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use common::{basic, config};
use futures::StreamExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use beacon_server::axum::setup_router;

const PUBLIC_HOST: &str = "beacon.example.org";
const SESSION_HEADER: &str = "mcp-session-id";

async fn router_with(config: beacon_server_config::Config) -> (Router, common::TestServer, String) {
    let harness = common::server_with(config).await;
    let cfg = harness.server.config().clone();
    let admin = basic(&cfg.admin.username, &cfg.admin.password);
    let router = setup_router(harness.server.clone(), cfg).unwrap();
    (router, harness, admin)
}

/// POST one JSON-RPC message to `/mcp`.
fn mcp_request(
    host: &str,
    session: Option<&str>,
    auth: Option<&str>,
    message: Value,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header(header::HOST, host)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream");
    if let Some(session) = session {
        builder = builder.header(SESSION_HEADER, session);
    }
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    builder.body(Body::from(message.to_string())).unwrap()
}

/// Read SSE frames until the JSON-RPC response with `id` arrives. The stream can stay
/// open for keep-alive pings, so the read stops at the response, not at the end.
async fn read_response(body: Body, id: u64) -> Value {
    let mut stream = body.into_data_stream();
    let mut buffer = String::new();
    let read = async {
        while let Some(chunk) = stream.next().await {
            buffer.push_str(&String::from_utf8_lossy(&chunk.expect("body chunk")));
            for line in buffer.lines() {
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                if let Ok(message) = serde_json::from_str::<Value>(data.trim()) {
                    if message["id"] == id {
                        return message;
                    }
                }
            }
        }
        panic!("the stream ended before response {id}: {buffer}");
    };
    tokio::time::timeout(Duration::from_secs(20), read)
        .await
        .expect("MCP response in time")
}

/// Run the MCP handshake. Returns the HTTP status and, on success, the session id.
async fn initialize(
    router: &Router,
    host: &str,
    auth: Option<&str>,
) -> (StatusCode, Option<String>) {
    let message = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                   "clientInfo": {"name": "test", "version": "0"}}
    });
    let res = router
        .clone()
        .oneshot(mcp_request(host, None, auth, message))
        .await
        .unwrap();
    let status = res.status();
    if status != StatusCode::OK {
        return (status, None);
    }
    let session = res.headers()[SESSION_HEADER].to_str().unwrap().to_string();
    let reply = read_response(res.into_body(), 1).await;
    assert!(reply.get("result").is_some(), "initialize failed: {reply}");
    let initialized = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
    let res = router
        .clone()
        .oneshot(mcp_request(host, Some(&session), auth, initialized))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    (status, Some(session))
}

/// The tool names that `tools/list` returns to the caller behind `auth`.
async fn tool_names(router: &Router, auth: Option<&str>) -> Vec<String> {
    let (status, session) = initialize(router, "localhost", auth).await;
    assert_eq!(status, StatusCode::OK);
    let list = json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"});
    let res = router
        .clone()
        .oneshot(mcp_request("localhost", session.as_deref(), auth, list))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let reply = read_response(res.into_body(), 2).await;
    reply["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tools in: {reply}"))
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect()
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


/// Call the tool `name` and parse its text result as JSON. An error text that is
/// not JSON comes back as a JSON string.
async fn call_tool(router: &Router, auth: Option<&str>, name: &str, arguments: Value) -> Value {
    let (status, session) = initialize(router, "localhost", auth).await;
    assert_eq!(status, StatusCode::OK);
    let call = json!({
        "jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": {"name": name, "arguments": arguments}
    });
    let res = router
        .clone()
        .oneshot(mcp_request("localhost", session.as_deref(), auth, call))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let reply = read_response(res.into_body(), 3).await;
    let text = reply["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text content in: {reply}"));
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
}

/// Create the managed table `name` with a table comment and a column comment.
async fn commented_table(router: &Router, admin: &str, name: &str) {
    for sql in [
        format!("CREATE TABLE {name} (id BIGINT, depth DOUBLE)"),
        format!("COMMENT ON TABLE {name} IS 'about {name}'"),
        format!("COMMENT ON COLUMN {name}.depth IS 'depth in meters'"),
    ] {
        admin_sql(router, admin, &sql).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_refuses_an_unlisted_host() {
    let (router, _harness, _admin) = router_with(config(false)).await;

    assert_eq!(
        initialize(&router, PUBLIC_HOST, None).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        initialize(&router, "localhost", None).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_accepts_a_configured_host_and_keeps_loopback() {
    let mut config = config(false);
    config.mcp.allowed_hosts = vec![PUBLIC_HOST.to_string()];
    let (router, _harness, _admin) = router_with(config).await;

    assert_eq!(
        initialize(&router, PUBLIC_HOST, None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        initialize(&router, "localhost", None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        initialize(&router, "other.example.org", None).await.0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_accepts_every_host_with_a_wildcard() {
    let mut config = config(false);
    config.mcp.allowed_hosts = vec!["*".to_string()];
    let (router, _harness, _admin) = router_with(config).await;

    assert_eq!(
        initialize(&router, "other.example.org", None).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tools_list_is_the_four_generic_tools() {
    let (router, _harness, admin) = router_with(config(false)).await;
    commented_table(&router, &admin, "obs").await;

    let tools = tool_names(&router, None).await;

    assert_eq!(
        tools,
        ["list_tables", "describe_table", "run_sql", "export_query"],
        "a table adds no tool of its own"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_table_shows_the_comments() {
    let (router, _harness, admin) = router_with(config(false)).await;
    commented_table(&router, &admin, "obs").await;

    let described = call_tool(&router, None, "describe_table", json!({"table_name": "obs"})).await;

    assert_eq!(described["description"], "about obs", "{described}");
    let columns = described["columns"].as_array().expect("columns");
    let depth = columns
        .iter()
        .find(|column| column["name"] == "depth")
        .expect("depth column");
    let id = columns
        .iter()
        .find(|column| column["name"] == "id")
        .expect("id column");
    assert_eq!(depth["description"], "depth in meters", "{described}");
    assert_eq!(id["description"], Value::Null, "{described}");
}

#[tokio::test(flavor = "multi_thread")]
async fn granted_reader_lists_only_readable_tables_with_their_comments() {
    let (router, _harness, admin) = router_with(config(true)).await;
    commented_table(&router, &admin, "obs").await;
    commented_table(&router, &admin, "secret").await;
    for sql in [
        "CREATE USER alice WITH PASSWORD 'pw'",
        "CREATE ROLE reader",
        "GRANT SELECT ON TABLE obs TO ROLE reader",
        "GRANT ROLE reader TO USER alice",
    ] {
        admin_sql(&router, &admin, sql).await;
    }
    let alice = basic("alice", "pw");

    let listed = call_tool(&router, Some(&alice), "list_tables", json!({})).await;
    let secret = call_tool(&router, Some(&alice), "describe_table", json!({"table_name": "secret"}))
        .await;

    assert_eq!(
        listed,
        json!([{"name": "obs", "description": "about obs"}]),
        "an ungranted table must not show"
    );
    assert!(secret.get("columns").is_none(), "secret must not be described: {secret}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_table_extensions_route_is_gone() {
    let (router, _harness, _admin) = router_with(config(false)).await;

    let request = Request::builder()
        .uri("/api/table-extensions?table_name=obs")
        .body(Body::empty())
        .unwrap();
    let res = router.clone().oneshot(request).await.unwrap();

    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}
