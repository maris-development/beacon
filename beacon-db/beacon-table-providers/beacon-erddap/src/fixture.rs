//! A local stand-in for an ERDDAP server, for tests in this crate and in beacon-core.
//!
//! Routes match on the path and, optionally, on a substring of the decoded query.
//! Every request is recorded, so a test can assert what Beacon pushed down.

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;

/// One canned response of the fixture server.
#[derive(Clone, Debug)]
pub struct Route {
    /// The request path to match.
    pub path: String,
    /// A substring that the decoded query must hold. `None` matches any query.
    pub query_contains: Option<String>,
    /// The HTTP status of the response.
    pub status: u16,
    /// The body of the response.
    pub body: Bytes,
}

impl Route {
    /// Serve a recorded file from `test-files` with status 200.
    pub fn file(path: &str, name: &str) -> Self {
        Self {
            path: path.to_string(),
            query_contains: None,
            status: 200,
            body: test_file(name),
        }
    }

    /// Serve `body` with the given status.
    pub fn status(path: &str, status: u16, body: impl Into<Bytes>) -> Self {
        Self {
            path: path.to_string(),
            query_contains: None,
            status,
            body: body.into(),
        }
    }

    /// Match only when the decoded query holds `needle`.
    pub fn with_query(mut self, needle: &str) -> Self {
        self.query_contains = Some(needle.to_string());
        self
    }
}

/// A recorded fixture file.
pub fn test_file(name: &str) -> Bytes {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test-files")
        .join(name);
    Bytes::from(std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())))
}

#[derive(Clone)]
struct ServerState {
    routes: Arc<Vec<Route>>,
    requests: Arc<Mutex<Vec<String>>>,
}

/// A running fixture server. It stops when dropped.
pub struct FixtureServer {
    base: String,
    requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl FixtureServer {
    /// Start a server on a free local port. The first matching route answers a request.
    pub async fn start(routes: Vec<Route>) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = ServerState {
            routes: Arc::new(routes),
            requests: requests.clone(),
        };
        let app = Router::new().fallback(handle).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture server");
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("fixture server");
        });
        Self {
            base,
            requests,
            task,
        }
    }

    /// The ERDDAP base URL: `http://127.0.0.1:<port>/erddap`.
    pub fn erddap_url(&self) -> String {
        format!("{}/erddap", self.base)
    }

    /// Every request so far: the path, plus `?` and the decoded query when one exists.
    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle(State(state): State<ServerState>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(|q| {
        percent_encoding::percent_decode_str(q)
            .decode_utf8_lossy()
            .into_owned()
    });
    state.requests.lock().unwrap().push(match &query {
        Some(q) => format!("{path}?{q}"),
        None => path.clone(),
    });
    let matched = state.routes.iter().find(|r| {
        r.path == path
            && r.query_contains
                .as_ref()
                .is_none_or(|needle| query.as_deref().unwrap_or("").contains(needle.as_str()))
    });
    match matched {
        Some(r) => (StatusCode::from_u16(r.status).unwrap(), r.body.clone()).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "Error {\n    code=404;\n    message=\"Not Found: no fixture route\";\n}\n",
        )
            .into_response(),
    }
}
