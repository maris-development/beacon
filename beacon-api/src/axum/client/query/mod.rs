//! Query endpoints for the client HTTP API, organized by concern:
//!
//! - [`execute`] — one-shot query execution, body validation, and column listing.
//! - [`jobs`] — asynchronous query jobs (submit / status / stream / result / cancel).
//! - [`explain`] — plan explanation (`EXPLAIN` and `EXPLAIN ANALYZE`).
//! - [`metrics`] — recorded planner/runtime metrics for a previously run query.
//!
//! Request helpers shared across those submodules (auth resolution, the
//! `sql.enable` gate, path-UUID parsing, and file streaming) live in this module.

use ::axum::{
    body::Body,
    http::{header, HeaderMap, HeaderName, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::query::Query;
use beacon_core::runtime::Runtime;

pub(crate) mod execute;
pub(crate) mod explain;
pub(crate) mod jobs;
pub(crate) mod metrics;

/// Resolves the HTTP super-user flag from the request's `Authorization` header.
///
/// Only HTTP basic auth elevates a request: this transport has no bearer concept
/// (bearer tokens are Flight-SQL-only), so non-`Basic` schemes are left untouched
/// rather than rejected.
///
/// - No `Authorization` header, or a non-`Basic` scheme (e.g. `Bearer …`) →
///   anonymous, read-only (`Ok(false)`).
/// - Valid admin basic credentials → super-user, DDL/DML allowed (`Ok(true)`).
/// - A `Basic` header that fails validation → `Err(UNAUTHORIZED)` so bad
///   credentials surface as an error instead of silently degrading to read-only.
fn resolve_super_user(
    headers: &HeaderMap,
    admin: &beacon_config::AdminConfig,
) -> Result<bool, StatusCode> {
    let is_basic = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("Basic "));

    if is_basic {
        crate::axum::auth::verify_basic_auth_header(headers, admin)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Maps a `resolve_super_user` failure to the standard 401 JSON response.
fn invalid_credentials(status: StatusCode) -> (StatusCode, Json<String>) {
    (status, Json("invalid admin credentials".to_string()))
}

/// Rejects SQL queries when `sql.enable` is off (JSON queries are always allowed).
///
/// SQL over the HTTP client API is gated by `sql.enable`; the Flight SQL transport
/// has its own `flight_sql.enable`. Endpoints that execute a query (run, submit a
/// job, or `EXPLAIN ANALYZE`) call this so SQL cannot slip through while disabled.
fn ensure_sql_allowed(
    query: &Query,
    state: &Runtime,
) -> Result<(), (StatusCode, Json<String>)> {
    if matches!(query.inner, beacon_core::query::InnerQuery::Sql(_)) && !state.config().sql.enable {
        return Err((
            StatusCode::BAD_REQUEST,
            Json("SQL queries are not enabled".to_string()),
        ));
    }
    Ok(())
}

/// Parses a path UUID, mapping a malformed value to `400`.
fn parse_query_id(raw: &str) -> Result<uuid::Uuid, (StatusCode, Json<String>)> {
    uuid::Uuid::parse_str(raw)
        .map_err(|_| (StatusCode::BAD_REQUEST, Json("Invalid UUID format".to_string())))
}

/// Streams a temporary result file to the client with the appropriate content headers.
async fn file_stream_response(
    file_path: &std::path::Path,
    content_type: &str,
    file_ext: &str,
    query_id: uuid::Uuid,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let file = tokio::fs::File::open(file_path).await.map_err(|err| {
        tracing::error!("failed to open query result file {file_path:?}: {err}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json("failed to open query result".to_string()),
        )
    })?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let inner_stream = Body::from_stream(stream);
    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"output.{}\"", file_ext).as_str(),
            ),
            (
                HeaderName::from_static("x-beacon-query-id"),
                query_id.to_string().as_str(),
            ),
        ],
        inner_stream,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use ::axum::http::{HeaderMap, StatusCode};
    use base64::{engine::general_purpose, Engine as _};

    use super::resolve_super_user;

    /// Builds an `Authorization: Basic ...` header map for the given credentials.
    fn basic_auth_headers(username: &str, password: &str) -> HeaderMap {
        let value = format!(
            "Basic {}",
            general_purpose::STANDARD.encode(format!("{username}:{password}"))
        );
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", value.parse().unwrap());
        headers
    }

    fn test_admin() -> beacon_config::AdminConfig {
        beacon_config::AdminConfig {
            username: "beacon-admin".to_string(),
            password: "beacon-password".to_string(),
        }
    }

    /// Valid admin credentials elevate the HTTP request to super-user so DDL/DML
    /// is allowed over HTTP.
    #[test]
    fn valid_admin_credentials_resolve_to_super_user() {
        let admin = test_admin();
        let headers = basic_auth_headers(&admin.username, &admin.password);
        assert_eq!(resolve_super_user(&headers, &admin), Ok(true));
    }

    /// No credentials at all stays anonymous (read-only), not an error.
    #[test]
    fn missing_authorization_header_is_anonymous() {
        let headers = HeaderMap::new();
        assert_eq!(resolve_super_user(&headers, &test_admin()), Ok(false));
    }

    /// Credentials that are present but wrong are rejected rather than silently
    /// degrading to read-only.
    #[test]
    fn wrong_credentials_are_rejected() {
        let headers = basic_auth_headers("not-the-admin", "wrong-password");
        assert_eq!(
            resolve_super_user(&headers, &test_admin()),
            Err(StatusCode::UNAUTHORIZED)
        );
    }

    /// A non-Basic scheme (e.g. a bearer token, which the endpoint's OpenAPI
    /// advertises) must not be treated as failed basic auth: it falls through to
    /// anonymous/read-only rather than being rejected with 401.
    #[test]
    fn bearer_token_is_anonymous_not_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer some-token".parse().unwrap());
        assert_eq!(resolve_super_user(&headers, &test_admin()), Ok(false));
    }
}
