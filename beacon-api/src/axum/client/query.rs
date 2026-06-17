//! Query execution endpoints for the client HTTP API.

use ::axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::runtime::Runtime;
use beacon_core::{
    api::{QueryMetricsView, QueryRequest},
    query_result::QueryOutputFile,
};
use futures::TryStreamExt;
use std::sync::Arc;

/// Resolves the HTTP super-user flag from the request's `Authorization` header.
///
/// - No `Authorization` header → anonymous, read-only (`Ok(false)`).
/// - Valid admin basic credentials → super-user, DDL/DML allowed (`Ok(true)`).
/// - An `Authorization` header that fails validation → `Err(UNAUTHORIZED)` so
///   bad credentials surface as an error instead of silently degrading to
///   read-only.
fn resolve_super_user(headers: &HeaderMap) -> Result<bool, StatusCode> {
    if headers.contains_key(header::AUTHORIZATION) {
        crate::axum::auth::verify_basic_auth_header(headers)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Executes a query against the runtime and streams the result to the client.
///
/// The response is either an Arrow IPC stream (zstd-compressed) for in-memory
/// results or a file download when the query produced a materialized output
/// file (CSV, Parquet, Arrow, JSON, ODV, NetCDF, GeoParquet).
#[tracing::instrument(level = "info", skip(state, headers))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/query",
    responses(
        (status=200, description="Response containing the query results in the format specified by the query"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn query(
    State(state): State<Arc<Runtime>>,
    headers: HeaderMap,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let query = query_obj.into_query().map_err(|err| {
        tracing::error!("Error parsing beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    // SQL over the HTTP client API is gated by `sql.enable` (JSON is always
    // allowed); the Flight SQL transport has its own `flight_sql.enable`.
    if matches!(query.inner, beacon_core::query::InnerQuery::Sql(_))
        && !beacon_config::CONFIG.sql.enable
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json("SQL queries are not enabled".to_string()),
        ));
    }

    // HTTP client queries are read-only by default; supplying valid admin basic
    // credentials elevates the request to super-user, allowing DDL/DML (e.g.
    // CREATE EXTERNAL TABLE, CREATE/INSERT on managed tables) over HTTP. This
    // mirrors the Flight SQL transport, where basic auth resolves to an admin
    // `AuthContext`.
    let is_super_user = resolve_super_user(&headers).map_err(|status| {
        (status, Json("invalid admin credentials".to_string()))
    })?;
    let query_result = state.run_query(query, is_super_user).await.map_err(|err| {
        tracing::error!("Error running beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    match query_result.query_output {
        beacon_core::query_result::QueryOutput::File(query_output_file) => {
            handle_query_output_file(query_output_file, query_result.query_id).await
        }
        beacon_core::query_result::QueryOutput::Stream(arrow_output_stream) => {
            let ipc_options = arrow::ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
                .map_err(|err| {
                    tracing::error!("failed to configure Arrow IPC zstd compression: {err}");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json("failed to configure Arrow IPC compression".to_string()),
                    )
                })?;

            let query_id_header = HeaderValue::from_str(&query_result.query_id.to_string())
                .map_err(|err| {
                    tracing::error!("failed to encode query id header: {err}");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json("failed to encode response headers".to_string()),
                    )
                })?;
            // Static content-disposition value is ASCII and cannot fail at runtime; wrapped for parity.
            let content_disposition =
                HeaderValue::from_static("application/vnd.apache.arrow.stream");

            let schema = arrow_output_stream.schema();

            // Stream Arrow IPC directly so large result sets do not need to be buffered in memory.
            let axum_stream = axum_streams::StreamBodyAs::arrow_ipc_with_options_errors(
                schema,
                arrow_output_stream.map_err(|e| ::axum::Error::new(Box::new(e))),
                ipc_options,
            )
            .header("x-beacon-query-id", query_id_header)
            .header(header::CONTENT_DISPOSITION, content_disposition);

            Ok(axum_stream.into_response())
        }
    }
}

/// Converts a completed file-backed query result into a streamed HTTP response.
async fn handle_query_output_file(
    output_file: QueryOutputFile,
    query_id: uuid::Uuid,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    match output_file {
        QueryOutputFile::Csv(named_temp_file) => {
            file_stream_response(named_temp_file.path(), "text/csv", "csv", query_id).await
        }
        QueryOutputFile::Parquet(named_temp_file) => {
            file_stream_response(
                named_temp_file.path(),
                "application/vnd.apache.parquet",
                "parquet",
                query_id,
            )
            .await
        }
        QueryOutputFile::Ipc(named_temp_file) => {
            file_stream_response(
                named_temp_file.path(),
                "application/vnd.apache.arrow.file",
                "arrow",
                query_id,
            )
            .await
        }
        QueryOutputFile::Json(named_temp_file) => {
            file_stream_response(named_temp_file.path(), "application/json", "json", query_id).await
        }
        QueryOutputFile::Odv(named_temp_file) => {
            file_stream_response(named_temp_file.path(), "application/zip", "zip", query_id).await
        }
        QueryOutputFile::NetCDF(named_temp_file) => {
            file_stream_response(named_temp_file.path(), "application/netcdf", "nc", query_id).await
        }
        QueryOutputFile::GeoParquet(named_temp_file) => {
            file_stream_response(
                named_temp_file.path(),
                "application/vnd.apache.arrow.geo+parquet",
                "geoparquet",
                query_id,
            )
            .await
        }
    }
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

/// Validates a query body by parsing it. Returns 200 if the payload deserializes
/// into a [`QueryRequest`] — the query is not executed.
#[tracing::instrument(level = "info")]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/parse-query",
    responses(
        (status=200, description="Valid Query"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn parse_query(Json(_query_obj): Json<QueryRequest>) -> StatusCode {
    StatusCode::OK
}

/// Returns recorded planner metrics for a previously executed query.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    get,
    path = "/api/query/metrics/{query_id}",
    responses(
        (status=200, description="Response containing the query metrics"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn query_metrics(
    State(state): State<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Result<Json<QueryMetricsView>, (StatusCode, Json<String>)> {
    let query_id = uuid::Uuid::parse_str(&query_id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json("Invalid UUID format".to_string()),
        )
    })?;

    let metrics = state.get_query_metrics(query_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json("Query ID not found".to_string()),
        )
    })?;

    Ok(Json(metrics))
}

/// Returns a JSON-encoded explanation of the plan the runtime would produce for
/// the supplied query without executing it.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/explain-query",
    responses(
        (status=200, description="Explains the produced plan of a given query"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn explain_query(
    State(state): State<Arc<Runtime>>,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let result = state.explain_client_query(query_obj).await;
    match result {
        Ok(explanation) => Ok((
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CONTENT_DISPOSITION, "attachment"),
            ],
            Body::from(explanation),
        )
            .into_response()),
        Err(err) => {
            tracing::error!("Error explaining beacon query: {}", err);
            Err((StatusCode::BAD_REQUEST, Json(err.to_string())))
        }
    }
}

/// Backward-compatible endpoint for clients that still request the default schema as column names.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    get,
    path = "/api/query/available-columns",
    responses(
        (status=200, description="Response containing the available columns in the default table schema"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
#[deprecated = "Use /api/default-table-schema instead"]
pub(crate) async fn available_columns(State(state): State<Arc<Runtime>>) -> Json<Vec<String>> {
    Json(
        state
            .list_default_table_schema_view()
            .await
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect(),
    )
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

    /// Valid admin credentials elevate the HTTP request to super-user so DDL/DML
    /// is allowed over HTTP.
    #[test]
    fn valid_admin_credentials_resolve_to_super_user() {
        let headers = basic_auth_headers(
            &beacon_config::CONFIG.admin.username,
            &beacon_config::CONFIG.admin.password,
        );
        assert_eq!(resolve_super_user(&headers), Ok(true));
    }

    /// No credentials at all stays anonymous (read-only), not an error.
    #[test]
    fn missing_authorization_header_is_anonymous() {
        let headers = HeaderMap::new();
        assert_eq!(resolve_super_user(&headers), Ok(false));
    }

    /// Credentials that are present but wrong are rejected rather than silently
    /// degrading to read-only.
    #[test]
    fn wrong_credentials_are_rejected() {
        let headers = basic_auth_headers("not-the-admin", "wrong-password");
        assert_eq!(resolve_super_user(&headers), Err(StatusCode::UNAUTHORIZED));
    }
}
