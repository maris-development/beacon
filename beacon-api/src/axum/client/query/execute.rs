//! One-shot query execution and validation.
//!
//! `POST /api/query` runs a query and returns its result inline (an Arrow IPC
//! stream for in-memory results, or a file download for materialized output
//! formats). `POST /api/parse-query` validates a body without executing it, and
//! the deprecated `GET /api/query/available-columns` lists the default table's
//! columns.

use ::axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::runtime::Runtime;
use beacon_core::{
    api::QueryRequest,
    query::Query,
    query_result::{QueryOutput, QueryOutputFile},
};
use futures::TryStreamExt;
use std::sync::Arc;

use super::{ensure_sql_allowed, file_stream_response, invalid_credentials, resolve_super_user};

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
    request_body = Query,
    responses(
        (
            status = 200,
            description = "Query results in the format requested by the query. The \
                default is a zstd-compressed Arrow IPC stream; file output formats \
                (CSV, Parquet, Arrow, ODV, NetCDF, GeoParquet) are returned as \
                a file download. The result's query id is returned in the \
                `x-beacon-query-id` response header.",
            content_type = "application/vnd.apache.arrow.stream"
        ),
        (status = 400, description = "Invalid, unsupported, or disabled query"),
        (status = 401, description = "Basic credentials were supplied but are invalid"),
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

    ensure_sql_allowed(&query, &state)?;

    // HTTP client queries are read-only by default; supplying valid admin basic
    // credentials elevates the request to super-user, allowing DDL/DML (e.g.
    // CREATE EXTERNAL TABLE, CREATE/INSERT on managed tables) over HTTP. This
    // mirrors the Flight SQL transport, where basic auth resolves to an admin
    // `AuthContext`.
    let is_super_user =
        resolve_super_user(&headers, &state.config().admin).map_err(invalid_credentials)?;
    let query_result = state.run_query(query, is_super_user).await.map_err(|err| {
        tracing::error!("Error running beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    match query_result.query_output {
        QueryOutput::File(query_output_file) => {
            handle_query_output_file(query_output_file, query_result.query_id).await
        }
        QueryOutput::Stream(arrow_output_stream) => {
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
                arrow_output_stream.map_err(|e| {
                    // The 200 OK status and headers are already on the wire by the
                    // time batches stream, so a mid-stream error cannot surface as an
                    // HTTP error code: the client just sees a truncated Arrow IPC
                    // stream. Log it here so the failure is at least observable
                    // server-side instead of being silently swallowed.
                    tracing::error!("error producing Arrow stream batch: {e}");
                    ::axum::Error::new(Box::new(e))
                }),
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
    file_stream_response(
        output_file.path(),
        output_file.content_type(),
        output_file.extension(),
        query_id,
    )
    .await
}

/// Validates a query body by parsing it. Returns 200 if the payload deserializes
/// into a [`QueryRequest`] — the query is not executed.
#[tracing::instrument(level = "info")]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/parse-query",
    request_body = Query,
    responses(
        (status = 200, description = "The query body is valid (it was not executed)"),
        (status = 400, description = "Malformed query body"),
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

/// Backward-compatible endpoint for clients that still request the default schema as column names.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    get,
    path = "/api/query/available-columns",
    responses(
        (status = 200, description = "Column names of the default table schema", body = Vec<String>),
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
