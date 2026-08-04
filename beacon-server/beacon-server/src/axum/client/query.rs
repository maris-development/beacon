//! Query execution endpoints for the client HTTP API.

use ::axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use crate::api::{QueryMetricsView, QueryRequest};
use crate::server::sql::rows_from_batches;
use crate::server::Server;
use beacon_core::{
    query::Query,
    query_result::QueryOutputFile,
    AuthIdentity,
};
use futures::TryStreamExt;
use std::sync::Arc;

/// Executes a query against the runtime and streams the result to the client.
///
/// The response is either an Arrow IPC stream (zstd-compressed) for in-memory
/// results or a file download when the query produced a materialized output
/// file (CSV, Parquet, Arrow, JSON, ODV, NetCDF, GeoParquet).
#[tracing::instrument(level = "info", skip(state, identity))]
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
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let query = query_obj.into_query().map_err(|err| {
        tracing::error!("Error parsing beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    // SQL over the HTTP client API is gated by `sql.enable` (JSON is always
    // allowed); the Flight SQL transport has its own `flight_sql.enable`.
    if matches!(query.inner, beacon_core::query::InnerQuery::Sql(_))
        && !state.config().sql.enable
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json("SQL queries are not enabled".to_string()),
        ));
    }

    // The caller's identity is resolved by the `resolve_identity` middleware and stored in the
    // request extensions: a super-user (e.g. the admin) may run DDL/DML, while anonymous/role
    // users are gated by `validate_query_plan` (and read-authz when enforcement is enabled).
    let query_result = state.runtime().run_query(query, identity).await.map_err(|err| {
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
    // `QueryOutputFile` carries its own format now, and with it the MIME type and
    // extension — so the transport no longer restates the format table.
    file_stream_response(
        output_file.path(),
        output_file.kind().content_type(),
        output_file.kind().suggested_extension().trim_start_matches('.'),
        query_id,
    )
    .await
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

/// Returns recorded planner/execution metrics for a previously executed query.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    get,
    path = "/api/query/metrics/{query_id}",
    params(
        ("query_id" = String, Path, description = "UUID of a previously executed query")
    ),
    responses(
        (status = 200, description = "Recorded metrics for the query", body = QueryMetricsView),
        (status = 400, description = "The query id is not a valid UUID"),
        (status = 404, description = "No metrics recorded for the given query id"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn query_metrics(
    State(state): State<Arc<Server>>,
    Path(query_id): Path<String>,
) -> Result<Json<QueryMetricsView>, (StatusCode, Json<String>)> {
    let query_id = uuid::Uuid::parse_str(&query_id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json("Invalid UUID format".to_string()),
        )
    })?;

    // The metrics live in a managed table, so this is a row read: an empty result
    // means nothing was recorded under that id.
    let batches = state
        .runtime()
        .get_query_metrics(query_id)
        .await
        .map_err(|error| {
            tracing::error!(%query_id, ?error, "failed to read query metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json("Failed to read query metrics".to_string()),
            )
        })?;
    let row = rows_from_batches(&batches)
        .map_err(|error| {
            tracing::error!(%query_id, ?error, "failed to decode query metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json("Failed to read query metrics".to_string()),
            )
        })?
        .into_iter()
        .next()
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json("Query ID not found".to_string()),
            )
        })?;

    Ok(Json(QueryMetricsView::from_row(&row)))
}

/// Returns a JSON-encoded explanation of the plan the runtime would produce for
/// the supplied query, without executing it.
#[tracing::instrument(level = "info", skip(state, identity))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/explain-query",
    request_body = Query,
    responses(
        (
            status = 200,
            description = "pgjson explanation of the plan the runtime would produce \
                for the query (the query is not executed)",
            content_type = "application/json"
        ),
        (status = 400, description = "Invalid or unsupported query"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn explain_query(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let query = query_obj.into_query().map_err(|err| {
        tracing::error!("Error parsing beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    // EXPLAIN does not execute, so it is not gated by `sql.enable`: it exposes
    // no more than `/api/parse-query` already does about a rejected query.
    match state.runtime().explain_query(query, identity).await {
        Ok(explanation) => Ok((
            [(header::CONTENT_TYPE, "application/json")],
            Body::from(explanation),
        )
            .into_response()),
        Err(err) => {
            tracing::error!("Error explaining beacon query: {}", err);
            Err((StatusCode::BAD_REQUEST, Json(err.to_string())))
        }
    }
}

/// Runs the supplied query and returns its physical plan annotated with per-node
/// runtime metrics as PostgreSQL-style JSON (pgjson) — the `EXPLAIN ANALYZE`
/// analog of `/api/explain-query`. Unlike that endpoint, the query IS executed.
#[tracing::instrument(level = "info", skip(state, identity))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/explain-analyze-query",
    request_body = Query,
    responses(
        (
            status = 200,
            description = "pgjson explanation of the query's physical plan annotated \
                with per-node runtime metrics (the query IS executed to collect them)",
            content_type = "application/json"
        ),
        (status = 400, description = "Invalid or unsupported query"),
        (status = 401, description = "Basic credentials were supplied but are invalid"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn explain_analyze_query(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let query = query_obj.into_query().map_err(|err| {
        tracing::error!("Error parsing beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    // EXPLAIN ANALYZE executes the query, so it is gated by `sql.enable` exactly
    // like `/api/query` (JSON is always allowed). Without this, SQL could be run
    // through this endpoint while SQL is disabled, bypassing the restriction.
    if matches!(query.inner, beacon_core::query::InnerQuery::Sql(_)) && !state.config().sql.enable {
        return Err((
            StatusCode::BAD_REQUEST,
            Json("SQL queries are not enabled".to_string()),
        ));
    }

    match state.runtime().explain_analyze_query(query, identity).await {
        Ok(explanation) => Ok((
            [(header::CONTENT_TYPE, "application/json")],
            Body::from(explanation),
        )
            .into_response()),
        Err(err) => {
            tracing::error!("Error explain-analyzing beacon query: {}", err);
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
        (status = 200, description = "Column names of the default table schema", body = Vec<String>),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
#[deprecated = "Use /api/default-table-schema instead"]
pub(crate) async fn available_columns(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Json<Vec<String>> {
    let table = crate::server::catalog::table_reference(
        None,
        None,
        &crate::server::catalog::default_table(&state),
    );
    let columns = crate::server::catalog::table_schema(&state, table, identity)
        .await
        .unwrap_or(None)
        .map(|schema| schema.fields().iter().map(|f| f.name().clone()).collect())
        .unwrap_or_default();
    Json(columns)
}
