//! Query execution endpoints for the client HTTP API.

use ::axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use beacon_core::runtime::Runtime;
use beacon_core::{
    api::{QueryMetricsView, QueryRequest},
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
    State(state): State<Arc<Runtime>>,
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
    let query_result = state.run_query(query, identity).await.map_err(|err| {
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
        QueryOutputFile::Zarr(named_temp_file) => {
            // A zarr store is a directory tree, delivered as a zip archive.
            file_stream_response(
                named_temp_file.path(),
                "application/zip",
                "zarr.zip",
                query_id,
            )
            .await
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

/// Returns recorded planner metrics for a previously executed query.
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
#[tracing::instrument(level = "info", skip(state, identity))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/explain-query",
    request_body = Query,
    responses(
        (
            status = 200,
            description = "JSON explanation of the plan the runtime would produce \
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
    State(state): State<Arc<Runtime>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let result = state.explain_client_query(query_obj, identity).await;
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

/// Runs the supplied query and returns its physical plan annotated with per-node
/// runtime metrics as PostgreSQL-style JSON (pgjson) — the `EXPLAIN ANALYZE`
/// analog of `/api/explain-query`. Unlike that endpoint, the query is executed.
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
    State(state): State<Arc<Runtime>>,
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
    if matches!(query.inner, beacon_core::query::InnerQuery::Sql(_))
        && !state.config().sql.enable
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json("SQL queries are not enabled".to_string()),
        ));
    }

    // The identity is resolved by the `resolve_identity` middleware: anonymous/role users are
    // gated by `validate_query_plan`, while a super-user may also analyze DDL/DML (with the same
    // side effects, since this executes the plan).
    let result = state.explain_analyze_client_query(query, identity).await;
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
