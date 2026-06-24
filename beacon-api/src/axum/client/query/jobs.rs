//! Asynchronous query jobs: submit a query, then poll for status and results.
//!
//! Unlike `POST /api/query`, which commits `200 OK` before the first batch (so a
//! mid-stream failure can only truncate the response), a job decouples execution
//! from delivery. Streamable jobs hand back Arrow batches incrementally via
//! `GET /api/query/jobs/{id}/stream`; file jobs materialize fully and are
//! downloaded via `GET /api/query/jobs/{id}/result`. Either way a mid-execution
//! error surfaces cleanly as a terminal `failed` status.

use ::axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::runtime::Runtime;
use beacon_core::{
    api::{QueryJobStatusView, QueryJobSubmitView, QueryRequest},
    query::Query,
    query_job::{CancelOutcome, JobKind, PollOutcome, QueryJobState},
};
use std::sync::Arc;

use super::{ensure_sql_allowed, file_stream_response, invalid_credentials, parse_query_id,
    resolve_super_user};

/// Serializes a chunk of batches as a self-contained, zstd-compressed Arrow IPC
/// stream (schema + batches), matching the `/api/query` streaming content type.
fn encode_arrow_ipc(
    schema: arrow::datatypes::SchemaRef,
    batches: &[arrow::array::RecordBatch],
) -> Result<Vec<u8>, (StatusCode, Json<String>)> {
    let options = arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
        .map_err(|err| {
            tracing::error!("failed to configure Arrow IPC zstd compression: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json("failed to configure Arrow IPC compression".to_string()),
            )
        })?;
    let mut buf = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new_with_options(&mut buf, &schema, options)
                .map_err(|err| {
                    tracing::error!("failed to create Arrow IPC writer: {err}");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json("failed to serialize query result".to_string()),
                    )
                })?;
        for batch in batches {
            writer.write(batch).map_err(|err| {
                tracing::error!("failed to write Arrow IPC batch: {err}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json("failed to serialize query result".to_string()),
                )
            })?;
        }
        writer.finish().map_err(|err| {
            tracing::error!("failed to finish Arrow IPC stream: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json("failed to serialize query result".to_string()),
            )
        })?;
    }
    Ok(buf)
}

/// Submits a query for asynchronous execution and returns its job id.
///
/// Planning and validation happen synchronously, so an invalid query is rejected
/// here with `400`; execution then runs in the background. Results are retrieved
/// by polling: streamable jobs via `GET /api/query/jobs/{id}/stream`, file jobs
/// via `GET /api/query/jobs/{id}` then `GET /api/query/jobs/{id}/result`. Auth
/// mirrors `/api/query` (valid admin basic credentials elevate to super-user).
#[tracing::instrument(level = "info", skip(state, headers))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/query/jobs",
    request_body = Query,
    responses(
        (status = 202, description = "Job accepted; poll for status and results", body = QueryJobSubmitView),
        (status = 400, description = "Invalid, unsupported, or disabled query"),
        (status = 401, description = "Basic credentials were supplied but are invalid"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn submit_query_job(
    State(state): State<Arc<Runtime>>,
    headers: HeaderMap,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let query = query_obj.into_query().map_err(|err| {
        tracing::error!("Error parsing beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    ensure_sql_allowed(&query, &state)?;

    let is_super_user =
        resolve_super_user(&headers, &state.config().admin).map_err(invalid_credentials)?;

    let (query_id, kind) = state
        .submit_query_job(query, is_super_user)
        .await
        .map_err(|err| {
            tracing::error!("Error submitting beacon query job: {}", err);
            (StatusCode::BAD_REQUEST, Json(err.to_string()))
        })?;

    let query_id_header = HeaderValue::from_str(&query_id.to_string()).map_err(|err| {
        tracing::error!("failed to encode query id header: {err}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json("failed to encode response headers".to_string()),
        )
    })?;

    let body = QueryJobSubmitView {
        query_id: query_id.to_string(),
        kind,
    };
    Ok((
        StatusCode::ACCEPTED,
        [(HeaderName::from_static("x-beacon-query-id"), query_id_header)],
        Json(body),
    )
        .into_response())
}

/// Returns the current status of an async query job.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    get,
    path = "/api/query/jobs/{query_id}",
    params(
        ("query_id" = String, Path, description = "UUID returned by POST /api/query/jobs")
    ),
    responses(
        (status = 200, description = "Current job status", body = QueryJobStatusView),
        (status = 400, description = "The query id is not a valid UUID"),
        (status = 404, description = "No job with the given id"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn query_job_status(
    State(state): State<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Result<Json<QueryJobStatusView>, (StatusCode, Json<String>)> {
    let query_id = parse_query_id(&query_id)?;
    let snapshot = state.query_job_snapshot(query_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json("Query job not found".to_string()),
        )
    })?;
    Ok(Json(QueryJobStatusView::from(snapshot)))
}

/// Long-polls a streamable job for its next batches.
///
/// Returns a zstd-compressed Arrow IPC stream (`200`) when batches are ready, a
/// `204` when none became ready within the server-side wait (still running), or
/// a JSON terminal body (`{"state":"completed"}` / `{"state":"failed","error":…}`)
/// once the job finishes. A mid-execution error therefore surfaces cleanly here
/// after any already-produced batches were delivered — unlike `/api/query`, where
/// it can only truncate the stream. Batches are delivered at-most-once: a poll
/// response that does not reach the client loses those rows.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    get,
    path = "/api/query/jobs/{query_id}/stream",
    params(
        ("query_id" = String, Path, description = "UUID of a streamable job")
    ),
    responses(
        (status = 200, description = "Arrow IPC batches, or a JSON terminal status", content_type = "application/vnd.apache.arrow.stream"),
        (status = 204, description = "No batch ready yet; the job is still running"),
        (status = 400, description = "The query id is not a valid UUID"),
        (status = 404, description = "No job with the given id"),
        (status = 409, description = "The job is a file job, or was cancelled"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn query_job_stream(
    State(state): State<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let id = parse_query_id(&query_id)?;
    let wait = state.query_job_poll_wait();
    match state.poll_query_job_stream(id, wait).await {
        PollOutcome::Batches { schema, batches } => {
            let body = encode_arrow_ipc(schema, &batches)?;
            Ok((
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/vnd.apache.arrow.stream"),
                )],
                body,
            )
                .into_response())
        }
        PollOutcome::Pending => Ok(StatusCode::NO_CONTENT.into_response()),
        PollOutcome::Completed => {
            Ok(Json(serde_json::json!({ "state": "completed" })).into_response())
        }
        PollOutcome::Failed(error) => {
            Ok(Json(serde_json::json!({ "state": "failed", "error": error })).into_response())
        }
        PollOutcome::NotStreamable => Err((
            StatusCode::CONFLICT,
            Json("job produces a file result; poll status and download via /result".to_string()),
        )),
        PollOutcome::Cancelled => Err((
            StatusCode::CONFLICT,
            Json("query job was cancelled".to_string()),
        )),
        PollOutcome::NotFound => Err((
            StatusCode::NOT_FOUND,
            Json("Query job not found".to_string()),
        )),
    }
}

/// Downloads the materialized result of a completed file job.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    get,
    path = "/api/query/jobs/{query_id}/result",
    params(
        ("query_id" = String, Path, description = "UUID of a file job")
    ),
    responses(
        (status = 200, description = "The materialized result file"),
        (status = 400, description = "Bad UUID, or the job failed (body carries the error)"),
        (status = 404, description = "No job with the given id"),
        (status = 409, description = "Streamable job, still running, or cancelled"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn query_job_result(
    State(state): State<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let id = parse_query_id(&query_id)?;
    let snapshot = state.query_job_snapshot(id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json("Query job not found".to_string()),
        )
    })?;

    if matches!(snapshot.kind, JobKind::Streamable) {
        return Err((
            StatusCode::CONFLICT,
            Json("streamable job; consume results via /stream".to_string()),
        ));
    }

    match snapshot.state {
        QueryJobState::Running => Err((
            StatusCode::CONFLICT,
            Json("query job is still running; result requires completion".to_string()),
        )),
        QueryJobState::Failed { error } => Err((StatusCode::BAD_REQUEST, Json(error))),
        QueryJobState::Cancelled => Err((
            StatusCode::CONFLICT,
            Json("query job was cancelled".to_string()),
        )),
        QueryJobState::Succeeded => {
            let file = snapshot.file.ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json("succeeded job has no result file".to_string()),
                )
            })?;
            file_stream_response(&file.path, file.content_type, file.file_ext, id).await
        }
    }
}

/// Cancels a running job, aborting its background execution.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    delete,
    path = "/api/query/jobs/{query_id}",
    params(
        ("query_id" = String, Path, description = "UUID of the job to cancel")
    ),
    responses(
        (status = 200, description = "The job was cancelled"),
        (status = 400, description = "The query id is not a valid UUID"),
        (status = 404, description = "No job with the given id"),
        (status = 409, description = "The job had already finished"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn cancel_query_job(
    State(state): State<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Result<(StatusCode, Json<String>), (StatusCode, Json<String>)> {
    let id = parse_query_id(&query_id)?;
    match state.cancel_query_job(id) {
        CancelOutcome::Cancelled => Ok((StatusCode::OK, Json("cancelled".to_string()))),
        CancelOutcome::AlreadyFinished => Err((
            StatusCode::CONFLICT,
            Json("query job already finished".to_string()),
        )),
        CancelOutcome::NotFound => Err((
            StatusCode::NOT_FOUND,
            Json("Query job not found".to_string()),
        )),
    }
}
