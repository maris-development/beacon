//! Authenticated dataset file-management endpoints (upload / download / delete).
//!
//! These operate on the datasets store through the runtime, which owns the path
//! safety, extension allowlist, size cap, and delete dependency check. All three
//! routes sit behind the super-user `basic_auth` gate attached in `setup_router`.

use std::sync::Arc;

use ::axum::{
    body::{to_bytes, Body},
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use crate::datalake::{
    files::{FileError, UploadResult},
    DataLake,
};
use futures::TryStreamExt;

/// Hard cap on a single chunked-upload part's body, bounding per-request memory
/// (each part is buffered in full to make it atomically retryable). Generously
/// above the advertised part size so a client choosing a larger part still works.
const MAX_PART_BYTES: usize = 128 * 1024 * 1024;

/// Query parameters for an upload: destination key plus an optional overwrite flag.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct UploadParams {
    /// Destination key relative to the datasets root, e.g. `ctd/cruise42/a.nc`.
    path: String,
    /// Replace an existing file instead of failing with `409`. Defaults to false.
    #[serde(default)]
    overwrite: bool,
}

/// Query parameters for download/delete: the dataset key.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct PathParams {
    /// Dataset key relative to the datasets root.
    path: String,
}

/// Streams an uploaded file into the datasets store.
///
/// The body is the raw file bytes, streamed straight into a multipart writer so
/// memory stays flat for large scientific files. The runtime validates the path
/// (anti-traversal, internal-prefix guard), checks the extension against the
/// readable-format allowlist, and enforces `BEACON_MAX_UPLOAD_BYTES`.
#[tracing::instrument(level = "info", skip(state, body))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/datasets/upload",
    params(
        ("path" = String, Query, description = "Destination key relative to the datasets root"),
        ("overwrite" = Option<bool>, Query, description = "Replace an existing file (default false)")
    ),
    request_body(content = Vec<u8>, description = "Raw file bytes", content_type = "application/octet-stream"),
    responses(
        (status = 200, description = "File uploaded", body = UploadResult),
        (status = 400, description = "Invalid path or unsupported extension"),
        (status = 409, description = "A file already exists at the destination"),
        (status = 413, description = "File exceeds the maximum upload size")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn upload_dataset(
    State(state): State<Arc<DataLake>>,
    Query(params): Query<UploadParams>,
    body: Body,
) -> Result<Json<UploadResult>, (StatusCode, String)> {
    let stream = body.into_data_stream().map_err(std::io::Error::other);
    let result = state
        .upload_dataset(&params.path, params.overwrite, stream)
        .await
        .map_err(file_error)?;
    Ok(Json(result))
}

/// Streams a dataset file back to the caller as an attachment.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/datasets/download",
    params(("path" = String, Query, description = "Dataset key relative to the datasets root")),
    responses(
        (status = 200, description = "File contents", content_type = "application/octet-stream"),
        (status = 400, description = "Invalid path"),
        (status = 404, description = "Dataset not found")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn download_dataset(
    State(state): State<Arc<DataLake>>,
    Query(params): Query<PathParams>,
) -> Result<Response, (StatusCode, String)> {
    let result = state.download_dataset(&params.path).await.map_err(file_error)?;
    let size = result.meta.size;
    let filename = attachment_filename(&params.path);
    let body = Body::from_stream(result.into_stream());

    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CONTENT_LENGTH, size.to_string()),
        ],
        body,
    )
        .into_response())
}

/// Query parameters identifying a chunked upload and a part within it.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct PartParams {
    /// The session id returned by `initiate`.
    upload_id: String,
    /// 1-based part number; parts must be submitted in order.
    part_number: u32,
}

/// Query parameters identifying a chunked upload session.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct UploadIdParams {
    upload_id: String,
}

/// Response from initiating a chunked upload.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub(crate) struct InitiateResult {
    /// Session id to pass to subsequent part/complete/abort calls.
    upload_id: String,
    /// Size, in bytes, clients should slice the file into for each part.
    part_size: usize,
}

/// Begins a chunked (resumable) upload for a large file.
///
/// The client then PUTs each part to `…/upload/part` in order and finishes with
/// `…/upload/complete` (or `DELETE …/upload` to abort). Path/extension/overwrite
/// are validated here, up front, before any part is accepted.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/datasets/upload/initiate",
    params(
        ("path" = String, Query, description = "Destination key relative to the datasets root"),
        ("overwrite" = Option<bool>, Query, description = "Replace an existing file (default false)")
    ),
    responses(
        (status = 200, description = "Upload session created", body = InitiateResult),
        (status = 400, description = "Invalid path or unsupported extension"),
        (status = 409, description = "A file already exists at the destination")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn initiate_upload(
    State(state): State<Arc<DataLake>>,
    Query(params): Query<UploadParams>,
) -> Result<Json<InitiateResult>, (StatusCode, String)> {
    let (id, part_size) = state
        .initiate_dataset_upload(&params.path, params.overwrite)
        .await
        .map_err(file_error)?;
    Ok(Json(InitiateResult {
        upload_id: id.to_string(),
        part_size,
    }))
}

/// Submits one part of a chunked upload. The body is the raw bytes of the part;
/// it is buffered in full (bounded by `MAX_PART_BYTES`) so the part is atomic and
/// can be safely retried by resending the same `part_number`.
#[tracing::instrument(level = "info", skip(state, body))]
#[utoipa::path(
    tag = "admin",
    put,
    path = "/api/admin/datasets/upload/part",
    params(
        ("upload_id" = String, Query, description = "Session id from initiate"),
        ("part_number" = u32, Query, description = "1-based part number, submitted in order")
    ),
    request_body(content = Vec<u8>, description = "Raw part bytes", content_type = "application/octet-stream"),
    responses(
        (status = 204, description = "Part accepted"),
        (status = 404, description = "Unknown or expired upload session"),
        (status = 409, description = "Part out of order"),
        (status = 413, description = "Part or cumulative size exceeds the limit")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn upload_part(
    State(state): State<Arc<DataLake>>,
    Query(params): Query<PartParams>,
    body: Body,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = parse_upload_id(&params.upload_id)?;
    let bytes = to_bytes(body, MAX_PART_BYTES).await.map_err(|_| {
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("upload part exceeds the maximum part size of {MAX_PART_BYTES} bytes"),
        )
    })?;
    state
        .upload_dataset_part(id, params.part_number, bytes)
        .await
        .map_err(file_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Finalizes a chunked upload, assembling the parts into the destination object.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/datasets/upload/complete",
    params(("upload_id" = String, Query, description = "Session id from initiate")),
    responses(
        (status = 200, description = "Upload completed", body = UploadResult),
        (status = 404, description = "Unknown or expired upload session")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn complete_upload(
    State(state): State<Arc<DataLake>>,
    Query(params): Query<UploadIdParams>,
) -> Result<Json<UploadResult>, (StatusCode, String)> {
    let id = parse_upload_id(&params.upload_id)?;
    let result = state.complete_dataset_upload(id).await.map_err(file_error)?;
    Ok(Json(result))
}

/// Aborts and discards an in-progress chunked upload.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    delete,
    path = "/api/admin/datasets/upload",
    params(("upload_id" = String, Query, description = "Session id from initiate")),
    responses(
        (status = 204, description = "Upload aborted"),
        (status = 404, description = "Unknown or expired upload session")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn abort_upload(
    State(state): State<Arc<DataLake>>,
    Query(params): Query<UploadIdParams>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = parse_upload_id(&params.upload_id)?;
    state.abort_dataset_upload(id).await.map_err(file_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Parse an `upload_id` query value into a UUID, mapping a malformed id to `400`.
fn parse_upload_id(raw: &str) -> Result<uuid::Uuid, (StatusCode, String)> {
    uuid::Uuid::parse_str(raw)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid upload_id".to_string()))
}

/// Deletes a dataset file, refusing if a registered table references it.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    delete,
    path = "/api/admin/datasets",
    params(("path" = String, Query, description = "Dataset key relative to the datasets root")),
    responses(
        (status = 204, description = "File deleted"),
        (status = 400, description = "Invalid path"),
        (status = 404, description = "Dataset not found"),
        (status = 409, description = "Dataset is in use by one or more tables")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn delete_dataset(
    State(state): State<Arc<DataLake>>,
    Query(params): Query<PathParams>,
) -> Result<StatusCode, (StatusCode, String)> {
    state.delete_dataset(&params.path).await.map_err(file_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Derive a safe `Content-Disposition` filename from the dataset key: the last
/// path segment with any quote/control characters stripped to prevent header
/// injection.
fn attachment_filename(path: &str) -> String {
    let base = path.rsplit('/').next().unwrap_or("download");
    let cleaned: String = base
        .chars()
        .filter(|c| *c != '"' && *c != '\\' && !c.is_control())
        .collect();
    if cleaned.is_empty() {
        "download".to_string()
    } else {
        cleaned
    }
}

/// Map a [`FileError`] to an HTTP status + message. The mapping is the contract
/// the web UI relies on to distinguish (e.g.) "already exists" from "too large".
fn file_error(err: FileError) -> (StatusCode, String) {
    let status = match &err {
        FileError::InvalidPath(_) | FileError::Body(_) => StatusCode::BAD_REQUEST,
        FileError::AlreadyExists(_)
        | FileError::InUse { .. }
        | FileError::PartOutOfOrder { .. } => StatusCode::CONFLICT,
        FileError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        FileError::NotFound(_) | FileError::UnknownUpload(_) => StatusCode::NOT_FOUND,
        FileError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    tracing::warn!(error = %err, "dataset file operation failed");
    (status, err.to_string())
}
