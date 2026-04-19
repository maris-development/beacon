//! Administrative file endpoints: upload, download, and delete data lake files.

use std::sync::Arc;

use ::axum::{
    extract::{Multipart, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use beacon_core::runtime::Runtime;
use futures::StreamExt;
use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

/// OpenAPI schema describing the multipart form accepted by [`upload_file`].
///
/// Fields are read dynamically from the request in [`upload_file`]; this struct
/// exists solely to teach utoipa about the shape of the multipart body.
#[derive(Deserialize, ToSchema)]
#[allow(unused)]
struct UploadDatasetMultipart {
    #[schema(example = "base_dir/sub_dir")]
    prefix: String,
    #[schema(format = Binary, content_media_type = "application/octet-stream")]
    file: String,
}

/// Uploads a file into the Beacon data lake under the supplied prefix.
///
/// Expects a `multipart/form-data` body with a `prefix` text field (the
/// destination directory) and a `file` binary field. Body size is unbounded
/// because the router disables [`axum::extract::DefaultBodyLimit`] for the
/// admin surface.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "file",
    post,
    request_body(content = UploadDatasetMultipart, content_type = "multipart/form-data"),
    path = "/api/admin/upload-file", 
    responses((status = 200, description = "File uploaded successfully")),
    security(
        ("basic-auth" = []),
        ("bearer" = [])
    ))
]
pub async fn upload_file(
    State(state): State<Arc<Runtime>>,
    mut multipart: Multipart,
) -> Result<Json<String>, Json<String>> {
    let mut prefix: Option<String> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| Json(format!("Multipart error: {e}")))?
    {
        // Reject fields without a name: OpenAPI requires named parts, and accepting them
        // silently would cause upload payloads to be dropped without explanation.
        let Some(name) = field.name().map(str::to_string) else {
            tracing::warn!("Rejected multipart field without a name");
            return Err(Json("multipart field missing name".to_string()));
        };

        match name.as_str() {
            "prefix" => {
                let value = field
                    .text()
                    .await
                    .map_err(|e| Json(format!("failed to read `prefix` field: {e}")))?;
                prefix = Some(value);
            }
            "file" => {
                let file_name = field
                    .file_name()
                    .ok_or_else(|| Json("missing filename".to_string()))?
                    .to_string();

                let prefix = prefix.clone().unwrap_or_default();
                let full_path = format!("{}/{}", prefix, file_name);

                tracing::info!("📤 Uploading `{}`...", full_path);

                let stream = futures::stream::unfold(field, |mut f| async {
                    match f.chunk().await {
                        Ok(Some(chunk)) => Some((Ok(chunk), f)),
                        Ok(None) => None,
                        Err(e) => Some((Err(e.into()), f)),
                    }
                });

                let boxed_stream = Box::pin(stream);
                state
                    .upload_file(&full_path, boxed_stream)
                    .await
                    .map_err(|e| Json(format!("Failed to upload file: {e}")))?;

                tracing::info!("✅ Uploaded `{}`", full_path);
            }
            _ => {
                tracing::warn!("Ignoring unknown field: {}", name);
            }
        }
    }

    Ok(Json("Upload successful!".to_string()))
}

/// Query parameters for the file download endpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct DownloadQuery {
    pub file_path: String,
}

/// Streams a data lake file to the caller as `application/octet-stream`.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "file",
    get,
    params(DownloadQuery),
    path = "/api/admin/download-file",
    responses((status = 200, description = "File downloaded successfully")),
    security(
        ("basic-auth" = []),
        ("bearer" = [])
    ))
]
pub async fn download_handler(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<DownloadQuery>,
) -> impl IntoResponse {
    tracing::info!("📥 Download request for `{}`", query.file_path);
    let file_path = query.file_path.clone();

    match state.download_file(&file_path).await {
        Ok(stream) => {
            let body_stream = stream.map(|result| {
                result.map_err(|e| {
                    tracing::error!("❌ Stream error: {}", e);
                    std::io::Error::other("Stream error")
                })
            });

            let body = ::axum::body::Body::from_stream(body_stream);

            let filename = std::path::Path::new(&file_path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("file");

            tracing::info!("✅ Downloaded `{}` as `{}`", file_path, filename);
            (
                StatusCode::OK,
                [
                    ("Content-Type", "application/octet-stream"),
                    (
                        "Content-Disposition",
                        &format!("attachment; filename=\"{filename}\""),
                    ),
                ],
                body,
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("❌ Download error: {e}");
            (
                StatusCode::NOT_FOUND,
                format!("File not found: {file_path}"),
            )
                .into_response()
        }
    }
}

/// Query parameters for the file delete endpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct DeleteQuery {
    pub file_path: String,
}

/// Deletes a data lake file at the given path.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "file",
    delete,
    params(DeleteQuery),
    path = "/api/admin/delete-file",
    responses((status = 200, description = "File deleted successfully")),
    security(
        ("basic-auth" = []),
        ("bearer" = [])
    ))
]
pub async fn delete_file(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<DeleteQuery>,
) -> impl IntoResponse {
    tracing::info!("📤 Delete request for `{}`", query.file_path);
    let file_path = query.file_path.clone();

    match state.delete_file(&file_path).await {
        Ok(_) => {
            tracing::info!("✅ Deleted `{}`", file_path);
            (StatusCode::OK, format!("File deleted: {file_path}")).into_response()
        }
        Err(e) => {
            tracing::error!("❌ Delete error: {e}");
            (
                StatusCode::NOT_FOUND,
                format!("File not found: {file_path}"),
            )
                .into_response()
        }
    }
}