use std::sync::Arc;

use axum::{
    extract::{Multipart, Query, State}, http::StatusCode, response::IntoResponse, Json
};
use beacon_core::{runtime::Runtime};
use futures::StreamExt;
use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

/// Just a schema for axum native multipart
#[derive(Deserialize, ToSchema)]
#[allow(unused)]
struct UploadDatasetMultipart {
    #[schema(example = "base_dir/sub_dir")]
    prefix: String,
    #[schema(format = Binary, content_media_type = "application/octet-stream")]
    file: String,
}

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
        let name = field.name().unwrap_or("");

        match name {
            "prefix" => {
                // read small text field
                let value = field.text().await.unwrap_or_default();
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

                // Convert Axum field into a stream of Bytes
                let stream = futures::stream::unfold(field, |mut f| async {
                    match f.chunk().await {
                        Ok(Some(chunk)) => Some((Ok(chunk), f)),
                        Ok(None) => None,
                        Err(e) => Some((Err(e.into()), f)),
                    }
                });

                let boxed_stream = Box::pin(stream);
                println!("Full path: {}", full_path);
                // Stream into storage
                let data_lake = state.data_lake();
                data_lake
                    .upload_file(&full_path, boxed_stream)
                    .await
                    .map_err(|e| Json(format!("Failed to upload file: {e}")))?;

                tracing::info!("✅ Uploaded `{}`", full_path);
            }

            _ => {
                // Ignore unknown fields
            }
        }
    }

    Ok(Json("Upload successful!".to_string()))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct DownloadQuery {
    pub file_name: String,
}

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
    tracing::error!("📥 Download request for `{}`", query.file_name);
    let file_name = query.file_name.clone();

    match state.data_lake().download_file(&file_name).await {
        Ok(stream) => {
            // Convert object_store stream into Axum-compatible stream
            let body_stream = stream.map(|result| {
                result.map_err(|e| {
                    tracing::error!("❌ Stream error: {}", e);
                    std::io::Error::other("Stream error")
                })
            });

            let body = axum::body::Body::from_stream(body_stream);

            (
                StatusCode::OK,
                [
                    ("Content-Type", "application/octet-stream"),
                    ("Content-Disposition", &format!("attachment; filename=\"{file_name}\"")),
                ],
                body,
            )
                .into_response()
        }
        Err(e) => {
            eprintln!("❌ Download error: {e}");
            (StatusCode::NOT_FOUND, format!("File not found: {file_name}")).into_response()
        }
    }
}