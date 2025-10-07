use std::sync::Arc;

use axum::{
    extract::{Multipart, Path, Query, State}, http::StatusCode, response::IntoResponse, Json
};
use beacon_core::{runtime::Runtime};
use futures::StreamExt;
use utoipa::{IntoParams, ToSchema};

use crate::admin::file;

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "file",
    post, 
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
    while let Some(field) = multipart.next_field().await.map_err(|e| e.to_string())? {
        let file_name = field
            .file_name()
            .ok_or(Json("missing filename".to_string()))?
            .to_string();

        tracing::info!("📤 Uploading `{}` to object store...", file_name);

        // Convert Axum field into a stream of Bytes
        let stream = futures::stream::unfold(field, |mut f| async {
            match f.chunk().await {
                Ok(Some(chunk)) => {
                    tracing::debug!("Read {} bytes for file `{}`", chunk.len(), file_name);
                    Some((Ok::<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>(chunk), f))
                },
                Ok(None) => None,
                Err(e) => Some((Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>), f)),
            }
        });
        let boxed_stream = Box::pin(stream);

        // Stream upload into the data lake
        let data_lake = state.data_lake();
        match data_lake.upload_file(&file_name, boxed_stream).await {
            Ok(_) => (),
            Err(e) => {
                tracing::error!("❌ Failed to upload `{}`: {}", file_name, e);
                return Err(Json(format!("Failed to upload file: {}", e)));
            }
        }

        tracing::info!("✅ Uploaded `{}`", file_name);
    }

    Ok(Json("Upload successful!".to_string()))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct DownloadQuery {
    pub file_name: String,
}

async fn download_handler(
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