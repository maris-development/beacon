use core::panic;
use std::{sync::Arc, thread::sleep};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderName, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::runtime::Runtime;
use beacon_functions::function_doc::FunctionDoc;
use beacon_planner::metrics::ConsolidatedMetrics;
use beacon_query::{output::QueryOutputFile, Query};
use either::Either;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Args {
    inner: String,
    output: String,
}

#[tracing::instrument(level = "info", skip(state))]
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
    Json(query_obj): Json<Query>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let result = state.run_client_query(query_obj).await;

    let query_result = if let Err(err) = &result {
        tracing::error!("Error running beacon query: {}", err);
        return Err((StatusCode::BAD_REQUEST, Json(err.to_string())));
    } else {
        result.unwrap()
    };

    match query_result.output_buffer {
        Either::Left(output_file) => match output_file {
            QueryOutputFile::Csv(temp_file) => {
                handle_output_file_stream(
                    temp_file.path(),
                    "text/csv",
                    "csv",
                    &query_result.query_id,
                )
                .await
            }
            QueryOutputFile::Parquet(temp_file) => {
                handle_output_file_stream(
                    temp_file.path(),
                    "application/vnd.apache.parquet",
                    "parquet",
                    &query_result.query_id,
                )
                .await
            }
            QueryOutputFile::Ipc(temp_file) => {
                handle_output_file_stream(
                    temp_file.path(),
                    "application/vnd.apache.arrow.file",
                    "arrow",
                    &query_result.query_id,
                )
                .await
            }
            QueryOutputFile::Json(temp_file) => {
                handle_output_file_stream(
                    temp_file.path(),
                    "application/json",
                    "json",
                    &query_result.query_id,
                )
                .await
            }
            QueryOutputFile::Odv(temp_file) => {
                handle_output_file_stream(
                    temp_file.path(),
                    "application/zip",
                    "zip",
                    &query_result.query_id,
                )
                .await
            }
            QueryOutputFile::NetCDF(temp_file) => {
                handle_output_file_stream(
                    temp_file.path(),
                    "application/netcdf",
                    "nc",
                    &query_result.query_id,
                )
                .await
            }
            QueryOutputFile::GeoParquet(temp_file) => {
                handle_output_file_stream(
                    temp_file.path(),
                    "application/vnd.apache.arrow.geo+parquet",
                    "geoparquet",
                    &query_result.query_id,
                )
                .await
            }
        },
        Either::Right(ipc_stream) => {
            // Create axum stream from ipc_stream
            let axum_stream = axum_streams::StreamBodyAs::arrow_ipc_with_errors(
                ipc_stream.schema(),
                ipc_stream.map_err(|e| axum::Error::new(Box::new(e))),
            );
            return Ok(axum_stream.into_response());
        }
    }
}

async fn handle_output_file_stream(
    file_path: &std::path::Path,
    content_type: &str,
    file_ext: &str,
    query_id: &uuid::Uuid,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let file = tokio::fs::File::open(file_path).await.map_err(|err| {
        tracing::error!("Error opening output file: {}", err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json("Error opening output file".to_string()),
        )
    })?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let inner_stream = Body::from_stream(stream);
    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (
                header::CONTENT_DISPOSITION,
                &format!("attachment; filename=\"output.{}\"", file_ext),
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
pub(crate) async fn parse_query(Json(query_obj): Json<Query>) -> StatusCode {
    StatusCode::OK
}

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
) -> Result<Json<ConsolidatedMetrics>, (StatusCode, Json<String>)> {
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
    Json(query_obj): Json<Query>,
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
            .list_default_table_schema()
            .await
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect(),
    )
}
