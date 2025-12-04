use core::panic;
use std::{sync::Arc, thread::sleep};

use arrow::ipc::{writer::IpcWriteOptions, CompressionType};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::runtime::Runtime;
use beacon_functions::function_doc::FunctionDoc;
use beacon_planner::metrics::ConsolidatedMetrics;
use beacon_query::{output::QueryOutputFile, Query};
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
    let query_result = state.run_client_query(query_obj).await.map_err(|err| {
        tracing::error!("Error running beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    match query_result.query_output {
        beacon_core::query_result::QueryOutput::File(query_output_file) => {
            Ok(handle_query_output_file(query_output_file, query_result.query_id).await)
        }
        beacon_core::query_result::QueryOutput::Stream(arrow_output_stream) => {
            let ipc_options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap();

            let schema = arrow_output_stream.schema();

            let axum_stream = axum_streams::StreamBodyAs::arrow_ipc_with_options_errors(
                schema,
                arrow_output_stream.map_err(|e| axum::Error::new(Box::new(e))),
                ipc_options,
            )
            .header(
                "x-beacon-query-id",
                HeaderValue::from_str(&query_result.query_id.to_string()).unwrap(),
            )
            .header(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str("application/vnd.apache.arrow.stream").unwrap(),
            );

            return Ok(axum_stream.into_response());
        }
    }
}

async fn handle_query_output_file(
    output_file: QueryOutputFile,
    query_id: uuid::Uuid,
) -> Response<Body> {
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

async fn file_stream_response(
    file_path: &std::path::Path,
    content_type: &str,
    file_ext: &str,
    query_id: uuid::Uuid,
) -> Response<Body> {
    let file = tokio::fs::File::open(file_path).await.unwrap();
    let stream = tokio_util::io::ReaderStream::new(file);
    let inner_stream = Body::from_stream(stream);
    (
        [
            (header::CONTENT_TYPE, format!("{}", content_type).as_str()),
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
        .into_response()
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
