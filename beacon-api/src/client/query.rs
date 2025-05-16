use core::panic;
use std::sync::Arc;

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

    match result {
        Ok(output) => match output.output_buffer {
            QueryOutputFile::Csv(named_temp_file) => {
                let file = tokio::fs::File::open(named_temp_file.path()).await.unwrap();
                let stream = tokio_util::io::ReaderStream::new(file);
                let inner_stream = Body::from_stream(stream);
                Ok((
                    [
                        (header::CONTENT_TYPE, "text/csv"),
                        (
                            header::CONTENT_DISPOSITION,
                            "attachment; filename=\"output.csv\"",
                        ),
                        (
                            HeaderName::from_static("x-beacon-query-id"),
                            output.query_id.to_string().as_str(),
                        ),
                    ],
                    inner_stream,
                )
                    .into_response())
            }
            _ => panic!("Unsupported output format"),
        },
        Err(err) => {
            tracing::error!("Error querying beacon: {}", err);
            Err((StatusCode::BAD_REQUEST, Json(err.to_string())))
        }
    }
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
    println!("Query ID: {}", query_id);
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
    path = "/api/query/functions",
    responses(
        (status=200, description="Response containing all the available functions"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_functions(State(state): State<Arc<Runtime>>) -> Json<Vec<FunctionDoc>> {
    Json(state.list_functions())
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
