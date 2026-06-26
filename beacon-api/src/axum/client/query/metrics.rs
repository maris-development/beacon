//! Recorded query metrics lookup.

use ::axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use beacon_core::{api::QueryMetricsView, runtime::Runtime};
use std::sync::Arc;

use super::parse_query_id;

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
    let query_id = parse_query_id(&query_id)?;

    let metrics = state.get_query_metrics(query_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json("Query ID not found".to_string()),
        )
    })?;

    Ok(Json(metrics))
}
