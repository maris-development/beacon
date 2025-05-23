use std::sync::Arc;

use arrow_schema::SchemaRef;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use beacon_core::runtime::Runtime;
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListDatasetsQuery {
    pub pattern: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "datasets",
    get, 
    path = "/api/datasets", 
    params(ListDatasetsQuery),
    responses((status = 200, description = "List of datasets")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_datasets(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListDatasetsQuery>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let result = state
        .list_datasets(query.pattern, query.offset, query.limit)
        .await;

    match result {
        Ok(datasets) => Ok(Json(datasets)),
        Err(err) => {
            tracing::error!("Error listing datasets: {:?}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error listing datasets".to_string(),
            ))
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListDatasetSchemaQuery {
    pub file: String,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "datasets",
    get, 
    path = "/api/dataset-schema", 
    params(ListDatasetSchemaQuery),
    responses((status = 200, description = "List the schema/available columns")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_dataset_schema(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListDatasetSchemaQuery>,
) -> Result<Json<SchemaRef>, (StatusCode, String)> {
    let result = state.list_dataset_schema(query.file.clone()).await;

    match result {
        Ok(schema) => Ok(Json(schema)),
        Err(err) => {
            tracing::error!("Error listing dataset schema: {:?}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error listing dataset schema".to_string(),
            ))
        }
    }
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "datasets",
    get, 
    path = "/api/total-datasets", 
    responses((status = 200, description = "List the total amount of datasets available")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn total_datasets(
    State(state): State<Arc<Runtime>>,
) -> Result<Json<usize>, (StatusCode, String)> {
    let result = state.total_datasets().await;

    match result {
        Ok(total_datasets) => Ok(Json(total_datasets)),
        Err(err) => {
            tracing::error!("Error reading total datasets: {:?}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error reading total datasets".to_string(),
            ))
        }
    }
}
