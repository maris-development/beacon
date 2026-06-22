//! Dataset discovery endpoints backed by the runtime's data lake listing.

use std::sync::Arc;

use ::axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use beacon_core::api::{DatasetInfo, SchemaView};
use beacon_core::runtime::Runtime;
use utoipa::{IntoParams, ToSchema};

/// Pagination and filter parameters shared by the dataset listing endpoints.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListDatasetsQuery {
    /// Optional glob pattern to filter dataset paths (e.g. `argo/**/*.parquet`).
    pub pattern: Option<String>,
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
    /// Number of entries to skip before returning results.
    pub offset: Option<usize>,
}

/// Lists dataset file paths matching the optional glob pattern.
///
/// Deprecated in favour of [`list_datasets`], which returns full
/// [`DatasetInfo`] entries.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "datasets",
    get, 
    path = "/api/datasets", 
    params(ListDatasetsQuery),
    responses(
        (status = 200, description = "List of dataset file paths", body = Vec<String>),
        (status = 500, description = "Failed to list datasets"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
#[deprecated = "Use /api/list-datasets instead"]
pub(crate) async fn datasets(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListDatasetsQuery>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let result = state
        .list_datasets(query.pattern, query.offset, query.limit)
        .await;

    match result {
        Ok(datasets) => Ok(Json(datasets.into_iter().map(|d| d.file_path).collect())),
        Err(err) => {
            tracing::error!("Error listing datasets: {:?}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error listing datasets".to_string(),
            ))
        }
    }
}

/// Lists dataset metadata (path plus detected file format) matching the optional glob pattern.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "datasets",
    get,
    path = "/api/list-datasets",
    params(ListDatasetsQuery),
    responses(
        (status = 200, description = "List of datasets including interpreted file format", body = Vec<DatasetInfo>),
        (status = 500, description = "Failed to list datasets"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_datasets(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListDatasetsQuery>,
) -> Result<Json<Vec<DatasetInfo>>, (StatusCode, String)> {
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

/// Query parameters for [`list_dataset_schema`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListDatasetSchemaQuery {
    /// Datasets-store-relative path of the file to inspect.
    pub file: String,
}

/// Returns the Arrow schema that would be produced when reading the given dataset file.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "datasets",
    get, 
    path = "/api/dataset-schema",
    params(ListDatasetSchemaQuery),
    responses(
        (status = 200, description = "The Arrow schema produced when reading the dataset", body = SchemaView),
        (status = 500, description = "Failed to read the dataset schema"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_dataset_schema(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListDatasetSchemaQuery>,
) -> Result<Json<SchemaView>, (StatusCode, String)> {
    let result = state.list_dataset_schema_view(query.file.clone()).await;

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

/// Returns the total number of datasets known to the runtime.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "datasets",
    get,
    path = "/api/total-datasets",
    responses(
        (status = 200, description = "Total number of datasets available", body = usize),
        (status = 500, description = "Failed to count datasets"),
    ),
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