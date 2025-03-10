use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use beacon_core::runtime::Runtime;
use utoipa::{IntoParams, ToSchema};

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(get, path = "/api/tables", responses((status = 200, description = "List of tables")))]
pub(crate) async fn list_tables(State(state): State<Arc<Runtime>>) -> Json<Vec<String>> {
    let result = state.list_tables();
    Json(result)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableSchemaQuery {
    pub table_name: String,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(get, path = "/api/table-schema", params(ListTableSchemaQuery) ,responses((status = 200, description = "List of schema of a table")))]
pub(crate) async fn list_table_schema(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListTableSchemaQuery>,
) -> Result<Json<SchemaRef>, (StatusCode, String)> {
    let result = state.list_table_schema(query.table_name.clone()).await;

    match result {
        Some(schema) => Ok(Json(schema)),
        None => {
            tracing::error!("Error listing table schema: table not found");
            Err((
                StatusCode::NOT_FOUND,
                format!("Table {} not found", query.table_name),
            ))
        }
    }
}
