use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use beacon_core::runtime::Runtime;
use beacon_data_lake::table::Table;
use utoipa::{IntoParams, ToSchema};

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/tables", 
    responses((status = 200, description = "List of tables")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_tables(State(state): State<Arc<Runtime>>) -> Json<Vec<String>> {
    let result = state.list_tables();
    Json(result)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableSchemaQuery {
    pub table_name: String,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/table-schema", 
    params(ListTableSchemaQuery) ,
    responses((status = 200, description = "List of schema of a table")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableConfigQuery {
    pub table_name: String,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/table-config", 
    params(ListTableConfigQuery) ,
    responses((status = 200, description = "List of schema of a table")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_table_config(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListTableConfigQuery>,
) -> Result<Json<Table>, (StatusCode, String)> {
    let result = state.list_table_config(query.table_name.clone()).await;

    match result {
        Some(config) => Ok(Json(config)),
        None => {
            tracing::error!("Error listing table config: table not found");
            Err((
                StatusCode::NOT_FOUND,
                format!("Table {} not found", query.table_name),
            ))
        }
    }
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/default-table-schema", 
    responses((status = 200, description = "List of schema of the default table")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn default_table_schema(State(state): State<Arc<Runtime>>) -> Json<SchemaRef> {
    let result = state.list_default_table_schema().await;
    Json(result)
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/default-table", 
    responses((status = 200, description = "Name of the default table")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn default_table(State(state): State<Arc<Runtime>>) -> Json<String> {
    let result = state.default_table();
    Json(result)
}
