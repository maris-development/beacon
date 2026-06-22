//! Read-only table discovery endpoints (listing, schema, configuration).

use std::sync::Arc;

use ::axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use beacon_core::api::{SchemaFieldView, SchemaView, TableConfigView};
use beacon_core::runtime::Runtime;
use utoipa::{IntoParams, ToSchema};

/// Returns the names of all tables registered in the runtime catalog.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/tables",
    responses((status = 200, description = "List of registered table names", body = Vec<String>)),
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

/// Response entry pairing a table name with its Arrow schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub(crate) struct TableWithSchema {
    /// Registered table name.
    table_name: String,
    /// Arrow schema fields (name, data type, nullability, metadata) of the table.
    columns: Vec<SchemaFieldView>,
}

/// Returns every registered table along with its Arrow schema fields.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/tables-with-schema",
    responses((status = 200, description = "Registered tables with their Arrow schemas", body = Vec<TableWithSchema>)),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_tables_with_schema(
    State(state): State<Arc<Runtime>>,
) -> Json<Vec<TableWithSchema>> {
    let table_names = state.list_tables();
    let mut result = Vec::new();
    for table_name in table_names {
        if let Some(schema) = state.list_table_schema_view(table_name.clone()).await {
            result.push(TableWithSchema {
                table_name,
                columns: schema.fields,
            });
        }
    }

    Json(result)
}

/// Query parameters for [`list_table_schema`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableSchemaQuery {
    /// Name of the registered table to inspect.
    pub table_name: String,
}

/// Returns the Arrow schema of the named table, or 404 if the table is not registered.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/table-schema",
    params(ListTableSchemaQuery),
    responses(
        (status = 200, description = "The Arrow schema of the table", body = SchemaView),
        (status = 404, description = "Table not found"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_table_schema(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListTableSchemaQuery>,
) -> Result<Json<SchemaView>, (StatusCode, String)> {
    let result = state.list_table_schema_view(query.table_name.clone()).await;

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

/// Query parameters for [`list_table_config`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableConfigQuery {
    /// Name of the registered table whose configuration to return.
    pub table_name: String,
}

/// Returns the storage format and configuration of the named table.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/table-config",
    params(ListTableConfigQuery),
    responses(
        (status = 200, description = "The storage format and configuration of the table", body = TableConfigView),
        (status = 404, description = "Table not found"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_table_config(
    State(state): State<Arc<Runtime>>,
    Query(query): Query<ListTableConfigQuery>,
) -> Result<Json<TableConfigView>, (StatusCode, String)> {
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

/// Returns the Arrow schema of the runtime's default table.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get,
    path = "/api/default-table-schema",
    responses((status = 200, description = "The Arrow schema of the default table", body = SchemaView)),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn default_table_schema(
    State(state): State<Arc<Runtime>>,
) -> Json<SchemaView> {
    let result = state.list_default_table_schema_view().await;
    Json(result)
}

/// Returns the name of the runtime's default table.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get,
    path = "/api/default-table",
    responses((status = 200, description = "Name of the default table", body = String)),
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