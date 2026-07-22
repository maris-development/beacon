//! Read-only table discovery endpoints (listing, schema, configuration).

use std::sync::Arc;

use ::axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use beacon_core::api::{SchemaFieldView, SchemaView, TableExtensions};
use crate::datalake::DataLake;
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
pub(crate) async fn list_tables(State(state): State<Arc<DataLake>>) -> Json<Vec<String>> {
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
    State(state): State<Arc<DataLake>>,
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
    State(state): State<Arc<DataLake>>,
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

/// Query parameters for [`list_table_extensions`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableExtensionsQuery {
    /// Name of the registered table whose extensions to return.
    pub table_name: String,
}

/// Returns the downstream extensions (MCP descriptor, query presets) attached to
/// the named table, or 404 if the table is not registered. A table with no
/// extensions returns an empty object.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get,
    path = "/api/table-extensions",
    params(ListTableExtensionsQuery),
    responses(
        (status = 200, description = "The table's extensions", body = TableExtensions),
        (status = 404, description = "Table not found"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_table_extensions(
    State(state): State<Arc<DataLake>>,
    Query(query): Query<ListTableExtensionsQuery>,
) -> Result<Json<TableExtensions>, (StatusCode, String)> {
    match state.get_table_extensions(query.table_name.clone()).await {
        Ok(extensions) => Ok(Json(extensions)),
        Err(error) => {
            tracing::error!(?error, "error listing table extensions");
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
    State(state): State<Arc<DataLake>>,
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
pub(crate) async fn default_table(State(state): State<Arc<DataLake>>) -> Json<String> {
    let result = state.default_table();
    Json(result)
}