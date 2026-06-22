//! Admin endpoints for managing a table's downstream extensions.

use std::sync::Arc;

use ::axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use beacon_core::api::TableExtensions;
use beacon_core::runtime::Runtime;

use super::bad_request;

/// Replaces the named table's extensions document (MCP descriptor, query
/// presets). The document is validated against the table schema; an empty body
/// (`{}`) clears all extensions.
#[tracing::instrument(level = "info", skip(state, extensions))]
#[utoipa::path(
    tag = "admin",
    put,
    path = "/api/admin/table-extensions/{table_name}",
    params(("table_name" = String, Path, description = "Registered table name")),
    request_body = TableExtensions,
    responses(
        (status = 200, description = "Extensions updated"),
        (status = 400, description = "Invalid request, validation failed, or table not found")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn set_table_extensions(
    State(state): State<Arc<Runtime>>,
    Path(table_name): Path<String>,
    Json(extensions): Json<TableExtensions>,
) -> Result<(), (StatusCode, String)> {
    state
        .set_table_extensions(table_name, extensions)
        .await
        .map_err(bad_request)
}

/// Removes all extensions from the named table.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    delete,
    path = "/api/admin/table-extensions/{table_name}",
    params(("table_name" = String, Path, description = "Registered table name")),
    responses(
        (status = 200, description = "Extensions removed"),
        (status = 400, description = "Table not found or removal failed")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn delete_table_extensions(
    State(state): State<Arc<Runtime>>,
    Path(table_name): Path<String>,
) -> Result<(), (StatusCode, String)> {
    state
        .delete_table_extensions(table_name)
        .await
        .map_err(bad_request)
}
