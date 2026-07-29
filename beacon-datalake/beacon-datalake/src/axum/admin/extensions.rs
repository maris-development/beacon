//! Admin endpoints for managing a table's downstream extensions.

use std::sync::Arc;

use ::axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use beacon_core::extensions::TableExtensions;
use beacon_core::AuthIdentity;
use crate::datalake::{
    sql::{execute, quote_ident, quote_literal},
    DataLake,
};

use super::bad_request;

/// Apply an extensions document with `SET EXTENSION` / `DROP EXTENSION`, the SQL
/// surface these endpoints wrap. A field left `None` is dropped, so a `{}` body
/// clears everything — matching the previous behaviour of replacing the whole
/// document.
async fn apply_extensions(
    state: &Arc<DataLake>,
    table: &str,
    extensions: &TableExtensions,
    identity: AuthIdentity,
) -> anyhow::Result<()> {
    let table = quote_ident(table);
    for (name, value) in [
        ("mcp", extensions.mcp.as_ref().map(serde_json::to_string)),
        ("preset", extensions.preset.as_ref().map(serde_json::to_string)),
    ] {
        let sql = match value {
            Some(document) => format!(
                "SET EXTENSION '{name}' FOR {table} TO {}",
                quote_literal(&document?)
            ),
            None => format!("DROP EXTENSION '{name}' FOR {table}"),
        };
        // Dropping an extension that was never set is not an error for the
        // caller: the end state is what was asked for either way.
        let _ = execute(state, sql, identity.clone()).await;
    }
    Ok(())
}

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
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(table_name): Path<String>,
    Json(extensions): Json<TableExtensions>,
) -> Result<(), (StatusCode, String)> {
    apply_extensions(&state, &table_name, &extensions, identity)
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
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(table_name): Path<String>,
) -> Result<(), (StatusCode, String)> {
    apply_extensions(
        &state,
        &table_name,
        &TableExtensions::default(),
        identity,
    )
    .await
    .map_err(bad_request)
}
