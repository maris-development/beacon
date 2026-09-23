//! Admin endpoints for inspecting a registered table: the statement that created
//! it, and the retired configuration endpoint.

use std::sync::Arc;

use ::axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use beacon_core::AuthIdentity;
use utoipa::{IntoParams, ToSchema};

use crate::api::DeprecationNotice;
use crate::server::{
    sql::{query_rows, quote_ident},
    Server,
};

/// Query parameters for [`get_table_definition`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct TableDefinitionQuery {
    /// Name of the registered table or view.
    pub table_name: String,
    /// Catalog the table lives in. Only used together with `schema`.
    pub catalog: Option<String>,
    /// Schema the table lives in. Defaults to the session's default schema.
    pub schema: Option<String>,
}

/// The statement that created a table, as `SHOW CREATE TABLE` returns it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct TableDefinition {
    /// Catalog the table lives in.
    pub table_catalog: String,
    /// Schema the table lives in.
    pub table_schema: String,
    /// Name of the table.
    pub table_name: String,
    /// The `CREATE` statement, with secret option values masked. Null when
    /// Beacon stores no statement for the table.
    pub definition: Option<String>,
}

/// `SHOW CREATE TABLE` for the named table, with each name part quoted.
fn show_create_table_sql(query: &TableDefinitionQuery) -> String {
    let mut parts = Vec::new();
    if let Some(schema) = &query.schema {
        if let Some(catalog) = &query.catalog {
            parts.push(quote_ident(catalog));
        }
        parts.push(quote_ident(schema));
    }
    parts.push(quote_ident(&query.table_name));
    format!("SHOW CREATE TABLE {}", parts.join("."))
}

/// Returns the statement that created the named table or view.
///
/// Beacon stores the statement when the table is created. Secret option values,
/// such as passwords and access keys, are masked before they are stored. The
/// definition is null for a table that has no statement, for example a table
/// that a crawler made.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/table-definition",
    params(TableDefinitionQuery),
    responses(
        (status = 200, description = "The statement that created the table", body = TableDefinition),
        (status = 404, description = "Table not found"),
    ),
    security(("basic-auth" = []), ("bearer" = []))
)]
pub(crate) async fn get_table_definition(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Query(query): Query<TableDefinitionQuery>,
) -> Result<Json<TableDefinition>, (StatusCode, String)> {
    let not_found = || {
        (
            StatusCode::NOT_FOUND,
            format!("Table {} not found", query.table_name),
        )
    };
    let rows = query_rows(&state, show_create_table_sql(&query), identity)
        .await
        .map_err(|error| {
            tracing::warn!(?error, "table definition lookup failed");
            not_found()
        })?;
    let row = rows.into_iter().next().ok_or_else(not_found)?;
    serde_json::from_value(row).map(Json).map_err(|error| {
        tracing::error!(?error, "SHOW CREATE TABLE returned an unexpected row");
        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    })
}

/// Query parameters for [`list_table_config`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableConfigQuery {
    /// Name of the table. Accepted, but no longer used.
    pub table_name: String,
}

/// Deprecated: always returns a notice that table configuration is no longer
/// served over HTTP.
///
/// A table's persisted definition is engine bookkeeping — the document beacon
/// writes to describe how it rebuilds the table, credentials and internal option
/// keys included — not an API contract, so the runtime no longer hands it out.
/// Kept routed (rather than removed) so an existing client gets an explanatory
/// answer instead of a 404, and still admin-only: whatever this endpoint says, it
/// says only to an administrator.
#[tracing::instrument(level = "info")]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/table-config",
    params(ListTableConfigQuery),
    responses(
        (status = 200, description = "A notice that this endpoint is no longer supported", body = DeprecationNotice),
    ),
    security(("basic-auth" = []), ("bearer" = []))
)]
// utoipa reads the `#[deprecated]` below and marks the operation deprecated in
// the OpenAPI document.
#[deprecated = "table configuration is no longer served over HTTP"]
pub(crate) async fn list_table_config(
    Query(_query): Query<ListTableConfigQuery>,
) -> Json<DeprecationNotice> {
    Json(DeprecationNotice::new(
        "Table configuration is no longer supported. A table's definition is \
         engine bookkeeping rather than an API contract; use SQL to inspect a \
         table (its schema through GET /api/table-schema, its extensions through \
         SHOW EXTENSIONS FOR <table>, its create statement through \
         GET /api/admin/table-definition).",
    ))
}
