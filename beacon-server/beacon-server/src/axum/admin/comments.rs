//! Admin endpoints for the comments of a table.
//!
//! The handlers wrap the SQL surface: `beacon.system.comments` for the read and
//! `COMMENT ON` for each write, so authorization stays in one place.

use std::sync::Arc;

use ::axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use beacon_core::comments::TableComments;
use beacon_core::{AuthIdentity, TableReference};
use serde_json::Value;

use crate::server::{
    catalog,
    sql::{execute, query_rows, quote_ident, quote_literal},
    Server,
};

use super::bad_request;

/// The stored comments of `table`, read from `beacon.system.comments`.
async fn stored_comments(
    state: &Arc<Server>,
    table: &str,
    identity: AuthIdentity,
) -> anyhow::Result<TableComments> {
    let rows = query_rows(
        state,
        format!(
            "SELECT column_name, comment FROM beacon.system.comments WHERE table_name = {}",
            quote_literal(table)
        ),
        identity,
    )
    .await?;
    let mut comments = TableComments::default();
    for row in rows {
        let text = row.get("comment").and_then(Value::as_str).map(String::from);
        match row.get("column_name").and_then(Value::as_str) {
            Some(column) => comments.set_column(column, text),
            None => comments.set_table(text),
        }
    }
    Ok(comments)
}

/// Replace all comments of `table` with `comments`.
///
/// Beacon checks every column before the first write, so an unknown column
/// changes nothing.
async fn replace_comments(
    state: &Arc<Server>,
    table: &str,
    comments: &TableComments,
    identity: AuthIdentity,
) -> anyhow::Result<()> {
    let schema = catalog::table_schema(state, TableReference::bare(table), identity.clone())
        .await?
        .ok_or_else(|| anyhow::anyhow!("table '{table}' not found"))?;
    comments.validate(&schema)?;
    let current = stored_comments(state, table, identity.clone()).await?;

    let quoted_table = quote_ident(table);
    let literal = |text: Option<&String>| text.map_or("NULL".to_string(), |t| quote_literal(t));
    let mut statements = vec![format!(
        "COMMENT ON TABLE {quoted_table} IS {}",
        literal(comments.table.as_ref())
    )];
    for column in current.columns.keys() {
        if !comments.columns.contains_key(column) {
            statements.push(format!(
                "COMMENT ON COLUMN {quoted_table}.{} IS NULL",
                quote_ident(column)
            ));
        }
    }
    for (column, text) in &comments.columns {
        statements.push(format!(
            "COMMENT ON COLUMN {quoted_table}.{} IS {}",
            quote_ident(column),
            literal(Some(text))
        ));
    }
    for sql in statements {
        execute(state, sql, identity.clone()).await?;
    }
    Ok(())
}

/// Returns the comments of the named table. A table without comments returns
/// an empty object.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/table-comments/{table_name}",
    params(("table_name" = String, Path, description = "Registered table name")),
    responses(
        (status = 200, description = "The table's comments", body = TableComments),
        (status = 404, description = "Table not found")
    ),
    security(("basic-auth" = []), ("bearer" = []))
)]
pub(crate) async fn get_table_comments(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(table_name): Path<String>,
) -> Result<Json<TableComments>, (StatusCode, String)> {
    let exists = catalog::table_schema(
        &state,
        TableReference::bare(table_name.as_str()),
        identity.clone(),
    )
    .await
    .map_err(bad_request)?
    .is_some();
    if !exists {
        return Err((
            StatusCode::NOT_FOUND,
            format!("Table {table_name} not found"),
        ));
    }
    stored_comments(&state, &table_name, identity)
        .await
        .map(Json)
        .map_err(bad_request)
}

/// Replaces all comments of the named table. Beacon checks each column against
/// the table schema. An empty body (`{}`) removes all comments.
#[tracing::instrument(level = "info", skip(state, comments))]
#[utoipa::path(
    tag = "admin",
    put,
    path = "/api/admin/table-comments/{table_name}",
    params(("table_name" = String, Path, description = "Registered table name")),
    request_body = TableComments,
    responses(
        (status = 200, description = "Comments replaced"),
        (status = 400, description = "Unknown column, unknown table, or write failed")
    ),
    security(("basic-auth" = []), ("bearer" = []))
)]
pub(crate) async fn set_table_comments(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(table_name): Path<String>,
    Json(comments): Json<TableComments>,
) -> Result<(), (StatusCode, String)> {
    replace_comments(&state, &table_name, &comments, identity)
        .await
        .map_err(bad_request)
}

/// Removes all comments of the named table.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    delete,
    path = "/api/admin/table-comments/{table_name}",
    params(("table_name" = String, Path, description = "Registered table name")),
    responses(
        (status = 200, description = "Comments removed"),
        (status = 400, description = "Unknown table or write failed")
    ),
    security(("basic-auth" = []), ("bearer" = []))
)]
pub(crate) async fn delete_table_comments(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(table_name): Path<String>,
) -> Result<(), (StatusCode, String)> {
    replace_comments(&state, &table_name, &TableComments::default(), identity)
        .await
        .map_err(bad_request)
}
