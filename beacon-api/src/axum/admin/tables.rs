//! Admin endpoint for inspecting a registered table's configuration.

use std::sync::Arc;

use ::axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use beacon_core::api::TableConfigView;
use beacon_core::runtime::Runtime;
use utoipa::{IntoParams, ToSchema};

/// Query parameters for [`list_table_config`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableConfigQuery {
    /// Name of the registered table whose configuration to return.
    pub table_name: String,
}

/// Returns the storage format and configuration of the named table.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/table-config",
    params(ListTableConfigQuery),
    responses(
        (status = 200, description = "The storage format and configuration of the table", body = TableConfigView),
        (status = 404, description = "Table not found"),
    ),
    security(("basic-auth" = []))
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
