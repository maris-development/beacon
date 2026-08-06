//! The retired admin endpoint for inspecting a registered table's configuration.

use ::axum::{extract::Query, Json};
use utoipa::{IntoParams, ToSchema};

use crate::api::DeprecationNotice;

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
    security(("basic-auth" = []))
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
         SHOW EXTENSIONS FOR <table>).",
    ))
}
