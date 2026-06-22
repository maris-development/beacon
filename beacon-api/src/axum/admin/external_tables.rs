//! Admin endpoint for creating external tables from structured fields.

use std::sync::Arc;

use ::axum::{extract::State, http::StatusCode, Json};
use beacon_core::api::CreateExternalTableRequest;
use beacon_core::runtime::Runtime;

use super::bad_request;

/// Creates an external table over files in the datasets store. The runtime
/// assembles and runs the equivalent `CREATE EXTERNAL TABLE` statement, so all
/// `STORED AS` variants and catalog persistence behave exactly as the SQL form.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/external-tables",
    request_body = CreateExternalTableRequest,
    responses(
        (status = 200, description = "External table created"),
        (status = 400, description = "Invalid request or registration failed")
    ),
    security(
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn create_external_table(
    State(state): State<Arc<Runtime>>,
    Json(req): Json<CreateExternalTableRequest>,
) -> Result<(), (StatusCode, String)> {
    state.create_external_table(req).await.map_err(bad_request)
}
