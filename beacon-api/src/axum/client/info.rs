//! Exposes Beacon runtime system information (version, host resources, build metadata).

use std::sync::Arc;

use ::axum::{extract::State, Json};
use beacon_core::{runtime::Runtime, sys::SystemInfo};

/// Returns Beacon runtime system information (version, host, resource totals).
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "system",
    get, 
    path = "/api/info", 
    responses((status = 200, description = "Returns Beacon system information")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn system_info(State(state): State<Arc<Runtime>>) -> Json<SystemInfo> {
    let info = state.system_info();
    Json(info)
}