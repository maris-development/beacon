//! Exposes Beacon runtime system information (version, host resources, build metadata).

use std::sync::Arc;

use ::axum::{extract::State, Json};
use crate::datalake::DataLake;
use beacon_core::{sys::SystemInfo};

/// Returns Beacon runtime system information (version, host, resource totals).
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "system",
    get, 
    path = "/api/info",
    responses((status = 200, description = "Beacon runtime system information", body = SystemInfo)),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn system_info(State(state): State<Arc<DataLake>>) -> Json<SystemInfo> {
    let info = state.system_info();
    Json(info)
}