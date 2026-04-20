//! Endpoints that expose the runtime's registered scalar and table functions.

use std::sync::Arc;

use ::axum::{extract::State, Json};
use beacon_core::api::FunctionInfo;
use beacon_core::runtime::Runtime;

/// Returns documentation for every scalar/aggregate function registered with the runtime.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "functions",
    get, 
    path = "/api/functions", 
    responses((status = 200, description = "List of available functions with documentation")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_functions(State(state): State<Arc<Runtime>>) -> Json<Vec<FunctionInfo>> {
    let functions = state.list_functions();
    Json(functions)
}

/// Returns documentation for every table-valued function registered with the runtime.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "functions",
    get,
    path = "/api/table-functions",
    responses((status = 200, description = "List of available table functions with documentation")),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_table_functions(
    State(state): State<Arc<Runtime>>,
) -> Json<Vec<FunctionInfo>> {
    let functions = state.list_table_functions();
    Json(functions)
}