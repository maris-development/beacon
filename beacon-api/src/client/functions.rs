use std::sync::Arc;

use axum::{extract::State, Json};
use beacon_core::runtime::Runtime;
use beacon_functions::function_doc::FunctionDoc;

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
pub(crate) async fn list_functions(
    State(state): State<Arc<Runtime>>,
) -> Json<Vec<FunctionDoc>> {
    let functions = state.list_functions();
    Json(functions)
}