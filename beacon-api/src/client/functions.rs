use std::sync::Arc;

use arrow_schema::{DataType, Field};
use axum::{extract::State, Json};
use beacon_core::runtime::Runtime;
use beacon_functions::function_doc::FunctionDoc;

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/functions", 
    responses((status = 200, description = "List of available functions")),
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