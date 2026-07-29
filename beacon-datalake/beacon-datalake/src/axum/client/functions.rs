//! Endpoints that expose the runtime's registered scalar and table functions.
//!
//! Both listings read `beacon.system.{functions,table_functions}` rather than a
//! typed runtime accessor, matching the SQL-backed pattern the rest of the HTTP
//! surface uses. The system tables carry the parameter list as a JSON string
//! column (`parameters`); the wire contract nests it as a `params` array, which
//! is what the admin UI's SQL autocomplete renders signatures from.

use std::sync::Arc;

use ::axum::{extract::State, http::StatusCode, Extension, Json};
use beacon_core::api::FunctionInfo;
use beacon_core::AuthIdentity;

use crate::datalake::{
    sql::{query_rows, str_field},
    DataLake,
};

/// Map one `beacon.system.{functions,table_functions}` row onto the API view.
///
/// A row whose `parameters` JSON does not parse yields an empty parameter list
/// instead of failing the listing — a function with undocumented parameters is
/// still worth completing on.
fn function_info(row: &serde_json::Value) -> FunctionInfo {
    FunctionInfo {
        function_name: str_field(row, "function_name").to_string(),
        description: str_field(row, "description").to_string(),
        return_type: str_field(row, "return_type").to_string(),
        params: serde_json::from_str(str_field(row, "parameters")).unwrap_or_default(),
    }
}

async fn list(
    state: &Arc<DataLake>,
    table: &str,
    identity: AuthIdentity,
) -> Result<Json<Vec<FunctionInfo>>, (StatusCode, String)> {
    let rows = query_rows(
        state,
        format!(
            "SELECT function_name, description, return_type, parameters \
             FROM beacon.system.{table} ORDER BY function_name"
        ),
        identity,
    )
    .await
    .map_err(|error| {
        tracing::error!(?error, table, "failed to enumerate functions");
        (StatusCode::BAD_REQUEST, error.to_string())
    })?;

    Ok(Json(rows.iter().map(function_info).collect()))
}

/// Returns documentation for every scalar/aggregate function registered with the runtime.
#[tracing::instrument(level = "info", skip(state, identity))]
#[utoipa::path(
    tag = "functions",
    get,
    path = "/api/functions",
    responses(
        (status = 200, description = "Available scalar/aggregate functions with documentation", body = Vec<FunctionInfo>),
        (status = 400, description = "Failed to enumerate functions"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_functions(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Result<Json<Vec<FunctionInfo>>, (StatusCode, String)> {
    list(&state, "functions", identity).await
}

/// Returns documentation for every table-valued function registered with the runtime.
#[tracing::instrument(level = "info", skip(state, identity))]
#[utoipa::path(
    tag = "functions",
    get,
    path = "/api/table-functions",
    responses(
        (status = 200, description = "Available table-valued functions with documentation", body = Vec<FunctionInfo>),
        (status = 400, description = "Failed to enumerate table functions"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_table_functions(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Result<Json<Vec<FunctionInfo>>, (StatusCode, String)> {
    list(&state, "table_functions", identity).await
}
