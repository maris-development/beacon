//! The endpoint that exposes the functions registered with the runtime.
//!
//! The rows come from `Runtime::show_functions`, which reads DataFusion's own
//! function catalog (`SHOW FUNCTIONS`) as the engine — it cannot be run as the
//! caller, since that catalog lives in the super-user-only `information_schema`,
//! while this endpoint is open to every caller (including an anonymous one)
//! because a function signature documents the engine, not anyone's data. Shaping
//! those rows into the wire contract is this module's job.
//!
//! Only scalar, aggregate, and window functions are listed. DataFusion does not
//! catalog table-valued functions (`read_parquet`, `read_netcdf`, …), so there is
//! nothing to enumerate them from — `/api/table-functions` stays routed for the
//! clients that call it, but answers empty.

use std::collections::BTreeMap;
use std::sync::Arc;

use ::axum::{extract::State, http::StatusCode, Json};
use serde_json::Value;

use crate::api::{FunctionInfo, FunctionParameterInfo};
use crate::datalake::sql::rows_from_batches;
use crate::datalake::DataLake;

/// A row's string field, or `""` when it is absent or null.
fn text(row: &Value, key: &str) -> String {
    row.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// A row's array-of-strings field (`parameters`, `parameter_types`).
fn list(row: &Value, key: &str) -> Vec<String> {
    row.get(key)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .map(|value| value.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// One entry per function name, sorted.
///
/// `SHOW FUNCTIONS` returns a row per overload signature — `abs` alone has a
/// dozen — so the first row of each name wins; the rest differ only in argument
/// types. `information_schema` carries no per-argument prose, so a parameter has
/// a name and a type but no description.
fn function_listing(rows: &[Value]) -> Vec<FunctionInfo> {
    let mut by_name: BTreeMap<String, FunctionInfo> = BTreeMap::new();
    for row in rows {
        let name = text(row, "function_name");
        if name.is_empty() || by_name.contains_key(&name) {
            continue;
        }
        let types = list(row, "parameter_types");
        let params = list(row, "parameters")
            .into_iter()
            .enumerate()
            .map(|(i, parameter)| FunctionParameterInfo {
                name: parameter,
                description: String::new(),
                data_type: types.get(i).cloned().unwrap_or_default(),
            })
            .collect();
        by_name.insert(
            name.clone(),
            FunctionInfo {
                function_name: name,
                description: text(row, "description"),
                return_type: text(row, "return_type"),
                params,
            },
        );
    }
    by_name.into_values().collect()
}

/// Returns documentation for every scalar, aggregate, and window function
/// registered with the runtime.
#[tracing::instrument(level = "info", skip(state))]
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
) -> Result<Json<Vec<FunctionInfo>>, (StatusCode, String)> {
    let batches = state.runtime().show_functions().await.map_err(|error| {
        tracing::error!(?error, "failed to enumerate functions");
        (StatusCode::BAD_REQUEST, error.to_string())
    })?;
    let rows = rows_from_batches(&batches).map_err(|error| {
        tracing::error!(?error, "failed to decode the function catalog");
        (StatusCode::BAD_REQUEST, error.to_string())
    })?;
    Ok(Json(function_listing(&rows)))
}

/// Deprecated: always returns an empty list.
///
/// Table-valued functions have no catalog to enumerate — DataFusion does not
/// register UDTFs in `information_schema`, and beacon no longer keeps a list of
/// its own — so there is nothing this can report. Kept routed (rather than
/// removed) so existing clients get an empty listing instead of a 404. Every
/// table function and its signature is in the documentation's table-function
/// reference.
#[tracing::instrument(level = "info")]
#[utoipa::path(
    tag = "functions",
    get,
    path = "/api/table-functions",
    responses(
        (status = 200, description = "Always empty: table-valued functions are not catalogued", body = Vec<FunctionInfo>),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
// utoipa reads the `#[deprecated]` below and marks the operation deprecated in
// the OpenAPI document.
#[deprecated = "table-valued functions are not catalogued; see the table function reference"]
pub(crate) async fn list_table_functions() -> Json<Vec<FunctionInfo>> {
    Json(Vec::new())
}
