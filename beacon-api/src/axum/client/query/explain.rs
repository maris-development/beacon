//! Plan explanation endpoints.
//!
//! `POST /api/explain-query` returns the logical plan as JSON without executing
//! the query; `POST /api/explain-analyze-query` executes it and returns the
//! physical plan annotated with per-node runtime metrics (pgjson).

use ::axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::runtime::Runtime;
use beacon_core::{api::QueryRequest, query::Query};
use std::sync::Arc;

use super::{ensure_sql_allowed, invalid_credentials, resolve_super_user};

/// Returns a JSON-encoded explanation of the plan the runtime would produce for
/// the supplied query without executing it.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/explain-query",
    request_body = Query,
    responses(
        (
            status = 200,
            description = "JSON explanation of the plan the runtime would produce \
                for the query (the query is not executed)",
            content_type = "application/json"
        ),
        (status = 400, description = "Invalid or unsupported query"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn explain_query(
    State(state): State<Arc<Runtime>>,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let result = state.explain_client_query(query_obj).await;
    match result {
        Ok(explanation) => Ok((
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CONTENT_DISPOSITION, "attachment"),
            ],
            Body::from(explanation),
        )
            .into_response()),
        Err(err) => {
            tracing::error!("Error explaining beacon query: {}", err);
            Err((StatusCode::BAD_REQUEST, Json(err.to_string())))
        }
    }
}

/// Runs the supplied query and returns its physical plan annotated with per-node
/// runtime metrics as PostgreSQL-style JSON (pgjson) — the `EXPLAIN ANALYZE`
/// analog of `/api/explain-query`. Unlike that endpoint, the query is executed.
#[tracing::instrument(level = "info", skip(state, headers))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/explain-analyze-query",
    request_body = Query,
    responses(
        (
            status = 200,
            description = "pgjson explanation of the query's physical plan annotated \
                with per-node runtime metrics (the query IS executed to collect them)",
            content_type = "application/json"
        ),
        (status = 400, description = "Invalid or unsupported query"),
        (status = 401, description = "Basic credentials were supplied but are invalid"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn explain_analyze_query(
    State(state): State<Arc<Runtime>>,
    headers: HeaderMap,
    Json(query_obj): Json<QueryRequest>,
) -> Result<Response<Body>, (StatusCode, Json<String>)> {
    let query = query_obj.into_query().map_err(|err| {
        tracing::error!("Error parsing beacon query: {}", err);
        (StatusCode::BAD_REQUEST, Json(err.to_string()))
    })?;

    // EXPLAIN ANALYZE executes the query, so it is gated by `sql.enable` exactly
    // like `/api/query`. Without this, SQL could be run through this endpoint
    // while SQL is disabled, bypassing the restriction.
    ensure_sql_allowed(&query, &state)?;

    // It also resolves admin vs anonymous the same way `/api/query` does:
    // anonymous is read-only, valid admin basic auth elevates to super-user
    // (allowing DDL/DML, with the same side effects).
    let is_super_user =
        resolve_super_user(&headers, &state.config().admin).map_err(invalid_credentials)?;
    let result = state
        .explain_analyze_client_query(query, is_super_user)
        .await;
    match result {
        Ok(explanation) => Ok((
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CONTENT_DISPOSITION, "attachment"),
            ],
            Body::from(explanation),
        )
            .into_response()),
        Err(err) => {
            tracing::error!("Error explain-analyzing beacon query: {}", err);
            Err((StatusCode::BAD_REQUEST, Json(err.to_string())))
        }
    }
}
