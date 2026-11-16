//! Admin endpoints for the datasets store cache (status, sync).

use std::sync::Arc;

use ::axum::{extract::State, http::StatusCode, Json};
use beacon_object_store_cache::{CacheHandle, CacheStatus, SyncReport};

use crate::server::Server;

/// The cache handle, or `409 Conflict` when the cache is off.
fn handle(state: &Server) -> Result<&CacheHandle, (StatusCode, String)> {
    state.store_cache().ok_or((
        StatusCode::CONFLICT,
        "the store cache is off; set BEACON_STORE_CACHE_ENABLE=true".to_string(),
    ))
}

/// Reports the size and the counters of the datasets store cache.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/store-cache",
    responses(
        (status = 200, description = "Size and counters of the store cache", body = Object),
        (status = 409, description = "The store cache is off")
    ),
    security(("basic-auth" = []), ("bearer" = []))
)]
pub(crate) async fn store_cache_status(
    State(state): State<Arc<Server>>,
) -> Result<Json<CacheStatus>, (StatusCode, String)> {
    Ok(Json(handle(&state)?.status()))
}

/// Compares the full store cache with the datasets store now.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/store-cache/sync",
    responses(
        (status = 200, description = "Result of the sync", body = Object),
        (status = 409, description = "The store cache is off")
    ),
    security(("basic-auth" = []), ("bearer" = []))
)]
pub(crate) async fn store_cache_sync(
    State(state): State<Arc<Server>>,
) -> Result<Json<SyncReport>, (StatusCode, String)> {
    Ok(Json(handle(&state)?.sync_now().await))
}
