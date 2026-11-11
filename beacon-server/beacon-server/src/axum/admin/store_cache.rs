//! Admin endpoints for the datasets store cache (status, change events, sync).

use std::sync::Arc;

use ::axum::{extract::State, http::StatusCode, Json};
use beacon_object_store_cache::{ApplyReport, CacheHandle, CacheStatus, StoreEvent, SyncReport};

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

/// Tells the store cache that objects changed in the datasets store.
///
/// Send this after a different program wrote, replaced or removed files. The
/// cache reads the store for each event, so a repeated or late event is safe.
/// Each event is `{"type":"changed","path":"a/b.nc"}`,
/// `{"type":"prefix_changed","path":"a"}` or `{"type":"resync"}`.
#[tracing::instrument(level = "info", skip(state, events))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/store-cache/events",
    request_body(content = Vec<Object>, description = "Change events"),
    responses(
        (status = 200, description = "Number of events applied, ignored and failed", body = Object),
        (status = 409, description = "The store cache is off")
    ),
    security(("basic-auth" = []), ("bearer" = []))
)]
pub(crate) async fn store_cache_events(
    State(state): State<Arc<Server>>,
    Json(events): Json<Vec<StoreEvent>>,
) -> Result<Json<ApplyReport>, (StatusCode, String)> {
    Ok(Json(handle(&state)?.apply(events).await))
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
