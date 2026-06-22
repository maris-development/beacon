//! Admin endpoints for managing crawlers (create, list, get, run, drop).

use std::sync::Arc;

use ::axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use beacon_core::api::{CrawlReportView, CrawlerView, CreateCrawlerRequest};
use beacon_core::runtime::Runtime;

use super::bad_request;

/// Defines (or replaces) a crawler and starts its triggers.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/crawlers",
    request_body = CreateCrawlerRequest,
    responses(
        (status = 200, description = "Crawler created"),
        (status = 400, description = "Invalid crawler definition")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn create_crawler(
    State(state): State<Arc<Runtime>>,
    Json(req): Json<CreateCrawlerRequest>,
) -> Result<(), (StatusCode, String)> {
    state.create_crawler(req).await.map_err(bad_request)
}

/// Lists all defined crawlers.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/crawlers",
    responses((status = 200, description = "List of crawlers", body = [CrawlerView])),
    security(("basic-auth" = []))
)]
pub(crate) async fn list_crawlers(State(state): State<Arc<Runtime>>) -> Json<Vec<CrawlerView>> {
    Json(state.list_crawlers())
}

/// Returns a single crawler by name, or 404 if it is not defined.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/crawlers/{name}",
    params(("name" = String, Path, description = "Crawler name")),
    responses(
        (status = 200, description = "Crawler definition", body = CrawlerView),
        (status = 404, description = "Crawler not found")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn get_crawler(
    State(state): State<Arc<Runtime>>,
    Path(name): Path<String>,
) -> Result<Json<CrawlerView>, (StatusCode, String)> {
    match state.get_crawler(&name) {
        Some(crawler) => Ok(Json(crawler)),
        None => Err((StatusCode::NOT_FOUND, format!("crawler '{name}' not found"))),
    }
}

/// Runs a crawler once on demand and returns its report.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/crawlers/{name}/run",
    params(("name" = String, Path, description = "Crawler name")),
    responses(
        (status = 200, description = "Crawl report", body = CrawlReportView),
        (status = 400, description = "Crawler does not exist or failed to run")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn run_crawler(
    State(state): State<Arc<Runtime>>,
    Path(name): Path<String>,
) -> Result<Json<CrawlReportView>, (StatusCode, String)> {
    state
        .run_crawler(&name)
        .await
        .map(Json)
        .map_err(bad_request)
}

/// Removes a crawler definition and stops its triggers. Crawled tables are left in place.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    delete,
    path = "/api/admin/crawlers/{name}",
    params(("name" = String, Path, description = "Crawler name")),
    responses(
        (status = 200, description = "Crawler dropped"),
        (status = 400, description = "Crawler does not exist")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn drop_crawler(
    State(state): State<Arc<Runtime>>,
    Path(name): Path<String>,
) -> Result<(), (StatusCode, String)> {
    state.drop_crawler(&name).await.map_err(bad_request)
}
