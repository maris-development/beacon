//! Authenticated administrative HTTP endpoints.

use std::sync::Arc;

use ::axum::{http::StatusCode, Router};
use crate::server::Server;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

mod auth;
mod check;
mod crawlers;
mod datasets;
mod extensions;
mod external_tables;
mod tables;

use crate::axum::security::SecurityAddon;

/// OpenAPI document marker for the admin surface.
#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    tags(
        (name = "admin", description = "Authenticated administrative endpoints (HTTP Basic auth or bearer token, super-user only) for managing crawlers and external tables, and inspecting table configuration.")
    )
)]
pub struct AdminApiDoc;

/// Builds the admin router and its OpenAPI document.
///
/// The super-user `basic_auth` middleware is attached by `setup_router`, where the runtime is
/// available as middleware state.
// `table-config` is retired but still routed, so registering it means naming a
// deprecated handler on purpose.
#[allow(deprecated)]
pub(crate) fn setup_admin_router() -> (Router<Arc<Server>>, utoipa::openapi::OpenApi) {
    let (admin_router, admin_api) = OpenApiRouter::with_openapi(AdminApiDoc::openapi())
        .routes(routes!(check::check))
        .routes(routes!(crawlers::create_crawler, crawlers::list_crawlers))
        .routes(routes!(
            crawlers::get_crawler,
            crawlers::drop_crawler
        ))
        .routes(routes!(crawlers::run_crawler))
        .routes(routes!(external_tables::create_external_table))
        .routes(routes!(datasets::upload_dataset))
        .routes(routes!(datasets::download_dataset))
        .routes(routes!(datasets::delete_dataset))
        .routes(routes!(datasets::initiate_upload))
        .routes(routes!(datasets::upload_part))
        .routes(routes!(datasets::complete_upload))
        .routes(routes!(datasets::abort_upload))
        .routes(routes!(datasets::dataset_storage))
        .routes(routes!(tables::list_table_config))
        .routes(routes!(tables::get_table_definition))
        .routes(routes!(auth::list_users))
        .routes(routes!(auth::list_roles))
        .routes(routes!(
            extensions::set_table_extensions,
            extensions::delete_table_extensions
        ))
        .split_for_parts();

    (admin_router, admin_api)
}

/// Map a runtime error to a `400 Bad Request` carrying the error text, the shared
/// failure shape for the admin write endpoints.
pub(super) fn bad_request(error: anyhow::Error) -> (StatusCode, String) {
    tracing::error!(?error, "admin request failed");
    (StatusCode::BAD_REQUEST, error.to_string())
}
