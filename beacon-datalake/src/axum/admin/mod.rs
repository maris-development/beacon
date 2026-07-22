//! Authenticated administrative HTTP endpoints.

use std::sync::Arc;

use ::axum::{http::StatusCode, Router};
use crate::datalake::DataLake;
use utoipa::{
    openapi::security::{Http, HttpAuthScheme, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_axum::{router::OpenApiRouter, routes};

mod auth;
mod check;
mod crawlers;
mod datasets;
mod extensions;
mod external_tables;
mod tables;

/// OpenAPI document marker for the admin surface.
#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    tags(
        (name = "admin", description = "Authenticated administrative endpoints (HTTP Basic auth) for managing crawlers and external tables, and inspecting table configuration.")
    )
)]
pub struct AdminApiDoc;

/// Builds the admin router and its OpenAPI document.
///
/// The super-user `basic_auth` middleware is attached by `setup_router`, where the runtime is
/// available as middleware state.
pub(crate) fn setup_admin_router() -> (Router<Arc<DataLake>>, utoipa::openapi::OpenApi) {
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
        .routes(routes!(tables::list_table_config))
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

/// Injects the auth scheme used by admin endpoints into the OpenAPI document.
///
/// The admin surface is gated solely by the `basic_auth` middleware, which only
/// accepts HTTP Basic credentials — so only the `basic-auth` scheme is advertised.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "basic-auth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Basic)),
            );
        }
    }
}
