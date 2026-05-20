//! Authenticated administrative HTTP endpoints.

use std::sync::Arc;

use ::axum::{extract::DefaultBodyLimit, Router};
use beacon_core::runtime::Runtime;
use utoipa::{
    openapi::security::{Http, HttpAuthScheme, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::auth::keycloak::KcLayer;
use crate::axum::auth::basic_auth;

mod check;
mod file;

/// OpenAPI document marker for the admin surface.
#[derive(OpenApi)]
#[openapi(modifiers(&SecurityAddon))]
pub struct AdminApiDoc;

/// Builds the admin router and attaches the configured auth middleware.
///
/// When a [`KcLayer`] is provided the admin surface validates Keycloak-issued
/// Bearer JWTs (Basic auth is rejected). Otherwise the legacy
/// [`basic_auth`] middleware is used.
pub(crate) fn setup_admin_router(
    keycloak: Option<KcLayer>,
) -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
    let base = OpenApiRouter::with_openapi(AdminApiDoc::openapi())
        .routes(routes!(file::upload_file))
        .routes(routes!(file::download_handler))
        .routes(routes!(file::delete_file))
        .routes(routes!(check::check));

    let with_auth = match keycloak {
        Some(layer) => base.layer(layer),
        None => base.layer(::axum::middleware::from_fn(basic_auth)),
    };

    let (admin_router, admin_api) = with_auth
        .layer(DefaultBodyLimit::disable())
        .split_for_parts();

    (admin_router, admin_api)
}

/// Injects the auth schemes used by admin endpoints into the OpenAPI document.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "basic-auth",
                SecurityScheme::Http(Http::new(utoipa::openapi::security::HttpAuthScheme::Basic)),
            );
            components.add_security_scheme(
                "bearer",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
        }
    }
}
