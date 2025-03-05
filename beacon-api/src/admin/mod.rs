use std::sync::Arc;

use axum::Router;
use beacon_core::runtime::Runtime;
use utoipa::{
    openapi::security::{Http, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_axum::{router::OpenApiRouter, routes};

mod stats;

#[derive(OpenApi)]
#[openapi(modifiers(&SecurityAddon))]
pub struct AdminApiDoc;

pub(crate) fn setup_admin_router() -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
    let (admin_router, admin_api) =
        OpenApiRouter::with_openapi(AdminApiDoc::openapi()).split_for_parts();

    (admin_router, admin_api)
}

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "basic-auth",
                SecurityScheme::Http(Http::new(utoipa::openapi::security::HttpAuthScheme::Bearer)),
            )
        }
    }
}
