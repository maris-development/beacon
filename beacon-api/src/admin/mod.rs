use std::sync::Arc;

use axum::{
    extract::{DefaultBodyLimit, Request},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
    Router,
};
use beacon_core::runtime::Runtime;
use utoipa::{
    openapi::security::{Http, HttpAuthScheme, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::auth::verify_basic_auth_header;

mod check;
mod file;
mod tables;

#[derive(OpenApi)]
#[openapi(modifiers(&SecurityAddon))]
pub struct AdminApiDoc;

async fn basic_auth(
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    verify_basic_auth_header(&headers)?;
    Ok(next.run(request).await)
}

pub(crate) fn setup_admin_router() -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
    let (admin_router, admin_api) = OpenApiRouter::with_openapi(AdminApiDoc::openapi())
        .routes(routes!(tables::create_table))
        .routes(routes!(tables::apply_table_operation))
        .routes(routes!(tables::delete_table))
        .routes(routes!(file::upload_file))
        .routes(routes!(file::download_handler))
        .routes(routes!(file::delete_file))
        .routes(routes!(check::check))
        .layer(axum::middleware::from_fn(basic_auth))
        .layer(DefaultBodyLimit::disable())
        .split_for_parts();

    (admin_router, admin_api)
}

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
