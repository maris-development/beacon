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

mod tables;

#[derive(OpenApi)]
#[openapi(modifiers(&SecurityAddon))]
pub struct AdminApiDoc;

async fn basic_auth(
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = headers
        .get("Authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;

    if !auth_str.starts_with("Basic ") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let credentials = base64::decode(&auth_str[6..]).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let credentials = String::from_utf8(credentials).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let parts: Vec<&str> = credentials.split(':').collect();
    if parts.len() != 2 {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let (username, password) = (parts[0], parts[1]);

    if username == beacon_config::CONFIG.admin_username
        && password == beacon_config::CONFIG.admin_password
    {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

pub(crate) fn setup_admin_router() -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
    let (admin_router, admin_api) = OpenApiRouter::with_openapi(AdminApiDoc::openapi())
        .routes(routes!(tables::create_table))
        .routes(routes!(tables::apply_table_operation))
        .routes(routes!(tables::delete_table))
        .layer(DefaultBodyLimit::disable())
        .layer(axum::middleware::from_fn(basic_auth))
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
