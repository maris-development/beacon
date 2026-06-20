//! Axum-specific authentication middleware built on the shared auth helpers.

use std::sync::Arc;

use ::axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
    Extension,
};

use crate::auth::verify_basic_auth_value;

/// Verifies the `Authorization` header for routes protected by admin basic auth.
pub(crate) fn verify_basic_auth_header(
    headers: &HeaderMap,
    admin: &beacon_config::AdminConfig,
) -> Result<(), StatusCode> {
    let auth_header = headers
        .get("Authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    verify_basic_auth_value(auth_str, admin).map_err(|_| StatusCode::UNAUTHORIZED)
}

/// Axum middleware that rejects requests without valid admin basic credentials.
///
/// The runtime config is injected as an `Extension` layer in `setup_router`.
pub(super) async fn basic_auth(
    Extension(config): Extension<Arc<beacon_config::Config>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    verify_basic_auth_header(&headers, &config.admin)?;
    Ok(next.run(request).await)
}