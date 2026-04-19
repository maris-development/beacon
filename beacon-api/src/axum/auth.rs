//! Axum-specific authentication middleware built on the shared auth helpers.

use ::axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};

use crate::auth::verify_basic_auth_value;

/// Verifies the `Authorization` header for routes protected by admin basic auth.
pub(crate) fn verify_basic_auth_header(headers: &HeaderMap) -> Result<(), StatusCode> {
    let auth_header = headers
        .get("Authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    verify_basic_auth_value(auth_str).map_err(|_| StatusCode::UNAUTHORIZED)
}

/// Axum middleware that rejects requests without valid admin basic credentials.
pub(super) async fn basic_auth(
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    verify_basic_auth_header(&headers)?;
    Ok(next.run(request).await)
}