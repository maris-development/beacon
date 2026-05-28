//! Axum-specific authentication middleware backed by the runtime's auth provider.

use std::sync::Arc;

use ::axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use beacon_core::runtime::Runtime;

use crate::auth::parse_basic_auth_credentials;

/// Axum middleware that rejects requests without valid super-user basic credentials.
pub(super) async fn basic_auth(
    State(runtime): State<Arc<Runtime>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    authorize_admin(&runtime, &headers).await?;
    Ok(next.run(request).await)
}

/// Authenticates the `Authorization` header and requires the principal to be a super-user.
async fn authorize_admin(runtime: &Runtime, headers: &HeaderMap) -> Result<(), StatusCode> {
    let auth_header = headers
        .get("Authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let (username, password) =
        parse_basic_auth_credentials(auth_str).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let identity = runtime
        .authenticate(&format!("{username}:{password}"))
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    if identity.is_super_user {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}
