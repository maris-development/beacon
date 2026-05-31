//! Axum-specific authentication middleware backed by the runtime's auth provider.

use std::sync::Arc;

use ::axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use beacon_core::{runtime::Runtime, AuthIdentity};

use crate::auth::parse_basic_auth_credentials;

/// Resolves the caller's [`AuthIdentity`] for client routes and stores it in request extensions.
///
/// Credentials present → authenticate (401 on invalid). No credentials → the anonymous user, or an
/// empty (role-less) identity when anonymous access is disabled. Missing credentials never hard-fail
/// here; query-time enforcement (when enabled) is what rejects unauthorized access.
pub(super) async fn resolve_identity(
    State(runtime): State<Arc<Runtime>>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let identity = match headers.get("Authorization") {
        Some(value) => {
            let auth_str = value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
            let (username, password) =
                parse_basic_auth_credentials(auth_str).map_err(|_| StatusCode::UNAUTHORIZED)?;
            runtime
                .authenticate(&format!("{username}:{password}"))
                .await
                .map_err(|_| StatusCode::UNAUTHORIZED)?
        }
        None => runtime
            .authenticate_anonymous()
            .await
            .unwrap_or_else(|_| AuthIdentity {
                username: String::new(),
                roles: Vec::new(),
                is_super_user: false,
            }),
    };

    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

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
