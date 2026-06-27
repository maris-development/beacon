//! Axum-specific authentication middleware backed by the runtime's auth context.
//!
//! Credential validation is delegated to the runtime; these helpers only parse the wire formats
//! (HTTP Basic, Bearer) into a [`Credential`] and resolve the caller's [`AuthIdentity`].

use std::sync::Arc;

use ::axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use beacon_core::{runtime::Runtime, AuthIdentity, Credential};

use crate::auth::{parse_basic_auth_credentials, parse_bearer_token};

/// Parses an `Authorization` header value into a [`Credential`].
///
/// Supports HTTP Basic (`Basic <base64>`) and bearer tokens (`Bearer <token>`, e.g. an OIDC JWT).
fn credential_from_header(value: &str) -> Result<Credential, StatusCode> {
    if value.starts_with("Basic ") {
        let (username, password) =
            parse_basic_auth_credentials(value).map_err(|_| StatusCode::UNAUTHORIZED)?;
        Ok(Credential::basic(username, password))
    } else if let Ok(token) = parse_bearer_token(value) {
        Ok(Credential::bearer(token.to_string()))
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

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
            let credential = credential_from_header(auth_str)?;
            runtime
                .authenticate(&credential)
                .await
                .map_err(|_| StatusCode::UNAUTHORIZED)?
        }
        None => runtime
            .authenticate_anonymous()
            .await
            .unwrap_or_else(|_| AuthIdentity::empty()),
    };

    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

/// Axum middleware that rejects requests without valid super-user credentials (admin routes).
///
/// The runtime is the router state, so the middleware can resolve credentials against the auth
/// context. Missing/invalid credentials → 401; a valid but non-super-user principal → 403.
pub(super) async fn basic_auth(
    State(runtime): State<Arc<Runtime>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = headers
        .get("Authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let credential = credential_from_header(auth_str)?;

    let identity = runtime
        .authenticate(&credential)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    if identity.is_super_user {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose, Engine as _};

    fn basic_header(username: &str, password: &str) -> String {
        format!(
            "Basic {}",
            general_purpose::STANDARD.encode(format!("{username}:{password}"))
        )
    }

    #[test]
    fn parses_basic_credentials() {
        let credential = credential_from_header(&basic_header("alice", "secret")).unwrap();
        match credential {
            Credential::Basic { username, password } => {
                assert_eq!(username, "alice");
                assert_eq!(password, "secret");
            }
            other => panic!("expected basic credential, got {other:?}"),
        }
    }

    #[test]
    fn parses_bearer_token() {
        let credential = credential_from_header("Bearer a.b.c").unwrap();
        match credential {
            Credential::Bearer(token) => assert_eq!(token, "a.b.c"),
            other => panic!("expected bearer credential, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_scheme_and_empty_bearer() {
        assert_eq!(
            credential_from_header("Digest xyz").unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            credential_from_header("Bearer ").unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
    }
}
