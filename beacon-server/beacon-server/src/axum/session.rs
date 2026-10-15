//! Session sign-in: trade a username and password for a `bst_` token.
//!
//! These routes carry no `resolve_identity` middleware. A sign-in has no credentials yet, and
//! that middleware refuses such a request when anonymous access is off.

use std::sync::Arc;

use ::axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json, Router,
};
use beacon_core::beacon_auth::{failed_sign_in_delay, SessionError, SESSION_TOKEN_PREFIX};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::api::{SessionCreated, SessionRequest, SessionView};
use crate::auth::parse_bearer_token;
use crate::axum::security::SecurityAddon;
use crate::server::Server;

/// OpenAPI document marker for the session surface.
#[derive(utoipa::OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    tags((name = "auth", description = "Sign in for a session token, check the session, and sign out."))
)]
pub struct SessionApiDoc;

/// Builds the session router and its OpenAPI document.
pub(crate) fn setup_session_router() -> (Router<Arc<Server>>, utoipa::openapi::OpenApi) {
    OpenApiRouter::with_openapi(SessionApiDoc::openapi())
        .routes(routes!(create_session, session_info, delete_session))
        .split_for_parts()
}

/// The `bst_` token in the `Authorization` header.
fn session_token(headers: &HeaderMap) -> Result<&str, StatusCode> {
    let value = headers
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let token = parse_bearer_token(value).map_err(|_| StatusCode::UNAUTHORIZED)?;
    if token.starts_with(SESSION_TOKEN_PREFIX) {
        Ok(token)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Signs in with a username and password and returns a session token.
#[utoipa::path(
    tag = "auth",
    post,
    path = "/api/auth/session",
    request_body = SessionRequest,
    responses(
        (status = 201, description = "Signed in", body = SessionCreated),
        (status = 401, description = "Wrong username or password"),
        (status = 500, description = "The session could not be stored")
    ),
    security(())
)]
pub(crate) async fn create_session(
    State(state): State<Arc<Server>>,
    Json(request): Json<SessionRequest>,
) -> Result<(StatusCode, Json<SessionCreated>), StatusCode> {
    match state
        .runtime()
        .auth()
        .create_session(&request.username, &request.password, request.remember)
        .await
    {
        Ok(issued) => Ok((
            StatusCode::CREATED,
            Json(SessionCreated {
                token: issued.token,
                username: issued.identity.username,
                is_super_user: issued.identity.is_super_user,
                remember: issued.remember,
                expires_at: issued.expires_at,
            }),
        )),
        Err(SessionError::InvalidCredentials) => {
            tokio::time::sleep(failed_sign_in_delay()).await;
            Err(StatusCode::UNAUTHORIZED)
        }
        Err(SessionError::Storage(error)) => {
            tracing::error!(?error, "could not store a new session");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Returns the current session and moves its expiry forward.
#[utoipa::path(
    tag = "auth",
    get,
    path = "/api/auth/session",
    responses(
        (status = 200, description = "The session is valid", body = SessionView),
        (status = 401, description = "No valid session token")
    ),
    security(("bearer" = []))
)]
pub(crate) async fn session_info(
    State(state): State<Arc<Server>>,
    headers: HeaderMap,
) -> Result<Json<SessionView>, StatusCode> {
    let token = session_token(&headers)?;
    let info = state
        .runtime()
        .auth()
        .session_info(token)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    Ok(Json(SessionView {
        username: info.identity.username,
        is_super_user: info.identity.is_super_user,
        remember: info.remember,
        expires_at: info.expires_at,
    }))
}

/// Signs out: the token stops working.
#[utoipa::path(
    tag = "auth",
    delete,
    path = "/api/auth/session",
    responses(
        (status = 204, description = "Signed out"),
        (status = 401, description = "No valid session token")
    ),
    security(("bearer" = []))
)]
pub(crate) async fn delete_session(
    State(state): State<Arc<Server>>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let token = session_token(&headers)?;
    let auth = state.runtime().auth();
    auth.session_info(token)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    auth.end_session(token).await.map_err(|error| {
        tracing::error!(?error, "could not delete a session");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(StatusCode::NO_CONTENT)
}
