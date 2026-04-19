//! Authentication and bearer-token management for the Flight SQL transport.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_flight::{BasicAuth, HandshakeRequest};
use prost::Message;
use tonic::{Request, Status};
use uuid::Uuid;

use crate::auth::{parse_bearer_token, validate_basic_auth_credentials, verify_basic_auth_value};

/// Per-request authorization context shared with the execution layer.
#[derive(Clone)]
pub(super) struct AuthContext {
    pub(super) is_super_user: bool,
}

impl AuthContext {
    fn admin() -> Self {
        Self {
            is_super_user: true,
        }
    }

    fn anonymous() -> Self {
        Self {
            is_super_user: false,
        }
    }
}

struct AuthToken {
    auth: AuthContext,
    expires_at: Instant,
}

/// Authenticates Flight SQL requests and manages short-lived bearer sessions.
#[derive(Clone)]
pub(super) struct Authenticator {
    allow_anonymous: bool,
    token_ttl: Duration,
    auth_tokens: Arc<tokio::sync::RwLock<HashMap<String, AuthToken>>>,
}

impl Authenticator {
    /// Creates a new authenticator with the configured anonymous-access policy and token TTL.
    pub(super) fn new(allow_anonymous: bool, token_ttl: Duration) -> Self {
        Self {
            allow_anonymous,
            token_ttl,
            auth_tokens: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Reads the authorization metadata value from a gRPC request if present.
    pub(super) fn authorization_value<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<String>, Status> {
        match request.metadata().get("authorization") {
            Some(value) => Ok(Some(
                value
                    .to_str()
                    .map_err(|_| Status::unauthenticated("invalid authorization metadata"))?
                    .to_string(),
            )),
            None => Ok(None),
        }
    }

    /// Resolves the request authorization metadata into an execution context.
    pub(super) async fn authorize_request<T>(
        &self,
        request: &Request<T>,
    ) -> Result<AuthContext, Status> {
        let auth_value = self.authorization_value(request)?;
        self.authorize_optional(auth_value).await
    }

    /// Authorizes an optional auth value, falling back to anonymous access when enabled.
    pub(super) async fn authorize_optional(
        &self,
        auth_value: Option<String>,
    ) -> Result<AuthContext, Status> {
        match auth_value {
            Some(auth_value) => self.authorize_value(auth_value).await,
            None if self.allow_anonymous => Ok(AuthContext::anonymous()),
            None => Err(Status::unauthenticated("missing authorization metadata")),
        }
    }

    /// Authenticates the Flight SQL handshake and decides whether the session is admin or anonymous.
    pub(super) fn authorize_handshake(
        &self,
        auth_value: Option<&str>,
        handshake: Option<&HandshakeRequest>,
    ) -> Result<AuthContext, Status> {
        if let Some(auth_value) = auth_value {
            verify_basic_auth_value(auth_value)
                .map_err(|_| Status::unauthenticated("invalid credentials"))?;
            return Ok(AuthContext::admin());
        }

        // Some Flight SQL clients send credentials in the handshake payload rather than metadata.
        if let Some(request) = handshake {
            if !request.payload.is_empty() {
                let credentials = BasicAuth::decode(request.payload.clone())
                    .map_err(|_| Status::unauthenticated("invalid credentials"))?;
                if validate_basic_auth_credentials(&credentials.username, &credentials.password) {
                    return Ok(AuthContext::admin());
                }

                return Err(Status::unauthenticated("invalid credentials"));
            }
        }

        if self.allow_anonymous {
            Ok(AuthContext::anonymous())
        } else {
            Err(Status::unauthenticated("invalid credentials"))
        }
    }

    /// Issues a short-lived bearer token that preserves the resolved authorization context.
    pub(super) async fn issue_token(&self, auth: AuthContext) -> String {
        let token = Uuid::new_v4().to_string();
        let expires_at = Instant::now() + self.token_ttl;

        self.auth_tokens
            .write()
            .await
            .insert(token.clone(), AuthToken { auth, expires_at });

        token
    }

    /// Resolves either bearer metadata or inline basic credentials into an auth context.
    async fn authorize_value(&self, auth_value: String) -> Result<AuthContext, Status> {
        if let Ok(token) = parse_bearer_token(&auth_value) {
            return self.authorize_bearer(token).await;
        }

        verify_basic_auth_value(&auth_value)
            .map_err(|_| Status::unauthenticated("invalid credentials"))?;

        Ok(AuthContext::admin())
    }

    /// Validates a bearer token and evicts expired sessions on access.
    async fn authorize_bearer(&self, token: &str) -> Result<AuthContext, Status> {
        let mut tokens = self.auth_tokens.write().await;
        let now = Instant::now();
        tokens.retain(|_, token| token.expires_at > now);

        let stored = tokens
            .get(token)
            .ok_or_else(|| Status::unauthenticated("invalid or expired bearer token"))?;

        Ok(stored.auth.clone())
    }
}
