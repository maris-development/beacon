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

use crate::auth::keycloak::{self, KcLayer, KeycloakLayers};
use crate::auth::{parse_bearer_token, validate_basic_auth_credentials, verify_basic_auth_value};

/// Keycloak layers held by the Flight SQL [`Authenticator`].
///
/// `admin_layer` is always present in Keycloak mode and used for validation when the
/// query layer is off. `query_layer` is `Some` when `BEACON_KEYCLOAK_PROTECT_QUERY=true`;
/// in that mode every Flight SQL request is validated against the query layer (anonymous
/// access is disabled regardless of `BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS`).
#[derive(Clone)]
pub(super) struct KeycloakAuth {
    admin_layer: KcLayer,
    query_layer: Option<KcLayer>,
}

impl KeycloakAuth {
    /// Returns the layer used for validating incoming JWTs.
    ///
    /// Prefers the query layer when present so user-role tokens are accepted.
    /// The admin role is then derived from the decoded claims by
    /// [`keycloak::validate_raw_token`].
    fn validation_layer(&self) -> &KcLayer {
        self.query_layer.as_ref().unwrap_or(&self.admin_layer)
    }

    /// Whether anonymous handshake/requests are permitted.
    ///
    /// Anonymous is allowed only when query protection is off; in that mode the
    /// caller's `allow_anonymous` configuration is honored.
    fn allows_anonymous(&self) -> bool {
        self.query_layer.is_none()
    }
}

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
///
/// When [`Self::keycloak`] is `Some`, all credentials are validated as
/// Keycloak-issued Bearer JWTs (Basic auth is rejected) and the token's
/// roles decide whether the session has admin (DDL) privileges. When `None`,
/// the legacy hardcoded Basic admin credentials are used and short-lived UUID
/// session tokens are issued after a successful handshake.
#[derive(Clone)]
pub(super) struct Authenticator {
    allow_anonymous: bool,
    token_ttl: Duration,
    auth_tokens: Arc<tokio::sync::RwLock<HashMap<String, AuthToken>>>,
    keycloak: Option<KeycloakAuth>,
}

impl Authenticator {
    /// Creates a new authenticator with the configured anonymous-access policy and token TTL.
    pub(super) fn new(
        allow_anonymous: bool,
        token_ttl: Duration,
        keycloak_layers: KeycloakLayers,
    ) -> Self {
        let keycloak = keycloak_layers.admin.map(|admin_layer| KeycloakAuth {
            admin_layer,
            query_layer: keycloak_layers.query,
        });
        Self {
            allow_anonymous,
            token_ttl,
            auth_tokens: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            keycloak,
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
    ///
    /// Anonymous access is only honored when query protection is off; with
    /// `BEACON_KEYCLOAK_PROTECT_QUERY=true` every Flight SQL request must
    /// present a JWT regardless of `BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS`.
    pub(super) async fn authorize_optional(
        &self,
        auth_value: Option<String>,
    ) -> Result<AuthContext, Status> {
        let anonymous_allowed = self.allow_anonymous
            && self
                .keycloak
                .as_ref()
                .map(KeycloakAuth::allows_anonymous)
                .unwrap_or(true);
        match auth_value {
            Some(auth_value) => self.authorize_value(auth_value).await,
            None if anonymous_allowed => Ok(AuthContext::anonymous()),
            None => Err(Status::unauthenticated("missing authorization metadata")),
        }
    }

    /// Authenticates the Flight SQL handshake and decides whether the session is admin or anonymous.
    pub(super) async fn authorize_handshake(
        &self,
        auth_value: Option<&str>,
        handshake: Option<&HandshakeRequest>,
    ) -> Result<AuthContext, Status> {
        // Keycloak mode: only Bearer JWTs are accepted; Basic credentials are rejected.
        if let Some(kc) = &self.keycloak {
            if let Some(auth_value) = auth_value {
                let token = parse_bearer_token(auth_value)
                    .map_err(|_| Status::unauthenticated("invalid credentials"))?;
                let claims = keycloak::validate_raw_token(kc.validation_layer(), token)
                    .await
                    .map_err(|_| Status::unauthenticated("invalid credentials"))?;
                return Ok(AuthContext {
                    is_super_user: claims.is_admin,
                });
            }
            // Anonymous is only honored when query protection is off; otherwise
            // every Flight SQL session must present a JWT.
            if kc.allows_anonymous() && self.allow_anonymous {
                return Ok(AuthContext::anonymous());
            }
            return Err(Status::unauthenticated("invalid credentials"));
        }

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
        // Keycloak mode: bearer tokens are validated directly as Keycloak JWTs;
        // Basic credentials are rejected. The token's roles decide whether the
        // session has admin (DDL) privileges via `is_super_user`.
        if let Some(kc) = &self.keycloak {
            let token = parse_bearer_token(&auth_value)
                .map_err(|_| Status::unauthenticated("invalid credentials"))?;
            let claims = keycloak::validate_raw_token(kc.validation_layer(), token)
                .await
                .map_err(|_| Status::unauthenticated("invalid credentials"))?;
            return Ok(AuthContext {
                is_super_user: claims.is_admin,
            });
        }

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
