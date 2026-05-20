//! Optional Keycloak Bearer-JWT authentication shared by the HTTP and Flight SQL transports.
//!
//! Construction is driven by [`beacon_config::CONFIG.keycloak`]. Two independent
//! [`KcLayer`]s are built when Keycloak is enabled — an **admin** layer (always present
//! when enabled) and a **query** layer (present when `protect_query=true`). The
//! admin layer's role should be configured as a composite of the query role in
//! Keycloak so admin tokens satisfy both gates.

use std::sync::Arc;

use axum_keycloak_auth::{
    decode::KeycloakToken,
    instance::{KeycloakAuthInstance, KeycloakConfig as KcInstanceConfig},
    layer::KeycloakAuthLayer,
    role::KeycloakRole,
    PassthroughMode,
};
use url::Url;

use crate::auth::AuthError;

/// Shared Keycloak validation layer.
///
/// `KeycloakAuthLayer` is `Clone` and is used both as an Axum middleware and as a
/// standalone validator (Flight SQL) via [`validate_raw_token`].
pub(crate) type KcLayer = KeycloakAuthLayer<String>;

/// Pair of Keycloak layers produced at startup.
///
/// `admin` is `Some` whenever Keycloak is enabled; `query` is `Some` only when
/// `BEACON_KEYCLOAK_PROTECT_QUERY=true`. Both layers share a single
/// [`KeycloakAuthInstance`], so JWKS discovery and caching happen once.
pub(crate) struct KeycloakLayers {
    pub(crate) admin: Option<KcLayer>,
    pub(crate) query: Option<KcLayer>,
}

/// Claims extracted from a validated Keycloak token, exposed to Flight SQL so it
/// can decide between read-only and admin (DDL) execution.
pub(crate) struct ValidatedClaims {
    pub(crate) is_admin: bool,
}

/// Builds the admin and (optionally) query Keycloak layers from configuration.
///
/// Returns `KeycloakLayers { admin: None, query: None }` when Keycloak is disabled,
/// in which case callers fall back to HTTP Basic admin authentication and anonymous
/// query access.
///
/// Must be called from within a tokio runtime: the underlying
/// [`KeycloakAuthInstance`] spawns a background task for JWKS refresh.
pub(crate) fn build_layers() -> anyhow::Result<KeycloakLayers> {
    let cfg = &beacon_config::CONFIG.keycloak;
    if !cfg.enabled {
        return Ok(KeycloakLayers {
            admin: None,
            query: None,
        });
    }

    let server = Url::parse(&cfg.server_url)
        .map_err(|e| anyhow::anyhow!("invalid BEACON_KEYCLOAK_SERVER_URL: {e}"))?;

    let instance = Arc::new(KeycloakAuthInstance::new(
        KcInstanceConfig::builder()
            .server(server)
            .realm(cfg.realm.clone())
            .build(),
    ));

    let admin = build_layer_with_role(instance.clone(), cfg.required_role.clone());
    let query = cfg
        .protect_query
        .then(|| build_layer_with_role(instance, cfg.query_role.clone()));

    Ok(KeycloakLayers {
        admin: Some(admin),
        query,
    })
}

fn build_layer_with_role(instance: Arc<KeycloakAuthInstance>, role: String) -> KcLayer {
    let cfg = &beacon_config::CONFIG.keycloak;
    KeycloakAuthLayer::<String>::builder()
        .instance(instance)
        .passthrough_mode(PassthroughMode::Block)
        .persist_raw_claims(false)
        .expected_audiences(vec![cfg.expected_audience.clone()])
        .required_roles(vec![role])
        .build()
}

/// Validates a raw Bearer JWT against the given Keycloak layer and extracts the
/// claims Flight SQL needs to decide whether the token represents an admin.
///
/// `is_admin` is `true` when the validated token carries the configured admin
/// role (`BEACON_KEYCLOAK_REQUIRED_ROLE`) as either a realm role or any client
/// role. Validation against `layer` enforces signature, audience, expiry, and
/// the layer's own required role (admin layer requires admin role; query layer
/// requires query role).
pub(crate) async fn validate_raw_token(
    layer: &KcLayer,
    token: &str,
) -> Result<ValidatedClaims, AuthError> {
    let (_, decoded): (_, KeycloakToken<String>) =
        layer.validate_raw_token(token).await.map_err(|e| {
            tracing::debug!("Keycloak token validation failed: {e}");
            AuthError
        })?;

    let admin_role = &beacon_config::CONFIG.keycloak.required_role;
    let is_admin = decoded.roles.iter().any(|r| match r {
        KeycloakRole::Realm { role } => role == admin_role,
        KeycloakRole::Client { role, .. } => role == admin_role,
    });

    Ok(ValidatedClaims { is_admin })
}
