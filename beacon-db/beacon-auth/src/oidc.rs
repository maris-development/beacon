//! OIDC/OAuth2 authentication provider.
//!
//! Validates a `Bearer` JWT access token against the issuer's JWKS (signature, `exp`, `iss`, and
//! optionally `aud`), then maps a configurable claim to the principal's username and another to its
//! role names. Beacon still owns authorization: the role names map onto the local role/grant model.
//!
//! The provider holds no user directory — OIDC users are managed in the external IdP, not via
//! Beacon SQL.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::BoxFuture;
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, DecodingKey, Validation};
use parking_lot::Mutex;
use serde_json::Value;

use crate::{
    credential::Credential,
    provider::{AuthProvider, Authenticated},
};

/// Configuration for the OIDC provider.
#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// Expected token issuer (`iss` claim).
    pub issuer: String,
    /// URL of the issuer's JWKS document (signing keys).
    pub jwks_url: String,
    /// Expected audience (`aud` claim). `None` disables audience validation.
    pub audience: Option<String>,
    /// Dotted path to the claim holding the principal's role names (e.g. `realm_access.roles`).
    pub roles_claim: String,
    /// Dotted path to the claim holding the principal's username (e.g. `preferred_username`).
    pub username_claim: String,
    /// How long a fetched JWKS document is cached before being re-fetched.
    pub jwks_cache_ttl: Duration,
}

struct CachedJwks {
    keys: Arc<JwkSet>,
    fetched_at: Instant,
}

/// Authentication provider that validates OIDC/OAuth2 bearer JWTs.
pub struct OidcAuthProvider {
    config: OidcConfig,
    http: reqwest::Client,
    cache: Mutex<Option<CachedJwks>>,
}

impl std::fmt::Debug for OidcAuthProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcAuthProvider")
            .field("issuer", &self.config.issuer)
            .finish_non_exhaustive()
    }
}

impl OidcAuthProvider {
    pub fn new(config: OidcConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
            cache: Mutex::new(None),
        }
    }

    /// Returns the cached JWKS if still fresh, otherwise fetches and caches a new copy.
    ///
    /// The lock is never held across the network fetch.
    async fn jwks(&self) -> anyhow::Result<Arc<JwkSet>> {
        if let Some(cached) = self.cache.lock().as_ref() {
            if cached.fetched_at.elapsed() < self.config.jwks_cache_ttl {
                return Ok(cached.keys.clone());
            }
        }

        let keys: JwkSet = self
            .http
            .get(&self.config.jwks_url)
            .send()
            .await
            .map_err(|err| anyhow::anyhow!("failed to fetch JWKS: {err}"))?
            .error_for_status()
            .map_err(|err| anyhow::anyhow!("JWKS endpoint returned an error: {err}"))?
            .json()
            .await
            .map_err(|err| anyhow::anyhow!("failed to parse JWKS: {err}"))?;

        let keys = Arc::new(keys);
        *self.cache.lock() = Some(CachedJwks {
            keys: keys.clone(),
            fetched_at: Instant::now(),
        });
        Ok(keys)
    }

    /// Validates a bearer JWT and resolves it to an authenticated principal.
    async fn verify_token(&self, token: &str) -> anyhow::Result<Authenticated> {
        let header =
            decode_header(token).map_err(|err| anyhow::anyhow!("invalid token header: {err}"))?;
        let kid = header
            .kid
            .ok_or_else(|| anyhow::anyhow!("token is missing a key id (kid)"))?;

        let jwks = self.jwks().await?;
        let jwk = jwks
            .find(&kid)
            .ok_or_else(|| anyhow::anyhow!("no signing key matches the token's kid"))?;
        let decoding_key = DecodingKey::from_jwk(jwk)
            .map_err(|err| anyhow::anyhow!("invalid signing key: {err}"))?;

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[self.config.issuer.as_str()]);
        match &self.config.audience {
            Some(audience) => validation.set_audience(&[audience.as_str()]),
            None => validation.validate_aud = false,
        }

        let claims = decode::<Value>(token, &decoding_key, &validation)
            .map_err(|err| anyhow::anyhow!("token validation failed: {err}"))?
            .claims;

        let username = claim_at(&claims, &self.config.username_claim)
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow::anyhow!("token is missing the '{}' claim", self.config.username_claim)
            })?
            .to_string();

        let roles = claim_at(&claims, &self.config.roles_claim)
            .map(roles_from_claim)
            .unwrap_or_default();

        Ok(Authenticated { username, roles })
    }
}

impl AuthProvider for OidcAuthProvider {
    fn authenticate<'a>(
        &'a self,
        credential: &'a Credential,
    ) -> BoxFuture<'a, anyhow::Result<Authenticated>> {
        Box::pin(async move {
            match credential {
                Credential::Bearer(token) => self.verify_token(token).await,
                Credential::Basic { .. } => {
                    anyhow::bail!("oidc provider does not accept basic credentials")
                }
            }
        })
    }
}

/// Resolves a dotted claim path (e.g. `realm_access.roles`) within a claims object.
fn claim_at<'a>(claims: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(claims, |value, segment| value.get(segment))
}

/// Extracts role names from a claim, accepting either an array of strings or a single
/// space-delimited string (both common in OIDC tokens).
fn roles_from_claim(value: &Value) -> Vec<String> {
    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect(),
        Value::String(s) => s.split_whitespace().map(str::to_string).collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_claim_path_resolves_nested() {
        let claims = serde_json::json!({
            "preferred_username": "alice",
            "realm_access": { "roles": ["reader", "writer"] }
        });
        assert_eq!(
            claim_at(&claims, "preferred_username").and_then(Value::as_str),
            Some("alice")
        );
        let roles = claim_at(&claims, "realm_access.roles").map(roles_from_claim);
        assert_eq!(roles, Some(vec!["reader".to_string(), "writer".to_string()]));
        assert!(claim_at(&claims, "missing.path").is_none());
    }

    #[test]
    fn roles_accepts_array_or_space_delimited_string() {
        assert_eq!(
            roles_from_claim(&serde_json::json!(["a", "b"])),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            roles_from_claim(&serde_json::json!("a b c")),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert!(roles_from_claim(&serde_json::json!(42)).is_empty());
    }

    #[test]
    fn dotted_path_through_a_non_object_yields_none() {
        let claims = serde_json::json!({ "user": "alice", "roles": ["reader"] });
        // Descending into a scalar (`user.name`) or an array (`roles.0`) resolves
        // to None rather than panicking.
        assert!(claim_at(&claims, "user.name").is_none());
        assert!(claim_at(&claims, "roles.reader").is_none());
        // A trailing empty segment does not match.
        assert!(claim_at(&claims, "user.").is_none());
    }

    #[test]
    fn roles_default_to_empty_when_claim_is_absent_or_ill_typed() {
        let claims = serde_json::json!({ "preferred_username": "alice" });
        let roles = claim_at(&claims, "realm_access.roles").map(roles_from_claim);
        assert_eq!(roles, None);
        // A present-but-wrong-typed roles claim contributes no roles rather than
        // erroring — the principal ends up with zero privileges (fail closed).
        assert!(roles_from_claim(&serde_json::json!({ "nested": "obj" })).is_empty());
        assert!(roles_from_claim(&serde_json::Value::Null).is_empty());
        // Non-string array elements are skipped, not stringified.
        assert_eq!(
            roles_from_claim(&serde_json::json!(["reader", 7, "writer"])),
            vec!["reader".to_string(), "writer".to_string()]
        );
        // A space-delimited string collapses runs of whitespace.
        assert_eq!(
            roles_from_claim(&serde_json::json!("  reader   writer ")),
            vec!["reader".to_string(), "writer".to_string()]
        );
    }

    /// A structurally invalid bearer token is rejected at header decoding, before
    /// any JWKS fetch — so this exercises the error path without a network.
    #[tokio::test]
    async fn malformed_bearer_token_fails_before_network() {
        let provider = OidcAuthProvider::new(OidcConfig {
            // An unreachable JWKS URL: if we ever reached the fetch this would
            // surface as a different error. We assert we fail earlier.
            issuer: "https://issuer.example".to_string(),
            jwks_url: "http://127.0.0.1:1/jwks".to_string(),
            audience: None,
            roles_claim: "realm_access.roles".to_string(),
            username_claim: "preferred_username".to_string(),
            jwks_cache_ttl: Duration::from_secs(300),
        });
        let err = provider
            .authenticate(&Credential::bearer("not-a-jwt"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("token header"), "got: {err}");
    }

    #[tokio::test]
    async fn basic_credential_is_rejected() {
        let provider = OidcAuthProvider::new(OidcConfig {
            issuer: "https://issuer.example".to_string(),
            jwks_url: "https://issuer.example/jwks".to_string(),
            audience: None,
            roles_claim: "realm_access.roles".to_string(),
            username_claim: "preferred_username".to_string(),
            jwks_cache_ttl: Duration::from_secs(300),
        });
        assert!(provider
            .authenticate(&Credential::basic("alice", "secret"))
            .await
            .is_err());
    }
}
