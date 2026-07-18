//! In-memory, DuckDB-style secret store for object-store credentials.
//!
//! A [`Secret`] is a named set of credentials scoped to a URL prefix (its
//! `scope`, e.g. `s3://bucket` or `s3://` for all of S3). When an object store
//! must be built for a path, the [`SecretStore`] returns the secret whose scope
//! is the **longest matching prefix** of that path — so multiple accounts /
//! buckets can coexist and a broad `s3://` secret acts as a default.
//!
//! Credentials are held in memory only (nothing is persisted yet). The option
//! keys are `object_store` config keys (e.g. `access_key_id`,
//! `secret_access_key`, `region`, `endpoint`, `session_token`, `allow_http`),
//! so they can be handed straight to the backend builders.

use std::collections::HashMap;
use std::fmt;

use parking_lot::RwLock;
use url::Url;

/// The object-store backend a [`Secret`] provides credentials for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretType {
    S3,
    Gcs,
    Azure,
    Http,
}

impl SecretType {
    /// Whether this secret type applies to a URL of the given scheme.
    pub fn matches_scheme(&self, scheme: &str) -> bool {
        matches!(
            (self, scheme),
            (SecretType::S3, "s3" | "s3a")
                | (SecretType::Gcs, "gs")
                | (
                    SecretType::Azure,
                    "az" | "azure" | "abfs" | "abfss" | "wasb" | "wasbs" | "adl"
                )
                | (SecretType::Http, "http" | "https")
        )
    }
}

/// A named, scoped set of object-store credentials.
#[derive(Clone)]
pub struct Secret {
    /// Unique name (the key in the [`SecretStore`]).
    pub name: String,
    /// Which backend these credentials are for.
    pub secret_type: SecretType,
    /// URL-prefix scope this secret applies to, e.g. `s3://bucket` or `s3://`
    /// (all S3). Matched against a path as a longest-prefix (DuckDB-style).
    pub scope: String,
    /// `object_store` config keys → values (e.g. `access_key_id`,
    /// `secret_access_key`, `region`, `endpoint`, `session_token`,
    /// `allow_http`). Applied verbatim to the backend builder.
    pub options: HashMap<String, String>,
}

// Manual Debug: never print credential *values* (only the option keys), so a
// secret can't leak into logs via a `{:?}` of the runtime/registry.
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut keys: Vec<&str> = self.options.keys().map(String::as_str).collect();
        keys.sort_unstable();
        f.debug_struct("Secret")
            .field("name", &self.name)
            .field("secret_type", &self.secret_type)
            .field("scope", &self.scope)
            .field("option_keys", &keys)
            .finish()
    }
}

/// In-memory, thread-safe secret store.
///
/// Also carries the deployment master key used to encrypt/decrypt credentials at
/// rest (e.g. external-database passwords). The store is published as a session
/// extension, so plan-time code reaches the key through it rather than through a
/// configuration type — keeping the lower layers free of a config dependency.
#[derive(Default)]
pub struct SecretStore {
    secrets: RwLock<HashMap<String, Secret>>,
    master_key: Option<[u8; 32]>,
}

impl SecretStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a store carrying the deployment master key. `None` leaves credential
    /// encryption unavailable (storing an encrypted secret then fails closed).
    pub fn new_with_master_key(master_key: Option<[u8; 32]>) -> Self {
        Self {
            master_key,
            ..Default::default()
        }
    }

    /// The deployment master key, or `None` when credential encryption is disabled.
    pub fn master_key(&self) -> Option<&[u8; 32]> {
        self.master_key.as_ref()
    }

    /// Insert (or replace) a secret, returning the previous one with that name.
    pub fn add(&self, secret: Secret) -> Option<Secret> {
        self.secrets.write().insert(secret.name.clone(), secret)
    }

    /// Remove a secret by name, returning it if present.
    pub fn remove(&self, name: &str) -> Option<Secret> {
        self.secrets.write().remove(name)
    }

    /// Snapshot of all secrets (clones).
    pub fn list(&self) -> Vec<Secret> {
        self.secrets.read().values().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.secrets.read().is_empty()
    }

    /// The best secret for `url`: the longest-scope secret whose type matches
    /// the URL scheme and whose scope is a prefix of the URL. `None` if no
    /// secret applies (the caller then falls back to the environment chain).
    pub fn resolve(&self, url: &Url) -> Option<Secret> {
        let url_str = url.as_str();
        let scheme = url.scheme();
        self.secrets
            .read()
            .values()
            .filter(|s| s.secret_type.matches_scheme(scheme) && scope_matches(&s.scope, url_str))
            .max_by_key(|s| s.scope.len())
            .cloned()
    }
}

impl fmt::Debug for SecretStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<String> = self.secrets.read().keys().cloned().collect();
        names.sort_unstable();
        f.debug_struct("SecretStore").field("names", &names).finish()
    }
}

/// Whether `scope` is a prefix of `url` respecting path-segment boundaries, so
/// `s3://bucket` matches `s3://bucket/x` but not `s3://bucketother/x`.
fn scope_matches(scope: &str, url: &str) -> bool {
    let Some(rest) = url.strip_prefix(scope) else {
        return false;
    };
    rest.is_empty() || scope.ends_with('/') || rest.starts_with('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret(name: &str, ty: SecretType, scope: &str) -> Secret {
        Secret {
            name: name.to_string(),
            secret_type: ty,
            scope: scope.to_string(),
            options: HashMap::new(),
        }
    }

    #[test]
    fn add_list_remove() {
        let store = SecretStore::new();
        assert!(store.is_empty());
        assert!(store.add(secret("a", SecretType::S3, "s3://")).is_none());
        assert_eq!(store.list().len(), 1);
        // replacing returns the old value
        assert!(store.add(secret("a", SecretType::S3, "s3://x")).is_some());
        assert_eq!(store.list().len(), 1);
        assert!(store.remove("a").is_some());
        assert!(store.is_empty());
    }

    #[test]
    fn resolve_longest_prefix_wins() {
        let store = SecretStore::new();
        store.add(secret("default", SecretType::S3, "s3://"));
        store.add(secret("private", SecretType::S3, "s3://private"));

        let pick = |u: &str| store.resolve(&Url::parse(u).unwrap()).map(|s| s.name);
        assert_eq!(pick("s3://private/y").as_deref(), Some("private"));
        assert_eq!(pick("s3://public/x").as_deref(), Some("default"));
    }

    #[test]
    fn resolve_respects_segment_boundary() {
        let store = SecretStore::new();
        store.add(secret("bucket", SecretType::S3, "s3://bucket"));
        // Same prefix string but different bucket must NOT match.
        assert!(store
            .resolve(&Url::parse("s3://bucketother/x").unwrap())
            .is_none());
        assert_eq!(
            store
                .resolve(&Url::parse("s3://bucket/x").unwrap())
                .map(|s| s.name)
                .as_deref(),
            Some("bucket")
        );
    }

    #[test]
    fn resolve_filters_by_type() {
        let store = SecretStore::new();
        store.add(secret("s3", SecretType::S3, "s3://"));
        // A gs:// URL must not match an S3 secret.
        assert!(store.resolve(&Url::parse("gs://bucket/x").unwrap()).is_none());
    }

    #[test]
    fn debug_redacts_option_values() {
        let mut options = HashMap::new();
        options.insert("access_key_id".to_string(), "AKIASECRET".to_string());
        options.insert("secret_access_key".to_string(), "topsecret".to_string());
        let s = Secret {
            name: "s".into(),
            secret_type: SecretType::S3,
            scope: "s3://".into(),
            options,
        };
        let dbg = format!("{s:?}");
        assert!(!dbg.contains("AKIASECRET"), "leaked key id: {dbg}");
        assert!(!dbg.contains("topsecret"), "leaked secret: {dbg}");
        // keys are fine to show
        assert!(dbg.contains("access_key_id"), "{dbg}");
    }
}
