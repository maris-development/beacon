//! Pluggable authentication provider interface.

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::credential::Credential;

/// The result of a successful authentication: who the principal is and which role names they hold.
///
/// Beacon owns the authorization model; the provider only answers "who is this and what roles do
/// they have". For local providers the roles come from the user store; for external providers
/// (e.g. OIDC) they are mapped from a token claim.
#[derive(Debug, Clone)]
pub struct Authenticated {
    pub username: String,
    pub roles: Vec<String>,
}

/// A pluggable authentication backend.
pub trait AuthProvider: Send + Sync {
    /// Authenticates a [`Credential`] and returns the principal's identity and role names.
    ///
    /// Providers should return an error (not an empty result) for credential kinds they do not
    /// handle, so a [`CompositeAuthProvider`](crate::CompositeAuthProvider) can fall through.
    fn authenticate<'a>(
        &'a self,
        credential: &'a Credential,
    ) -> BoxFuture<'a, anyhow::Result<Authenticated>>;

    /// Optional in-process user management. Returns `None` for providers backed by an external
    /// directory (e.g. OIDC) that cannot be managed through Beacon SQL.
    fn user_directory(&self) -> Option<Arc<dyn UserDirectory>> {
        None
    }
}

/// A stored user and the roles assigned to it.
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub username: String,
    pub roles: Vec<String>,
}

/// A user as held in durable storage, including the password hash.
///
/// Only used to hydrate an in-memory working copy at startup; the hash never leaves the auth
/// subsystem ([`UserRecord`] is the shape exposed to callers).
#[derive(Debug, Clone)]
pub struct StoredUser {
    pub username: String,
    pub password_hash: String,
    pub roles: Vec<String>,
}

/// SQL-managed user lifecycle and role assignment, exposed by providers that own their user store.
///
/// Implemented both by the in-memory working copy ([`InMemoryUserStore`](crate::InMemoryUserStore))
/// and by the durable backend it writes through to, so the same shape covers both ends.
#[async_trait::async_trait]
pub trait UserDirectory: std::fmt::Debug + Send + Sync {
    async fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()>;
    async fn drop_user(&self, username: &str) -> anyhow::Result<()>;
    async fn grant_role(&self, username: &str, role: &str) -> anyhow::Result<()>;
    async fn revoke_role(&self, username: &str, role: &str) -> anyhow::Result<()>;
    /// Whether a user with `username` exists. Used for idempotent bootstrap.
    async fn user_exists(&self, username: &str) -> bool;
    /// Enumerate all stored users with their assigned roles.
    async fn list_users(&self) -> anyhow::Result<Vec<UserRecord>>;
    /// Loads every user with their password hash, to hydrate an in-memory working copy at startup.
    async fn load_users(&self) -> anyhow::Result<Vec<StoredUser>>;

    /// Rebuilds this directory's in-memory working copy from its durable backend. The default is a
    /// no-op: a durable backend has nothing to hydrate *from*, and only the in-memory working copy
    /// ([`InMemoryUserStore`](crate::InMemoryUserStore)) overrides it. Called once at startup by
    /// [`AuthContext::hydrate`](crate::AuthContext::hydrate).
    async fn hydrate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
