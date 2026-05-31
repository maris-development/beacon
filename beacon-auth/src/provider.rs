//! Pluggable authentication provider interface.

use std::sync::Arc;

use futures::future::BoxFuture;

/// A pluggable authentication backend.
///
/// Implementations validate a credential string and return the names of the roles the
/// authenticated principal holds. Beacon owns the authorization model; the provider only
/// answers "who is this and what roles do they have".
pub trait AuthProvider: Send + Sync {
    /// Authenticates `auth_str` (for the basic provider, `"username:password"`) and returns the
    /// principal's role names.
    fn authenticate<'a>(&'a self, auth_str: &'a str) -> BoxFuture<'a, anyhow::Result<Vec<String>>>;

    /// Optional in-process user management. Returns `None` for providers backed by an external
    /// directory (e.g. a future Keycloak provider) that cannot be managed through Beacon SQL.
    fn user_directory(&self) -> Option<Arc<dyn UserDirectory>> {
        None
    }
}

/// SQL-managed user lifecycle and role assignment, exposed by providers that own their user store.
pub trait UserDirectory: Send + Sync {
    fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()>;
    fn drop_user(&self, username: &str) -> anyhow::Result<()>;
    fn grant_role(&self, username: &str, role: &str) -> anyhow::Result<()>;
    fn revoke_role(&self, username: &str, role: &str) -> anyhow::Result<()>;
    /// Whether a user with `username` exists. Used for idempotent bootstrap.
    fn user_exists(&self, username: &str) -> bool;
}
