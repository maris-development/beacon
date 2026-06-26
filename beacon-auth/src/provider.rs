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

/// SQL-managed user lifecycle and role assignment, exposed by providers that own their user store.
pub trait UserDirectory: Send + Sync {
    fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()>;
    fn drop_user(&self, username: &str) -> anyhow::Result<()>;
    fn grant_role(&self, username: &str, role: &str) -> anyhow::Result<()>;
    fn revoke_role(&self, username: &str, role: &str) -> anyhow::Result<()>;
    /// Whether a user with `username` exists. Used for idempotent bootstrap.
    fn user_exists(&self, username: &str) -> bool;
}
