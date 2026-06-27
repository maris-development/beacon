//! Composite authentication provider: routes by credential kind.
//!
//! `Basic` credentials go to a local provider (the SQLite/in-memory user store); `Bearer` tokens
//! go to an external token provider (OIDC). User management (`CREATE USER`, …) is delegated to the
//! local provider's directory, so local admin users coexist with external IdP users.

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::{
    credential::Credential,
    provider::{AuthProvider, Authenticated, UserDirectory},
};

/// Routes [`Credential::Basic`] to `local` and [`Credential::Bearer`] to `token`.
pub struct CompositeAuthProvider {
    local: Arc<dyn AuthProvider>,
    token: Arc<dyn AuthProvider>,
}

impl CompositeAuthProvider {
    pub fn new(local: Arc<dyn AuthProvider>, token: Arc<dyn AuthProvider>) -> Self {
        Self { local, token }
    }
}

impl AuthProvider for CompositeAuthProvider {
    fn authenticate<'a>(
        &'a self,
        credential: &'a Credential,
    ) -> BoxFuture<'a, anyhow::Result<Authenticated>> {
        match credential {
            Credential::Basic { .. } => self.local.authenticate(credential),
            Credential::Bearer(_) => self.token.authenticate(credential),
        }
    }

    fn user_directory(&self) -> Option<Arc<dyn UserDirectory>> {
        // SQL-managed users live in the local provider; the token provider has no directory.
        self.local.user_directory()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{basic::BasicAuthProvider, oidc::{OidcAuthProvider, OidcConfig}};
    use std::time::Duration;

    fn composite() -> CompositeAuthProvider {
        let local = Arc::new(BasicAuthProvider::new());
        local
            .user_directory()
            .unwrap()
            .create_user("alice", "secret")
            .unwrap();
        let oidc = Arc::new(OidcAuthProvider::new(OidcConfig {
            issuer: "https://issuer.example".to_string(),
            jwks_url: "https://issuer.example/jwks".to_string(),
            audience: None,
            roles_claim: "realm_access.roles".to_string(),
            username_claim: "preferred_username".to_string(),
            jwks_cache_ttl: Duration::from_secs(300),
        }));
        CompositeAuthProvider::new(local, oidc)
    }

    #[tokio::test]
    async fn basic_is_routed_to_local() {
        let provider = composite();
        let authed = provider
            .authenticate(&Credential::basic("alice", "secret"))
            .await
            .unwrap();
        assert_eq!(authed.username, "alice");
    }

    #[tokio::test]
    async fn bearer_is_routed_to_oidc() {
        // A bogus token reaches the OIDC provider and fails validation there (not at the local
        // store), confirming the routing.
        let provider = composite();
        let err = provider
            .authenticate(&Credential::bearer("not-a-jwt"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("token header") || err.contains("invalid"), "got: {err}");
    }

    #[test]
    fn user_directory_is_the_local_one() {
        let provider = composite();
        assert!(provider.user_directory().is_some());
        assert!(provider.user_directory().unwrap().user_exists("alice"));
    }
}
