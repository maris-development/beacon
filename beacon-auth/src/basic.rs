//! Default in-memory username/password authentication provider.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures::future::BoxFuture;
use parking_lot::RwLock;

use crate::{
    credential::Credential,
    password::{hash_password, verify_password},
    provider::{AuthProvider, Authenticated, UserDirectory},
};

#[derive(Debug, Clone)]
struct UserRecord {
    password_hash: String,
    roles: HashSet<String>,
}

/// In-memory store of users, their hashed passwords, and assigned roles.
#[derive(Debug, Default)]
pub struct InMemoryUserStore {
    users: RwLock<HashMap<String, UserRecord>>,
}

impl InMemoryUserStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the role names assigned to a user after verifying their password.
    fn verify(&self, username: &str, password: &str) -> anyhow::Result<Vec<String>> {
        let users = self.users.read();
        let record = users
            .get(username)
            .ok_or_else(|| anyhow::anyhow!("authentication failed"))?;
        if !verify_password(&record.password_hash, password) {
            anyhow::bail!("authentication failed");
        }
        Ok(record.roles.iter().cloned().collect())
    }
}

impl UserDirectory for InMemoryUserStore {
    fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        let password_hash = hash_password(password)?;
        let mut users = self.users.write();
        if users.contains_key(username) {
            anyhow::bail!("user '{username}' already exists");
        }
        users.insert(
            username.to_string(),
            UserRecord {
                password_hash,
                roles: HashSet::new(),
            },
        );
        Ok(())
    }

    fn drop_user(&self, username: &str) -> anyhow::Result<()> {
        if self.users.write().remove(username).is_none() {
            anyhow::bail!("user '{username}' does not exist");
        }
        Ok(())
    }

    fn grant_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        let mut users = self.users.write();
        let record = users
            .get_mut(username)
            .ok_or_else(|| anyhow::anyhow!("user '{username}' does not exist"))?;
        record.roles.insert(role.to_string());
        Ok(())
    }

    fn revoke_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        let mut users = self.users.write();
        let record = users
            .get_mut(username)
            .ok_or_else(|| anyhow::anyhow!("user '{username}' does not exist"))?;
        record.roles.remove(role);
        Ok(())
    }

    fn user_exists(&self, username: &str) -> bool {
        self.users.read().contains_key(username)
    }
}

/// Default authentication provider backed by an in-memory user store.
///
/// Handles [`Credential::Basic`]; rejects bearer tokens so a composite provider can route those to
/// an external provider instead.
#[derive(Debug, Default, Clone)]
pub struct BasicAuthProvider {
    store: Arc<InMemoryUserStore>,
}

impl BasicAuthProvider {
    pub fn new() -> Self {
        Self {
            store: Arc::new(InMemoryUserStore::new()),
        }
    }

    /// Direct access to the user store (used to seed the bootstrap admin at startup).
    pub fn store(&self) -> Arc<InMemoryUserStore> {
        self.store.clone()
    }
}

impl AuthProvider for BasicAuthProvider {
    fn authenticate<'a>(
        &'a self,
        credential: &'a Credential,
    ) -> BoxFuture<'a, anyhow::Result<Authenticated>> {
        Box::pin(async move {
            match credential {
                Credential::Basic { username, password } => {
                    let roles = self.store.verify(username, password)?;
                    Ok(Authenticated {
                        username: username.clone(),
                        roles,
                    })
                }
                Credential::Bearer(_) => {
                    anyhow::bail!("basic auth provider does not accept bearer credentials")
                }
            }
        })
    }

    fn user_directory(&self) -> Option<Arc<dyn UserDirectory>> {
        Some(self.store.clone() as Arc<dyn UserDirectory>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn authenticate_returns_roles() {
        let provider = BasicAuthProvider::new();
        let dir = provider.user_directory().unwrap();
        dir.create_user("alice", "secret").unwrap();
        dir.grant_role("alice", "reader").unwrap();

        let authed = provider
            .authenticate(&Credential::basic("alice", "secret"))
            .await
            .unwrap();
        assert_eq!(authed.username, "alice");
        assert_eq!(authed.roles, vec!["reader".to_string()]);
    }

    #[tokio::test]
    async fn wrong_password_fails() {
        let provider = BasicAuthProvider::new();
        provider
            .user_directory()
            .unwrap()
            .create_user("alice", "secret")
            .unwrap();
        assert!(provider
            .authenticate(&Credential::basic("alice", "wrong"))
            .await
            .is_err());
        assert!(provider
            .authenticate(&Credential::basic("ghost", "secret"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn bearer_credential_is_rejected() {
        let provider = BasicAuthProvider::new();
        assert!(provider
            .authenticate(&Credential::bearer("a.b.c"))
            .await
            .is_err());
    }

    #[test]
    fn user_crud_and_role_assignment() {
        let store = InMemoryUserStore::new();
        store.create_user("alice", "secret").unwrap();
        assert!(store.user_exists("alice"));
        assert!(store.create_user("alice", "again").is_err());

        store.grant_role("alice", "reader").unwrap();
        store.grant_role("alice", "writer").unwrap();
        store.revoke_role("alice", "writer").unwrap();
        assert_eq!(store.verify("alice", "secret").unwrap(), vec!["reader".to_string()]);

        store.drop_user("alice").unwrap();
        assert!(!store.user_exists("alice"));
        assert!(store.drop_user("alice").is_err());
    }
}
