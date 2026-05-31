//! Default in-memory username/password authentication provider.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures::future::BoxFuture;
use parking_lot::RwLock;

use crate::{
    password::{hash_password, verify_password},
    provider::{AuthProvider, UserDirectory},
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
/// Expects credentials in the form `"username:password"`.
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
    fn authenticate<'a>(&'a self, auth_str: &'a str) -> BoxFuture<'a, anyhow::Result<Vec<String>>> {
        Box::pin(async move {
            let (username, password) = auth_str
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("expected credentials in 'username:password' form"))?;
            self.store.verify(username, password)
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

        let roles = provider.authenticate("alice:secret").await.unwrap();
        assert_eq!(roles, vec!["reader".to_string()]);
    }

    #[tokio::test]
    async fn wrong_password_fails() {
        let provider = BasicAuthProvider::new();
        provider
            .user_directory()
            .unwrap()
            .create_user("alice", "secret")
            .unwrap();
        assert!(provider.authenticate("alice:wrong").await.is_err());
        assert!(provider.authenticate("ghost:secret").await.is_err());
    }

    #[tokio::test]
    async fn malformed_credential_string_fails() {
        let provider = BasicAuthProvider::new();
        assert!(provider.authenticate("no-colon").await.is_err());
    }

    #[test]
    fn user_crud_and_role_assignment() {
        let store = InMemoryUserStore::new();
        store.create_user("bob", "pw").unwrap();
        assert!(store.user_exists("bob"));
        assert!(store.create_user("bob", "pw").is_err());

        store.grant_role("bob", "writer").unwrap();
        assert_eq!(store.verify("bob", "pw").unwrap(), vec!["writer".to_string()]);

        store.revoke_role("bob", "writer").unwrap();
        assert!(store.verify("bob", "pw").unwrap().is_empty());

        store.drop_user("bob").unwrap();
        assert!(!store.user_exists("bob"));
        assert!(store.drop_user("bob").is_err());
    }
}
