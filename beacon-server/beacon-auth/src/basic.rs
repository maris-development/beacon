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
    provider::{AuthProvider, Authenticated, StoredUser, UserDirectory},
};

#[derive(Debug, Clone)]
struct UserRecord {
    password_hash: String,
    roles: HashSet<String>,
}

/// In-memory store of users, their hashed passwords, and assigned roles.
///
/// This is the working copy consulted on every login. When a durable [`UserDirectory`] is attached
/// it is hydrated from it at startup ([`hydrate`](InMemoryUserStore::hydrate)) and every mutation is
/// written through, so authentication never touches storage.
#[derive(Debug, Default)]
pub struct InMemoryUserStore {
    users: RwLock<HashMap<String, UserRecord>>,
    persistence: Option<Arc<dyn UserDirectory>>,
    /// Serializes mutations so each validate -> persist -> apply sequence is atomic. See
    /// [`RoleProvider`](crate::RoleProvider)'s equivalent: the `users` guard is `!Send` and cannot
    /// span the persist `.await`, so this async mutex closes the window between the two steps.
    write_lock: futures::lock::Mutex<()>,
}

impl InMemoryUserStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attaches `store` without reading it: every later mutation is written through, but the
    /// in-memory map stays empty until [`hydrate`](InMemoryUserStore::hydrate) runs.
    ///
    /// Split from hydration because the tables-backed store reaches its tables through the session,
    /// which does not exist yet when the auth context is built (see `runtime_builder`).
    pub fn with_store(store: Arc<dyn UserDirectory>) -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            persistence: Some(store),
            write_lock: futures::lock::Mutex::new(()),
        }
    }

    /// Replaces the in-memory map with the durable store's contents. No-op without a store.
    pub async fn hydrate(&self) -> anyhow::Result<()> {
        let Some(store) = &self.persistence else {
            return Ok(());
        };
        let _write = self.write_lock.lock().await;
        let loaded = store.load_users().await?;
        *self.users.write() = loaded
            .into_iter()
            .map(|user| {
                (
                    user.username,
                    UserRecord {
                        password_hash: user.password_hash,
                        roles: user.roles.into_iter().collect(),
                    },
                )
            })
            .collect();
        Ok(())
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

    fn assert_user_exists(&self, username: &str) -> anyhow::Result<()> {
        if !self.users.read().contains_key(username) {
            anyhow::bail!("user '{username}' does not exist");
        }
        Ok(())
    }

    /// Applies `apply` to a user under a short-lived write guard. Callers hold `write_lock`, so the
    /// user cannot have disappeared since `assert_user_exists`.
    fn with_user(&self, username: &str, apply: impl FnOnce(&mut UserRecord)) -> anyhow::Result<()> {
        let mut users = self.users.write();
        let record = users
            .get_mut(username)
            .ok_or_else(|| anyhow::anyhow!("user '{username}' does not exist"))?;
        apply(record);
        Ok(())
    }
}

#[async_trait::async_trait]
impl UserDirectory for InMemoryUserStore {
    async fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        if self.users.read().contains_key(username) {
            anyhow::bail!("user '{username}' already exists");
        }
        if let Some(store) = &self.persistence {
            store.create_user(username, password).await?;
        }
        // Hashed independently of the durable store's own hash: both verify the same password, and
        // this keeps `UserDirectory` free of a hash-passing method. Argon2 is deliberately slow, but
        // CREATE USER is rare and off every hot path.
        let password_hash = hash_password(password)?;
        self.users.write().insert(
            username.to_string(),
            UserRecord {
                password_hash,
                roles: HashSet::new(),
            },
        );
        Ok(())
    }

    async fn drop_user(&self, username: &str) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        self.assert_user_exists(username)?;
        if let Some(store) = &self.persistence {
            store.drop_user(username).await?;
        }
        self.users.write().remove(username);
        Ok(())
    }

    async fn grant_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        self.assert_user_exists(username)?;
        if let Some(store) = &self.persistence {
            store.grant_role(username, role).await?;
        }
        self.with_user(username, |record| {
            record.roles.insert(role.to_string());
        })
    }

    async fn revoke_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        self.assert_user_exists(username)?;
        if let Some(store) = &self.persistence {
            store.revoke_role(username, role).await?;
        }
        self.with_user(username, |record| {
            record.roles.remove(role);
        })
    }

    async fn user_exists(&self, username: &str) -> bool {
        self.users.read().contains_key(username)
    }

    async fn list_users(&self) -> anyhow::Result<Vec<crate::provider::UserRecord>> {
        let users = self.users.read();
        let mut out: Vec<crate::provider::UserRecord> = users
            .iter()
            .map(|(username, record)| {
                let mut roles: Vec<String> = record.roles.iter().cloned().collect();
                roles.sort();
                crate::provider::UserRecord {
                    username: username.clone(),
                    roles,
                }
            })
            .collect();
        out.sort_by(|a, b| a.username.cmp(&b.username));
        Ok(out)
    }

    /// Delegates to the inherent [`hydrate`](InMemoryUserStore::hydrate) so this working copy can be
    /// rehydrated through a `dyn UserDirectory` handle (how [`AuthContext`](crate::AuthContext)
    /// reaches it at startup).
    async fn hydrate(&self) -> anyhow::Result<()> {
        InMemoryUserStore::hydrate(self).await
    }

    async fn load_users(&self) -> anyhow::Result<Vec<StoredUser>> {
        let users = self.users.read();
        let mut out: Vec<StoredUser> = users
            .iter()
            .map(|(username, record)| {
                let mut roles: Vec<String> = record.roles.iter().cloned().collect();
                roles.sort();
                StoredUser {
                    username: username.clone(),
                    password_hash: record.password_hash.clone(),
                    roles,
                }
            })
            .collect();
        out.sort_by(|a, b| a.username.cmp(&b.username));
        Ok(out)
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

    /// Builds a provider around an existing store, so the caller keeps a handle to hydrate it once
    /// its durable backend is reachable.
    pub fn with_user_store(store: Arc<InMemoryUserStore>) -> Self {
        Self { store }
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
        dir.create_user("alice", "secret").await.unwrap();
        dir.grant_role("alice", "reader").await.unwrap();

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
            .await
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

    #[tokio::test]
    async fn user_crud_and_role_assignment() {
        let store = InMemoryUserStore::new();
        store.create_user("alice", "secret").await.unwrap();
        assert!(store.user_exists("alice").await);
        assert!(store.create_user("alice", "again").await.is_err());

        store.grant_role("alice", "reader").await.unwrap();
        store.grant_role("alice", "writer").await.unwrap();
        store.revoke_role("alice", "writer").await.unwrap();
        assert_eq!(store.verify("alice", "secret").unwrap(), vec!["reader".to_string()]);

        store.drop_user("alice").await.unwrap();
        assert!(!store.user_exists("alice").await);
        assert!(store.drop_user("alice").await.is_err());
        assert!(store.grant_role("ghost", "reader").await.is_err());
    }

    /// The working copy writes every mutation through and can be rebuilt from the backend alone —
    /// the property the tables-backed store relies on across restarts.
    #[tokio::test]
    async fn mutations_write_through_and_rehydrate() {
        let durable: Arc<InMemoryUserStore> = Arc::new(InMemoryUserStore::new());

        let store = InMemoryUserStore::with_store(durable.clone());
        store.create_user("alice", "secret").await.unwrap();
        store.grant_role("alice", "reader").await.unwrap();
        store.create_user("bob", "pw").await.unwrap();
        store.drop_user("bob").await.unwrap();

        assert!(durable.user_exists("alice").await);
        assert!(!durable.user_exists("bob").await);

        // A fresh working copy over the same backend sees the same users, roles and passwords.
        let reloaded = InMemoryUserStore::with_store(durable);
        reloaded.hydrate().await.unwrap();
        assert_eq!(reloaded.verify("alice", "secret").unwrap(), vec!["reader".to_string()]);
        assert!(reloaded.verify("alice", "wrong").is_err());
        assert!(!reloaded.user_exists("bob").await);
    }
}
