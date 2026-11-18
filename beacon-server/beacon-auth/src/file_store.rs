//! The auth state of a server as one JSON file: users, password hashes, roles, rules and
//! role settings.
//!
//! [`FileAuthStore`] is the durable backend of the in-memory working copies
//! ([`InMemoryUserStore`](crate::InMemoryUserStore) and [`RoleProvider`](crate::RoleProvider)).
//! Those copies answer each login and each access check. The file is read once at startup and
//! written on each change.
//!
//! A change writes a temporary file and renames it over the store, so a crash never leaves half
//! a file. On Unix only the owner can read the file, because it holds password hashes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::password::hash_password;
use crate::provider::{StoredUser, UserDirectory, UserRecord};
use crate::role::{
    decode_target, encode_target, normalize_setting, Privilege, PrivilegeRule, Role, RoleStore,
};

/// The format version that this build writes.
const FORMAT_VERSION: u32 = 1;

/// The content of the store file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AuthFile {
    version: u32,
    /// Sorted maps keep the file stable between writes.
    #[serde(default)]
    users: BTreeMap<String, FileUser>,
    #[serde(default)]
    roles: BTreeMap<String, FileRole>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FileUser {
    password_hash: String,
    #[serde(default)]
    roles: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FileRole {
    #[serde(default)]
    grants: Vec<FileRule>,
    #[serde(default)]
    denies: Vec<FileRule>,
    #[serde(default)]
    settings: BTreeMap<String, String>,
}

impl FileRole {
    /// The grant rules or the deny rules of the role.
    fn rules_mut(&mut self, is_deny: bool) -> &mut Vec<FileRule> {
        if is_deny {
            &mut self.denies
        } else {
            &mut self.grants
        }
    }
}

/// One rule in the encoding of [`encode_target`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileRule {
    privilege: String,
    target_type: String,
    #[serde(default)]
    target_value: String,
}

impl From<&PrivilegeRule> for FileRule {
    fn from(rule: &PrivilegeRule) -> Self {
        let (target_type, target_value) = encode_target(&rule.target);
        Self {
            privilege: rule.privilege.to_string(),
            target_type: target_type.to_string(),
            target_value,
        }
    }
}

impl TryFrom<&FileRule> for PrivilegeRule {
    type Error = anyhow::Error;

    fn try_from(rule: &FileRule) -> anyhow::Result<Self> {
        let privilege = Privilege::from_str(&rule.privilege).map_err(|e| anyhow::anyhow!(e))?;
        let target = decode_target(&rule.target_type, rule.target_value.clone())?;
        Ok(PrivilegeRule::new(privilege, target))
    }
}

/// The durable auth store of a server: one JSON file.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> anyhow::Result<()> {
/// use beacon_auth::{FileAuthStore, UserDirectory};
///
/// let store = FileAuthStore::open("data/beacon-auth.json").await?;
/// store.create_user("alice", "secret").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct FileAuthStore {
    path: PathBuf,
    /// The content of the file. The lock also puts the writes in sequence.
    state: tokio::sync::Mutex<AuthFile>,
}

impl FileAuthStore {
    /// Opens the store at `path`. A file that does not exist gives an empty store, and the
    /// first change creates the file.
    ///
    /// # Arguments
    ///
    /// * `path` - The store file
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, is not valid, or has a newer format
    /// version than this build writes.
    pub async fn open(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();
        let state = match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let file: AuthFile = serde_json::from_slice(&bytes)
                    .with_context(|| format!("invalid auth store {}", path.display()))?;
                if file.version > FORMAT_VERSION {
                    anyhow::bail!(
                        "auth store {} has format version {}; this build reads up to {}",
                        path.display(),
                        file.version,
                        FORMAT_VERSION
                    );
                }
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => AuthFile::default(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("cannot read auth store {}", path.display()))
            }
        };
        Ok(Self {
            path,
            state: tokio::sync::Mutex::new(state),
        })
    }

    /// The store file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Replaces the content of the store with `users` and `roles`, and writes the file.
    ///
    /// For a one-time import from another store. The password hashes go in as they are.
    ///
    /// # Arguments
    ///
    /// * `users` - Each user with its password hash and role names
    /// * `roles` - Each role by name
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be written.
    pub async fn import(
        &self,
        users: Vec<StoredUser>,
        roles: HashMap<String, Role>,
    ) -> anyhow::Result<()> {
        let users = users
            .into_iter()
            .map(|user| {
                let record = FileUser {
                    password_hash: user.password_hash,
                    roles: user.roles.into_iter().collect(),
                };
                (user.username, record)
            })
            .collect();
        let roles = roles
            .into_iter()
            .map(|(name, role)| {
                let record = FileRole {
                    grants: role.grants.iter().map(FileRule::from).collect(),
                    denies: role.denies.iter().map(FileRule::from).collect(),
                    settings: role.settings,
                };
                (name, record)
            })
            .collect();
        self.update(|file| {
            file.users = users;
            file.roles = roles;
            Ok(())
        })
        .await
    }

    /// Applies `change` to a copy of the content and writes it. The store keeps the copy only
    /// when the write succeeds, so the memory and the file never differ.
    async fn update(
        &self,
        change: impl FnOnce(&mut AuthFile) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.clone();
        change(&mut next)?;
        next.version = FORMAT_VERSION;
        self.write(&next).await?;
        *state = next;
        Ok(())
    }

    /// Writes `file` to a temporary file, then renames it over the store.
    async fn write(&self, file: &AuthFile) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec_pretty(file)?;
        let mut temporary = self.path.clone().into_os_string();
        temporary.push(".tmp");
        let temporary = PathBuf::from(temporary);

        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        // The file holds password hashes, so only the owner can read it.
        #[cfg(unix)]
        options.mode(0o600);
        let mut handle = options
            .open(&temporary)
            .await
            .with_context(|| format!("cannot write auth store {}", temporary.display()))?;
        handle.write_all(&bytes).await?;
        handle.sync_all().await?;
        drop(handle);

        tokio::fs::rename(&temporary, &self.path)
            .await
            .with_context(|| format!("cannot replace auth store {}", self.path.display()))
    }
}

/// The user `username` of `file`, or an error when the store has no such user.
fn user_mut<'a>(file: &'a mut AuthFile, username: &str) -> anyhow::Result<&'a mut FileUser> {
    file.users
        .get_mut(username)
        .ok_or_else(|| anyhow::anyhow!("user '{username}' does not exist"))
}

#[async_trait::async_trait]
impl UserDirectory for FileAuthStore {
    async fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        // The working copy makes its own hash. Both hashes verify the same password.
        let password_hash = hash_password(password)?;
        self.update(|file| {
            let user = file.users.entry(username.to_string()).or_default();
            user.password_hash = password_hash;
            Ok(())
        })
        .await
    }

    async fn drop_user(&self, username: &str) -> anyhow::Result<()> {
        self.update(|file| {
            file.users.remove(username);
            Ok(())
        })
        .await
    }

    async fn grant_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        self.update(|file| {
            user_mut(file, username)?.roles.insert(role.to_string());
            Ok(())
        })
        .await
    }

    async fn revoke_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        self.update(|file| {
            user_mut(file, username)?.roles.remove(role);
            Ok(())
        })
        .await
    }

    async fn user_exists(&self, username: &str) -> bool {
        self.state.lock().await.users.contains_key(username)
    }

    async fn list_users(&self) -> anyhow::Result<Vec<UserRecord>> {
        let state = self.state.lock().await;
        Ok(state
            .users
            .iter()
            .map(|(username, user)| UserRecord {
                username: username.clone(),
                roles: user.roles.iter().cloned().collect(),
            })
            .collect())
    }

    async fn load_users(&self) -> anyhow::Result<Vec<StoredUser>> {
        let state = self.state.lock().await;
        Ok(state
            .users
            .iter()
            .map(|(username, user)| StoredUser {
                username: username.clone(),
                password_hash: user.password_hash.clone(),
                roles: user.roles.iter().cloned().collect(),
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl RoleStore for FileAuthStore {
    async fn load_roles(&self) -> anyhow::Result<HashMap<String, Role>> {
        let state = self.state.lock().await;
        let mut roles = HashMap::with_capacity(state.roles.len());
        for (name, stored) in &state.roles {
            let mut role = Role::new(name.clone());
            for rule in &stored.grants {
                role.grants.insert(PrivilegeRule::try_from(rule)?);
            }
            for rule in &stored.denies {
                role.denies.insert(PrivilegeRule::try_from(rule)?);
            }
            for (key, value) in &stored.settings {
                // A setting that fails the check is skipped, so the other roles still load.
                match normalize_setting(key, value) {
                    Ok((key, value)) => {
                        role.settings.insert(key, value);
                    }
                    Err(error) => {
                        tracing::warn!(role = %name, %key, %value, ?error, "skipped a role setting");
                    }
                }
            }
            roles.insert(name.clone(), role);
        }
        Ok(roles)
    }

    async fn persist_create_role(&self, name: &str) -> anyhow::Result<()> {
        self.update(|file| {
            file.roles.entry(name.to_string()).or_default();
            Ok(())
        })
        .await
    }

    async fn persist_drop_role(&self, name: &str) -> anyhow::Result<()> {
        self.update(|file| {
            file.roles.remove(name);
            Ok(())
        })
        .await
    }

    async fn persist_insert_rule(
        &self,
        role: &str,
        is_deny: bool,
        rule: &PrivilegeRule,
    ) -> anyhow::Result<()> {
        let rule = FileRule::from(rule);
        self.update(|file| {
            let rules = file
                .roles
                .entry(role.to_string())
                .or_default()
                .rules_mut(is_deny);
            if !rules.contains(&rule) {
                rules.push(rule);
            }
            Ok(())
        })
        .await
    }

    async fn persist_remove_rule(
        &self,
        role: &str,
        is_deny: bool,
        rule: &PrivilegeRule,
    ) -> anyhow::Result<()> {
        let rule = FileRule::from(rule);
        self.update(|file| {
            if let Some(stored) = file.roles.get_mut(role) {
                stored.rules_mut(is_deny).retain(|held| held != &rule);
            }
            Ok(())
        })
        .await
    }

    async fn persist_set_setting(&self, role: &str, key: &str, value: &str) -> anyhow::Result<()> {
        self.update(|file| {
            file.roles
                .entry(role.to_string())
                .or_default()
                .settings
                .insert(key.to_string(), value.to_string());
            Ok(())
        })
        .await
    }

    async fn persist_reset_setting(&self, role: &str, key: &str) -> anyhow::Result<()> {
        self.update(|file| {
            if let Some(stored) = file.roles.get_mut(role) {
                stored.settings.remove(key);
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::password::verify_password;
    use crate::role::{PrivilegeTarget, QUERY_CPU_LIMIT_MS};

    fn select_on(table: &str) -> PrivilegeRule {
        PrivilegeRule::new(
            Privilege::Select,
            Some(PrivilegeTarget::Table(table.to_string())),
        )
    }

    #[tokio::test]
    async fn a_missing_file_gives_an_empty_store_and_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");

        let store = FileAuthStore::open(&path).await.unwrap();

        assert!(store.load_users().await.unwrap().is_empty());
        assert!(store.load_roles().await.unwrap().is_empty());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn users_and_their_roles_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let store = FileAuthStore::open(&path).await.unwrap();
        store.create_user("alice", "secret").await.unwrap();
        store.create_user("bob", "other").await.unwrap();
        store.grant_role("alice", "reader").await.unwrap();
        store.grant_role("alice", "writer").await.unwrap();
        store.revoke_role("alice", "writer").await.unwrap();
        store.drop_user("bob").await.unwrap();

        let reopened = FileAuthStore::open(&path).await.unwrap();
        let users = reopened.load_users().await.unwrap();

        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "alice");
        assert_eq!(users[0].roles, vec!["reader".to_string()]);
        assert!(verify_password(&users[0].password_hash, "secret"));
        assert!(reopened.user_exists("alice").await);
        assert!(!reopened.user_exists("bob").await);
    }

    #[tokio::test]
    async fn a_role_change_for_a_missing_user_fails() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileAuthStore::open(dir.path().join("auth.json"))
            .await
            .unwrap();

        let error = store.grant_role("nobody", "reader").await.unwrap_err();

        assert!(error.to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn roles_rules_and_settings_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let store = FileAuthStore::open(&path).await.unwrap();
        store.persist_create_role("reader").await.unwrap();
        store.persist_create_role("gone").await.unwrap();
        store
            .persist_insert_rule("reader", false, &select_on("argo"))
            .await
            .unwrap();
        store
            .persist_insert_rule("reader", false, &select_on("argo"))
            .await
            .unwrap();
        store
            .persist_insert_rule("reader", false, &select_on("removed"))
            .await
            .unwrap();
        store
            .persist_remove_rule("reader", false, &select_on("removed"))
            .await
            .unwrap();
        let deny = PrivilegeRule::new(
            Privilege::Select,
            Some(PrivilegeTarget::Path("secret/*".to_string())),
        );
        store
            .persist_insert_rule("reader", true, &deny)
            .await
            .unwrap();
        store
            .persist_set_setting("reader", QUERY_CPU_LIMIT_MS, "500")
            .await
            .unwrap();
        store
            .persist_set_setting("reader", "wms.max_tiles", "4")
            .await
            .unwrap();
        store
            .persist_reset_setting("reader", "wms.max_tiles")
            .await
            .unwrap();
        store.persist_drop_role("gone").await.unwrap();

        let roles = FileAuthStore::open(&path)
            .await
            .unwrap()
            .load_roles()
            .await
            .unwrap();

        assert_eq!(roles.len(), 1);
        let reader = &roles["reader"];
        assert_eq!(reader.grants.len(), 1);
        assert!(reader.grants.contains(&select_on("argo")));
        assert!(reader.denies.contains(&deny));
        assert_eq!(reader.settings.len(), 1);
        assert_eq!(reader.settings[QUERY_CPU_LIMIT_MS], "500");
    }

    #[tokio::test]
    async fn an_import_replaces_the_content_and_keeps_the_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let store = FileAuthStore::open(&path).await.unwrap();
        store.create_user("old", "x").await.unwrap();
        let hash = hash_password("secret").unwrap();
        let mut role = Role::new("reader");
        role.grants.insert(select_on("argo"));

        store
            .import(
                vec![StoredUser {
                    username: "alice".to_string(),
                    password_hash: hash.clone(),
                    roles: vec!["reader".to_string()],
                }],
                HashMap::from([("reader".to_string(), role)]),
            )
            .await
            .unwrap();

        let reopened = FileAuthStore::open(&path).await.unwrap();
        let users = reopened.load_users().await.unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].password_hash, hash);
        let roles = reopened.load_roles().await.unwrap();
        assert!(roles["reader"].grants.contains(&select_on("argo")));
    }

    #[tokio::test]
    async fn a_write_leaves_no_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileAuthStore::open(dir.path().join("auth.json"))
            .await
            .unwrap();

        store.persist_create_role("reader").await.unwrap();

        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from("auth.json")]);
    }

    #[tokio::test]
    async fn a_newer_format_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, r#"{"version": 99}"#).unwrap();

        let error = FileAuthStore::open(&path).await.unwrap_err();

        assert!(error.to_string().contains("format version 99"));
    }
}
