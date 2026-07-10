//! SQLite-backed persistence for the Beacon auth directory.
//!
//! [`SqliteStore`] is the durable source of truth for users (username + Argon2 password hash +
//! role assignments) and for the role model (roles + grant/deny privilege rules). It implements
//! both [`UserDirectory`] (user lifecycle) and [`RoleStore`] (write-through persistence for the
//! in-memory [`RoleProvider`](crate::role::RoleProvider)).
//!
//! The in-memory caches used at request time are hydrated from this store at startup and written
//! through on every mutation, so authorization evaluation stays fast while all state survives
//! restarts.

use std::{collections::HashMap, path::Path, str::FromStr, sync::Arc};

use futures::future::BoxFuture;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};

use crate::{
    credential::Credential,
    password::{hash_password, verify_password},
    provider::{AuthProvider, Authenticated, UserDirectory, UserRecord},
    role::{Privilege, PrivilegeRule, PrivilegeTarget, Role, RoleStore},
};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS users (
    username      TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS user_roles (
    username TEXT NOT NULL,
    role     TEXT NOT NULL,
    PRIMARY KEY (username, role)
);
CREATE TABLE IF NOT EXISTS roles (
    name TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS role_rules (
    role         TEXT NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('grant', 'deny')),
    privilege    TEXT NOT NULL,
    target_type  TEXT NOT NULL,
    target_value TEXT NOT NULL,
    PRIMARY KEY (role, kind, privilege, target_type, target_value)
);
"#;

/// Durable auth directory backed by a single SQLite database.
///
/// Shared across requests via `Arc`. A single connection is serialized behind a `Mutex`; the auth
/// workload (logins and admin DDL) is low-frequency, so this is simpler than a connection pool.
pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStore").finish_non_exhaustive()
    }
}

impl SqliteStore {
    /// Opens (creating if needed) the SQLite directory database at `path` and runs migrations.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    /// Opens an in-memory database (used by tests).
    pub fn open_in_memory() -> anyhow::Result<Arc<Self>> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> anyhow::Result<Arc<Self>> {
        // Wait (rather than failing immediately) when another connection to the same database file
        // holds a lock, e.g. a second runtime instance opening the store concurrently.
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Arc::new(Self {
            conn: Mutex::new(conn),
        }))
    }

    /// Returns the principal's role names after verifying their password.
    pub fn verify(&self, username: &str, password: &str) -> anyhow::Result<Vec<String>> {
        let hash = {
            let conn = self.conn.lock();
            conn.query_row(
                "SELECT password_hash FROM users WHERE username = ?1",
                params![username],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        }
        .ok_or_else(|| anyhow::anyhow!("authentication failed"))?;

        if !verify_password(&hash, password) {
            anyhow::bail!("authentication failed");
        }
        self.roles_of(username)
    }

    fn roles_of(&self, username: &str) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT role FROM user_roles WHERE username = ?1")?;
        let roles = stmt
            .query_map(params![username], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(roles)
    }

    fn assert_user_exists(conn: &Connection, username: &str) -> anyhow::Result<()> {
        let exists = conn
            .query_row(
                "SELECT 1 FROM users WHERE username = ?1",
                params![username],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            anyhow::bail!("user '{username}' does not exist");
        }
        Ok(())
    }
}

impl UserDirectory for SqliteStore {
    fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        let password_hash = hash_password(password)?;
        let conn = self.conn.lock();
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO users (username, password_hash) VALUES (?1, ?2)",
            params![username, password_hash],
        )?;
        if inserted == 0 {
            anyhow::bail!("user '{username}' already exists");
        }
        Ok(())
    }

    fn drop_user(&self, username: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM user_roles WHERE username = ?1",
            params![username],
        )?;
        let removed = conn.execute("DELETE FROM users WHERE username = ?1", params![username])?;
        if removed == 0 {
            anyhow::bail!("user '{username}' does not exist");
        }
        Ok(())
    }

    fn grant_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock();
        Self::assert_user_exists(&conn, username)?;
        conn.execute(
            "INSERT OR IGNORE INTO user_roles (username, role) VALUES (?1, ?2)",
            params![username, role],
        )?;
        Ok(())
    }

    fn revoke_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock();
        Self::assert_user_exists(&conn, username)?;
        conn.execute(
            "DELETE FROM user_roles WHERE username = ?1 AND role = ?2",
            params![username, role],
        )?;
        Ok(())
    }

    fn user_exists(&self, username: &str) -> bool {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT 1 FROM users WHERE username = ?1",
            params![username],
            |_| Ok(()),
        )
        .optional()
        .map(|row| row.is_some())
        .unwrap_or(false)
    }

    fn list_users(&self) -> anyhow::Result<Vec<UserRecord>> {
        let conn = self.conn.lock();
        let usernames = conn
            .prepare("SELECT username FROM users ORDER BY username")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let pairs = conn
            .prepare("SELECT username, role FROM user_roles")?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<(String, String)>, _>>()?;

        let mut by_user: HashMap<String, Vec<String>> = HashMap::new();
        for (username, role) in pairs {
            by_user.entry(username).or_default().push(role);
        }
        Ok(usernames
            .into_iter()
            .map(|username| {
                let mut roles = by_user.remove(&username).unwrap_or_default();
                roles.sort();
                UserRecord { username, roles }
            })
            .collect())
    }
}

impl RoleStore for SqliteStore {
    fn load_roles(&self) -> anyhow::Result<HashMap<String, Role>> {
        let conn = self.conn.lock();
        let mut roles: HashMap<String, Role> = HashMap::new();

        let mut role_stmt = conn.prepare("SELECT name FROM roles")?;
        let names = role_stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for name in names {
            roles.insert(name.clone(), Role::new(name));
        }

        let mut rule_stmt = conn
            .prepare("SELECT role, kind, privilege, target_type, target_value FROM role_rules")?;
        let rows = rule_stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        for (role, kind, privilege, target_type, target_value) in rows {
            let Some(entry) = roles.get_mut(&role) else {
                continue;
            };
            let privilege = Privilege::from_str(&privilege).map_err(|err| anyhow::anyhow!(err))?;
            let rule = PrivilegeRule::new(privilege, decode_target(&target_type, target_value)?);
            if kind == "deny" {
                entry.denies.insert(rule);
            } else {
                entry.grants.insert(rule);
            }
        }

        Ok(roles)
    }

    fn persist_create_role(&self, name: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT OR IGNORE INTO roles (name) VALUES (?1)",
            params![name],
        )?;
        Ok(())
    }

    fn persist_drop_role(&self, name: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM role_rules WHERE role = ?1", params![name])?;
        conn.execute("DELETE FROM roles WHERE name = ?1", params![name])?;
        Ok(())
    }

    fn persist_insert_rule(
        &self,
        role: &str,
        is_deny: bool,
        rule: &PrivilegeRule,
    ) -> anyhow::Result<()> {
        let (target_type, target_value) = encode_target(&rule.target);
        let conn = self.conn.lock();
        conn.execute(
            "INSERT OR IGNORE INTO role_rules (role, kind, privilege, target_type, target_value) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                role,
                kind(is_deny),
                rule.privilege.to_string(),
                target_type,
                target_value
            ],
        )?;
        Ok(())
    }

    fn persist_remove_rule(
        &self,
        role: &str,
        is_deny: bool,
        rule: &PrivilegeRule,
    ) -> anyhow::Result<()> {
        let (target_type, target_value) = encode_target(&rule.target);
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM role_rules WHERE role = ?1 AND kind = ?2 AND privilege = ?3 \
             AND target_type = ?4 AND target_value = ?5",
            params![
                role,
                kind(is_deny),
                rule.privilege.to_string(),
                target_type,
                target_value
            ],
        )?;
        Ok(())
    }
}

/// Authentication provider backed by a [`SqliteStore`]. Handles [`Credential::Basic`].
#[derive(Debug, Clone)]
pub struct SqliteAuthProvider {
    store: Arc<SqliteStore>,
}

impl SqliteAuthProvider {
    pub fn new(store: Arc<SqliteStore>) -> Self {
        Self { store }
    }
}

impl AuthProvider for SqliteAuthProvider {
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
                    anyhow::bail!("sqlite auth provider does not accept bearer credentials")
                }
            }
        })
    }

    fn user_directory(&self) -> Option<Arc<dyn UserDirectory>> {
        Some(self.store.clone() as Arc<dyn UserDirectory>)
    }
}

fn kind(is_deny: bool) -> &'static str {
    if is_deny {
        "deny"
    } else {
        "grant"
    }
}

/// Encodes a rule target into `(target_type, target_value)` columns. `None` (rule applies to every
/// target) is stored as the `"none"` sentinel rather than SQL NULL so the rule primary key dedupes.
fn encode_target(target: &Option<PrivilegeTarget>) -> (&'static str, String) {
    match target {
        None => ("none", String::new()),
        Some(PrivilegeTarget::All) => ("all", String::new()),
        Some(PrivilegeTarget::Table(name)) => ("table", name.clone()),
        Some(PrivilegeTarget::Path(pattern)) => ("path", pattern.clone()),
    }
}

fn decode_target(
    target_type: &str,
    target_value: String,
) -> anyhow::Result<Option<PrivilegeTarget>> {
    Ok(match target_type {
        "none" => None,
        "all" => Some(PrivilegeTarget::All),
        "table" => Some(PrivilegeTarget::Table(target_value)),
        "path" => Some(PrivilegeTarget::Path(target_value)),
        other => anyhow::bail!("unknown target_type '{other}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::role::{ConcreteTarget, RoleProvider};

    #[test]
    fn user_crud_and_role_assignment() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.create_user("bob", "pw").unwrap();
        assert!(store.user_exists("bob"));
        assert!(store.create_user("bob", "pw").is_err());

        store.grant_role("bob", "writer").unwrap();
        assert_eq!(
            store.verify("bob", "pw").unwrap(),
            vec!["writer".to_string()]
        );
        assert!(store.verify("bob", "wrong").is_err());

        store.revoke_role("bob", "writer").unwrap();
        assert!(store.verify("bob", "pw").unwrap().is_empty());

        store.drop_user("bob").unwrap();
        assert!(!store.user_exists("bob"));
        assert!(store.drop_user("bob").is_err());
        assert!(store.grant_role("ghost", "writer").is_err());
    }

    #[tokio::test]
    async fn provider_authenticates_against_store() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.create_user("alice", "secret").unwrap();
        store.grant_role("alice", "reader").unwrap();
        let provider = SqliteAuthProvider::new(store);

        let authed = provider
            .authenticate(&Credential::basic("alice", "secret"))
            .await
            .unwrap();
        assert_eq!(authed.roles, vec!["reader".to_string()]);
        assert!(provider
            .authenticate(&Credential::basic("alice", "wrong"))
            .await
            .is_err());
        assert!(provider
            .authenticate(&Credential::bearer("a.b.c"))
            .await
            .is_err());
    }

    #[test]
    fn role_store_round_trip_through_provider() {
        let store = SqliteStore::open_in_memory().unwrap();
        let roles = RoleProvider::with_persistence(store.clone()).unwrap();
        roles.create_role("reader").unwrap();
        roles
            .grant("reader", PrivilegeRule::new(Privilege::Select, None))
            .unwrap();
        roles
            .deny(
                "reader",
                PrivilegeRule::new(
                    Privilege::Select,
                    Some(PrivilegeTarget::Path("example/*".to_string())),
                ),
            )
            .unwrap();

        // Re-hydrate a fresh provider from the same store and confirm the rules survived.
        let reloaded = RoleProvider::with_persistence(store).unwrap();
        let r = vec!["reader".to_string()];
        assert!(reloaded.is_allowed(
            &r,
            Privilege::Select,
            &ConcreteTarget::Path("other/file.parquet".to_string())
        ));
        assert!(!reloaded.is_allowed(
            &r,
            Privilege::Select,
            &ConcreteTarget::Path("example/file.parquet".to_string())
        ));
    }

    #[test]
    fn persistence_survives_reopen_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("directory.db");

        {
            let store = SqliteStore::open(&path).unwrap();
            store.create_user("alice", "secret").unwrap();
            let roles = RoleProvider::with_persistence(store.clone()).unwrap();
            roles.create_role("reader").unwrap();
            roles
                .grant(
                    "reader",
                    PrivilegeRule::new(
                        Privilege::Select,
                        Some(PrivilegeTarget::Table("obs".to_string())),
                    ),
                )
                .unwrap();
            store.grant_role("alice", "reader").unwrap();
        }

        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(store.verify("alice", "secret").unwrap(), vec!["reader"]);
        let roles = RoleProvider::with_persistence(store).unwrap();
        assert!(roles.is_allowed(
            &["reader".to_string()],
            Privilege::Select,
            &ConcreteTarget::Table("obs".to_string())
        ));
    }
}
