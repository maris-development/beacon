//! Beacon-owned authorization model: roles, privileges, and the deny-wins evaluator.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::Arc,
};

use glob::{MatchOptions, Pattern};
use parking_lot::RwLock;

/// A SQL-style privilege that can be granted to or denied from a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    All,
}

impl Display for Privilege {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Update => "UPDATE",
            Privilege::Delete => "DELETE",
            Privilege::Create => "CREATE",
            Privilege::Drop => "DROP",
            Privilege::All => "ALL",
        };
        f.write_str(s)
    }
}

impl FromStr for Privilege {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "SELECT" => Ok(Privilege::Select),
            "INSERT" => Ok(Privilege::Insert),
            "UPDATE" => Ok(Privilege::Update),
            "DELETE" => Ok(Privilege::Delete),
            "CREATE" => Ok(Privilege::Create),
            "DROP" => Ok(Privilege::Drop),
            "ALL" => Ok(Privilege::All),
            other => Err(format!("unknown privilege '{other}'")),
        }
    }
}

/// The target a privilege rule applies to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PrivilegeTarget {
    Table(String),
    Path(String),
    All,
}

impl Display for PrivilegeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrivilegeTarget::Table(name) => write!(f, "TABLE {name}"),
            PrivilegeTarget::Path(pattern) => write!(f, "PATH '{pattern}'"),
            PrivilegeTarget::All => write!(f, "ALL"),
        }
    }
}

/// A single grant/deny rule: a privilege, optionally scoped to a target.
///
/// `target == None` means the rule applies to every target for that privilege.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrivilegeRule {
    pub privilege: Privilege,
    pub target: Option<PrivilegeTarget>,
}

impl PrivilegeRule {
    pub fn new(privilege: Privilege, target: Option<PrivilegeTarget>) -> Self {
        Self { privilege, target }
    }

    /// Whether this rule matches a concrete access request.
    fn matches(&self, privilege: Privilege, target: &ConcreteTarget) -> bool {
        let privilege_matches = self.privilege == privilege || self.privilege == Privilege::All;
        if !privilege_matches {
            return false;
        }

        match &self.target {
            None | Some(PrivilegeTarget::All) => true,
            Some(PrivilegeTarget::Table(name)) => {
                matches!(target, ConcreteTarget::Table(requested) if requested == name)
            }
            Some(PrivilegeTarget::Path(pattern)) => {
                matches!(target, ConcreteTarget::Path(path) if path_matches(pattern, path))
            }
        }
    }
}

/// A concrete resource being accessed, used by the evaluator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcreteTarget {
    Table(String),
    Path(String),
}

/// A named role holding a set of grant and deny rules.
#[derive(Debug, Clone, Default)]
pub struct Role {
    pub name: String,
    pub grants: HashSet<PrivilegeRule>,
    pub denies: HashSet<PrivilegeRule>,
}

impl Role {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            grants: HashSet::new(),
            denies: HashSet::new(),
        }
    }
}

/// Durable backend for the role model. Implemented by a persistent store (the internal
/// `__beacon_roles` / `__beacon_role_rules` tables) so role/grant changes survive restarts.
/// Mutations are written through after the in-memory state is validated;
/// [`load_roles`](RoleStore::load_roles) hydrates the cache at startup.
#[async_trait::async_trait]
pub trait RoleStore: std::fmt::Debug + Send + Sync {
    /// Loads all roles and their grant/deny rules from durable storage.
    async fn load_roles(&self) -> anyhow::Result<HashMap<String, Role>>;
    async fn persist_create_role(&self, name: &str) -> anyhow::Result<()>;
    async fn persist_drop_role(&self, name: &str) -> anyhow::Result<()>;
    async fn persist_insert_rule(
        &self,
        role: &str,
        is_deny: bool,
        rule: &PrivilegeRule,
    ) -> anyhow::Result<()>;
    async fn persist_remove_rule(
        &self,
        role: &str,
        is_deny: bool,
        rule: &PrivilegeRule,
    ) -> anyhow::Result<()>;
}

/// In-memory registry of roles, with interior mutability for SQL-driven management.
///
/// When a [`RoleStore`] is attached the in-memory map is hydrated from durable storage
/// ([`hydrate`](RoleProvider::hydrate)) and every mutation is written through. The default
/// constructor has no backend (used by tests and ephemeral contexts).
#[derive(Debug, Default)]
pub struct RoleProvider {
    roles: RwLock<HashMap<String, Role>>,
    persistence: Option<Arc<dyn RoleStore>>,
    /// Serializes mutations so each validate -> persist -> apply sequence is atomic.
    ///
    /// The `roles` guard cannot span the persist `.await` (a `parking_lot` guard is `!Send`), so it
    /// is taken only for the validate and apply steps; this async mutex closes the window between
    /// them. Reads (`is_allowed`) never take it and stay lock-free of the write path.
    write_lock: futures::lock::Mutex<()>,
}

impl RoleProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attaches `store` without reading it: every later mutation is written through, but the
    /// in-memory map stays empty until [`hydrate`](RoleProvider::hydrate) runs.
    ///
    /// Split from hydration because the tables-backed store reaches its tables through the session,
    /// which does not exist yet when the auth context is built (see `runtime_builder`).
    pub fn with_store(store: Arc<dyn RoleStore>) -> Self {
        Self {
            roles: RwLock::new(HashMap::new()),
            persistence: Some(store),
            write_lock: futures::lock::Mutex::new(()),
        }
    }

    /// Builds a role provider hydrated from `store`, writing every later mutation through to it.
    pub async fn with_persistence(store: Arc<dyn RoleStore>) -> anyhow::Result<Self> {
        let provider = Self::with_store(store);
        provider.hydrate().await?;
        Ok(provider)
    }

    /// Replaces the in-memory map with the durable store's contents. No-op without a store.
    pub async fn hydrate(&self) -> anyhow::Result<()> {
        let Some(store) = &self.persistence else {
            return Ok(());
        };
        let _write = self.write_lock.lock().await;
        let loaded = store.load_roles().await?;
        *self.roles.write() = loaded;
        Ok(())
    }

    pub fn role_exists(&self, name: &str) -> bool {
        self.roles.read().contains_key(name)
    }

    pub async fn create_role(&self, name: &str) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        if self.roles.read().contains_key(name) {
            anyhow::bail!("role '{name}' already exists");
        }
        if let Some(store) = &self.persistence {
            store.persist_create_role(name).await?;
        }
        self.roles.write().insert(name.to_string(), Role::new(name));
        Ok(())
    }

    pub async fn drop_role(&self, name: &str) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        if !self.roles.read().contains_key(name) {
            anyhow::bail!("role '{name}' does not exist");
        }
        if let Some(store) = &self.persistence {
            store.persist_drop_role(name).await?;
        }
        self.roles.write().remove(name);
        Ok(())
    }

    pub async fn grant(&self, role: &str, rule: PrivilegeRule) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        self.assert_role_exists(role)?;
        if let Some(store) = &self.persistence {
            store.persist_insert_rule(role, false, &rule).await?;
        }
        self.with_role(role, |entry| {
            entry.grants.insert(rule);
        })
    }

    pub async fn deny(&self, role: &str, rule: PrivilegeRule) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        self.assert_role_exists(role)?;
        if let Some(store) = &self.persistence {
            store.persist_insert_rule(role, true, &rule).await?;
        }
        self.with_role(role, |entry| {
            entry.denies.insert(rule);
        })
    }

    /// Removes a grant (`is_deny == false`) or deny (`is_deny == true`) rule from a role.
    pub async fn revoke(
        &self,
        role: &str,
        rule: &PrivilegeRule,
        is_deny: bool,
    ) -> anyhow::Result<()> {
        let _write = self.write_lock.lock().await;
        self.assert_role_exists(role)?;
        if let Some(store) = &self.persistence {
            store.persist_remove_rule(role, is_deny, rule).await?;
        }
        self.with_role(role, |entry| {
            if is_deny {
                entry.denies.remove(rule);
            } else {
                entry.grants.remove(rule);
            }
        })
    }

    fn assert_role_exists(&self, role: &str) -> anyhow::Result<()> {
        if !self.roles.read().contains_key(role) {
            anyhow::bail!("role '{role}' does not exist");
        }
        Ok(())
    }

    /// Applies `apply` to a role under a short-lived write guard. Callers hold `write_lock`, so the
    /// role cannot have disappeared since `assert_role_exists`.
    fn with_role(&self, role: &str, apply: impl FnOnce(&mut Role)) -> anyhow::Result<()> {
        let mut roles = self.roles.write();
        let entry = roles
            .get_mut(role)
            .ok_or_else(|| anyhow::anyhow!("role '{role}' does not exist"))?;
        apply(entry);
        Ok(())
    }

    /// Snapshot of all roles with their grant/deny rules, ordered by name.
    pub fn list_roles(&self) -> Vec<Role> {
        let mut roles: Vec<Role> = self.roles.read().values().cloned().collect();
        roles.sort_by(|a, b| a.name.cmp(&b.name));
        roles
    }

    /// Whether any of the given roles grants a global `ALL` privilege (i.e. super-user).
    pub fn has_global_all_grant(&self, roles: &[String]) -> bool {
        let registry = self.roles.read();
        roles.iter().any(|role_name| {
            registry.get(role_name).is_some_and(|role| {
                role.grants.iter().any(|rule| {
                    rule.privilege == Privilege::All
                        && matches!(rule.target, None | Some(PrivilegeTarget::All))
                })
            })
        })
    }

    /// Evaluates whether the given roles are allowed to perform `privilege` on `target`.
    ///
    /// Deny rules win over grant rules; absent any matching grant, access is denied.
    pub fn is_allowed(
        &self,
        roles: &[String],
        privilege: Privilege,
        target: &ConcreteTarget,
    ) -> bool {
        let registry = self.roles.read();
        let matched: Vec<&Role> = roles
            .iter()
            .filter_map(|name| registry.get(name))
            .collect();

        let denied = matched
            .iter()
            .any(|role| role.denies.iter().any(|rule| rule.matches(privilege, target)));
        if denied {
            return false;
        }

        matched
            .iter()
            .any(|role| role.grants.iter().any(|rule| rule.matches(privilege, target)))
    }
}

/// The `kind` column value for a grant/deny rule.
pub fn rule_kind(is_deny: bool) -> &'static str {
    if is_deny {
        "deny"
    } else {
        "grant"
    }
}

/// Encodes a rule target into `(target_type, target_value)` columns. `None` (rule applies to every
/// target) is stored as the `"none"` sentinel rather than NULL so the rule's identity stays a plain
/// column tuple and dedupes.
pub fn encode_target(target: &Option<PrivilegeTarget>) -> (&'static str, String) {
    match target {
        None => ("none", String::new()),
        Some(PrivilegeTarget::All) => ("all", String::new()),
        Some(PrivilegeTarget::Table(name)) => ("table", name.clone()),
        Some(PrivilegeTarget::Path(pattern)) => ("path", pattern.clone()),
    }
}

/// Inverse of [`encode_target`].
pub fn decode_target(
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

/// Segment-aware glob match: `*` does not cross `/`, so `example/*` does not match
/// `example_2/file.parquet` nor `example/sub/file.parquet`.
fn path_matches(pattern: &str, path: &str) -> bool {
    let options = MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    };
    match Pattern::new(pattern) {
        Ok(compiled) => compiled.matches_with(path, options),
        Err(_) => pattern == path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(p: &str) -> ConcreteTarget {
        ConcreteTarget::Path(p.to_string())
    }
    fn table(t: &str) -> ConcreteTarget {
        ConcreteTarget::Table(t.to_string())
    }

    #[tokio::test]
    async fn create_and_drop_role() {
        let provider = RoleProvider::new();
        provider.create_role("reader").await.unwrap();
        assert!(provider.role_exists("reader"));
        assert!(provider.create_role("reader").await.is_err());
        provider.drop_role("reader").await.unwrap();
        assert!(!provider.role_exists("reader"));
        assert!(provider.drop_role("reader").await.is_err());
    }

    #[tokio::test]
    async fn grant_requires_existing_role() {
        let provider = RoleProvider::new();
        assert!(provider
            .grant("missing", PrivilegeRule::new(Privilege::Select, None))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn global_grant_allows_any_target() {
        let provider = RoleProvider::new();
        provider.create_role("reader").await.unwrap();
        provider
            .grant("reader", PrivilegeRule::new(Privilege::Select, None))
            .await
            .unwrap();
        let roles = vec!["reader".to_string()];
        assert!(provider.is_allowed(&roles, Privilege::Select, &path("example/file.parquet")));
        assert!(provider.is_allowed(&roles, Privilege::Select, &table("observations")));
        assert!(!provider.is_allowed(&roles, Privilege::Insert, &table("observations")));
    }

    #[tokio::test]
    async fn deny_wins_over_grant() {
        let provider = RoleProvider::new();
        provider.create_role("reader").await.unwrap();
        provider
            .grant("reader", PrivilegeRule::new(Privilege::Select, None))
            .await
            .unwrap();
        provider
            .deny(
                "reader",
                PrivilegeRule::new(
                    Privilege::Select,
                    Some(PrivilegeTarget::Path("example/*".to_string())),
                ),
            )
            .await
            .unwrap();

        let roles = vec!["reader".to_string()];
        assert!(!provider.is_allowed(&roles, Privilege::Select, &path("example/file.parquet")));
        assert!(provider.is_allowed(&roles, Privilege::Select, &path("example_2/file.parquet")));
    }

    #[test]
    fn path_matching_is_segment_aware() {
        assert!(path_matches("example/*", "example/file.parquet"));
        assert!(!path_matches("example/*", "example_2/file.parquet"));
        assert!(!path_matches("example/*", "example/sub/file.parquet"));
        assert!(path_matches("example/**", "example/sub/file.parquet"));
    }

    #[tokio::test]
    async fn privilege_all_grants_everything() {
        let provider = RoleProvider::new();
        provider.create_role("admin").await.unwrap();
        provider
            .grant("admin", PrivilegeRule::new(Privilege::All, None))
            .await
            .unwrap();
        let roles = vec!["admin".to_string()];
        assert!(provider.is_allowed(&roles, Privilege::Drop, &table("observations")));
        assert!(provider.is_allowed(&roles, Privilege::Insert, &path("any/where.parquet")));
        assert!(provider.has_global_all_grant(&roles));
    }

    #[tokio::test]
    async fn table_target_is_scoped() {
        let provider = RoleProvider::new();
        provider.create_role("writer").await.unwrap();
        provider
            .grant(
                "writer",
                PrivilegeRule::new(
                    Privilege::Insert,
                    Some(PrivilegeTarget::Table("observations".to_string())),
                ),
            )
            .await
            .unwrap();
        let roles = vec!["writer".to_string()];
        assert!(provider.is_allowed(&roles, Privilege::Insert, &table("observations")));
        assert!(!provider.is_allowed(&roles, Privilege::Insert, &table("other")));
    }

    #[tokio::test]
    async fn revoke_removes_grant_and_deny() {
        let provider = RoleProvider::new();
        provider.create_role("reader").await.unwrap();
        let grant = PrivilegeRule::new(Privilege::Select, None);
        let deny = PrivilegeRule::new(
            Privilege::Select,
            Some(PrivilegeTarget::Path("example/*".to_string())),
        );
        provider.grant("reader", grant.clone()).await.unwrap();
        provider.deny("reader", deny.clone()).await.unwrap();

        let roles = vec!["reader".to_string()];
        assert!(!provider.is_allowed(&roles, Privilege::Select, &path("example/f.parquet")));

        provider.revoke("reader", &deny, true).await.unwrap();
        assert!(provider.is_allowed(&roles, Privilege::Select, &path("example/f.parquet")));

        provider.revoke("reader", &grant, false).await.unwrap();
        assert!(!provider.is_allowed(&roles, Privilege::Select, &path("anything.parquet")));
    }

    #[tokio::test]
    async fn default_deny_without_rules() {
        let provider = RoleProvider::new();
        provider.create_role("empty").await.unwrap();
        let roles = vec!["empty".to_string()];
        assert!(!provider.is_allowed(&roles, Privilege::Select, &table("x")));
    }

    #[test]
    fn target_round_trips_through_columns() {
        for target in [
            None,
            Some(PrivilegeTarget::All),
            Some(PrivilegeTarget::Table("obs".to_string())),
            Some(PrivilegeTarget::Path("example/*".to_string())),
        ] {
            let (target_type, target_value) = encode_target(&target);
            assert_eq!(decode_target(target_type, target_value).unwrap(), target);
        }
        assert!(decode_target("bogus", String::new()).is_err());
    }
}
