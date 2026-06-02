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

/// Durable backend for the role model. Implemented by a persistent store (e.g. the SQLite auth
/// directory) so role/grant changes survive restarts. Mutations are written through after the
/// in-memory state is validated; [`load_roles`](RoleStore::load_roles) hydrates the cache at startup.
pub trait RoleStore: std::fmt::Debug + Send + Sync {
    /// Loads all roles and their grant/deny rules from durable storage.
    fn load_roles(&self) -> anyhow::Result<HashMap<String, Role>>;
    fn persist_create_role(&self, name: &str) -> anyhow::Result<()>;
    fn persist_drop_role(&self, name: &str) -> anyhow::Result<()>;
    fn persist_insert_rule(&self, role: &str, is_deny: bool, rule: &PrivilegeRule)
        -> anyhow::Result<()>;
    fn persist_remove_rule(&self, role: &str, is_deny: bool, rule: &PrivilegeRule)
        -> anyhow::Result<()>;
}

/// In-memory registry of roles, with interior mutability for SQL-driven management.
///
/// When constructed with [`with_persistence`](RoleProvider::with_persistence) the in-memory map is
/// hydrated from durable storage and every mutation is written through. The default constructor has
/// no backend (used by tests and ephemeral contexts).
#[derive(Debug, Default)]
pub struct RoleProvider {
    roles: RwLock<HashMap<String, Role>>,
    persistence: Option<Arc<dyn RoleStore>>,
}

impl RoleProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a role provider hydrated from `store`, writing every later mutation through to it.
    pub fn with_persistence(store: Arc<dyn RoleStore>) -> anyhow::Result<Self> {
        let roles = store.load_roles()?;
        Ok(Self {
            roles: RwLock::new(roles),
            persistence: Some(store),
        })
    }

    pub fn role_exists(&self, name: &str) -> bool {
        self.roles.read().contains_key(name)
    }

    pub fn create_role(&self, name: &str) -> anyhow::Result<()> {
        let mut roles = self.roles.write();
        if roles.contains_key(name) {
            anyhow::bail!("role '{name}' already exists");
        }
        if let Some(store) = &self.persistence {
            store.persist_create_role(name)?;
        }
        roles.insert(name.to_string(), Role::new(name));
        Ok(())
    }

    pub fn drop_role(&self, name: &str) -> anyhow::Result<()> {
        let mut roles = self.roles.write();
        if !roles.contains_key(name) {
            anyhow::bail!("role '{name}' does not exist");
        }
        if let Some(store) = &self.persistence {
            store.persist_drop_role(name)?;
        }
        roles.remove(name);
        Ok(())
    }

    pub fn grant(&self, role: &str, rule: PrivilegeRule) -> anyhow::Result<()> {
        let mut roles = self.roles.write();
        let entry = roles
            .get_mut(role)
            .ok_or_else(|| anyhow::anyhow!("role '{role}' does not exist"))?;
        if let Some(store) = &self.persistence {
            store.persist_insert_rule(role, false, &rule)?;
        }
        entry.grants.insert(rule);
        Ok(())
    }

    pub fn deny(&self, role: &str, rule: PrivilegeRule) -> anyhow::Result<()> {
        let mut roles = self.roles.write();
        let entry = roles
            .get_mut(role)
            .ok_or_else(|| anyhow::anyhow!("role '{role}' does not exist"))?;
        if let Some(store) = &self.persistence {
            store.persist_insert_rule(role, true, &rule)?;
        }
        entry.denies.insert(rule);
        Ok(())
    }

    /// Removes a grant (`is_deny == false`) or deny (`is_deny == true`) rule from a role.
    pub fn revoke(&self, role: &str, rule: &PrivilegeRule, is_deny: bool) -> anyhow::Result<()> {
        let mut roles = self.roles.write();
        let entry = roles
            .get_mut(role)
            .ok_or_else(|| anyhow::anyhow!("role '{role}' does not exist"))?;
        if let Some(store) = &self.persistence {
            store.persist_remove_rule(role, is_deny, rule)?;
        }
        if is_deny {
            entry.denies.remove(rule);
        } else {
            entry.grants.remove(rule);
        }
        Ok(())
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
    pub fn is_allowed(&self, roles: &[String], privilege: Privilege, target: &ConcreteTarget) -> bool {
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

    #[test]
    fn create_and_drop_role() {
        let provider = RoleProvider::new();
        provider.create_role("reader").unwrap();
        assert!(provider.role_exists("reader"));
        assert!(provider.create_role("reader").is_err());
        provider.drop_role("reader").unwrap();
        assert!(!provider.role_exists("reader"));
        assert!(provider.drop_role("reader").is_err());
    }

    #[test]
    fn grant_requires_existing_role() {
        let provider = RoleProvider::new();
        assert!(provider
            .grant("missing", PrivilegeRule::new(Privilege::Select, None))
            .is_err());
    }

    #[test]
    fn global_grant_allows_any_target() {
        let provider = RoleProvider::new();
        provider.create_role("reader").unwrap();
        provider
            .grant("reader", PrivilegeRule::new(Privilege::Select, None))
            .unwrap();
        let roles = vec!["reader".to_string()];
        assert!(provider.is_allowed(&roles, Privilege::Select, &path("example/file.parquet")));
        assert!(provider.is_allowed(&roles, Privilege::Select, &table("observations")));
        assert!(!provider.is_allowed(&roles, Privilege::Insert, &table("observations")));
    }

    #[test]
    fn deny_wins_over_grant() {
        let provider = RoleProvider::new();
        provider.create_role("reader").unwrap();
        provider
            .grant("reader", PrivilegeRule::new(Privilege::Select, None))
            .unwrap();
        provider
            .deny(
                "reader",
                PrivilegeRule::new(
                    Privilege::Select,
                    Some(PrivilegeTarget::Path("example/*".to_string())),
                ),
            )
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

    #[test]
    fn privilege_all_grants_everything() {
        let provider = RoleProvider::new();
        provider.create_role("admin").unwrap();
        provider
            .grant("admin", PrivilegeRule::new(Privilege::All, None))
            .unwrap();
        let roles = vec!["admin".to_string()];
        assert!(provider.is_allowed(&roles, Privilege::Drop, &table("observations")));
        assert!(provider.is_allowed(&roles, Privilege::Insert, &path("any/where.parquet")));
        assert!(provider.has_global_all_grant(&roles));
    }

    #[test]
    fn table_target_is_scoped() {
        let provider = RoleProvider::new();
        provider.create_role("writer").unwrap();
        provider
            .grant(
                "writer",
                PrivilegeRule::new(
                    Privilege::Insert,
                    Some(PrivilegeTarget::Table("observations".to_string())),
                ),
            )
            .unwrap();
        let roles = vec!["writer".to_string()];
        assert!(provider.is_allowed(&roles, Privilege::Insert, &table("observations")));
        assert!(!provider.is_allowed(&roles, Privilege::Insert, &table("other")));
    }

    #[test]
    fn revoke_removes_grant_and_deny() {
        let provider = RoleProvider::new();
        provider.create_role("reader").unwrap();
        let grant = PrivilegeRule::new(Privilege::Select, None);
        let deny = PrivilegeRule::new(
            Privilege::Select,
            Some(PrivilegeTarget::Path("example/*".to_string())),
        );
        provider.grant("reader", grant.clone()).unwrap();
        provider.deny("reader", deny.clone()).unwrap();

        let roles = vec!["reader".to_string()];
        assert!(!provider.is_allowed(&roles, Privilege::Select, &path("example/f.parquet")));

        provider.revoke("reader", &deny, true).unwrap();
        assert!(provider.is_allowed(&roles, Privilege::Select, &path("example/f.parquet")));

        provider.revoke("reader", &grant, false).unwrap();
        assert!(!provider.is_allowed(&roles, Privilege::Select, &path("anything.parquet")));
    }

    #[test]
    fn default_deny_without_rules() {
        let provider = RoleProvider::new();
        provider.create_role("empty").unwrap();
        let roles = vec!["empty".to_string()];
        assert!(!provider.is_allowed(&roles, Privilege::Select, &table("x")));
    }
}
