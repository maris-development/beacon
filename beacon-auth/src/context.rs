//! The central authorization context owned by Beacon.

use std::sync::Arc;

use crate::{
    credential::Credential,
    provider::AuthProvider,
    role::{ConcreteTarget, Privilege, PrivilegeRule, PrivilegeTarget, RoleProvider},
};

/// Default username of the built-in anonymous principal.
pub const ANONYMOUS_USERNAME: &str = "anonymous";

/// The built-in super-user role.
///
/// Analogous to a Postgres bootstrap superuser: it is always present with a global `ALL` grant
/// (re-seeded on every startup) and is protected — it cannot be dropped, nor can its global `ALL`
/// grant be revoked, through the auth-management SQL. Any principal holding this role (local or from
/// an external IdP) is therefore always a super-user.
pub const SUPERUSER_ROLE: &str = "admin";

/// Whether `rule` is the global `ALL` grant that confers super-user (no target / `ALL` target).
fn is_global_all(rule: &PrivilegeRule) -> bool {
    rule.privilege == Privilege::All && matches!(rule.target, None | Some(PrivilegeTarget::All))
}

/// The resolved identity of an authenticated principal.
#[derive(Debug, Clone)]
pub struct AuthIdentity {
    pub username: String,
    pub roles: Vec<String>,
    pub is_super_user: bool,
}

impl AuthIdentity {
    /// A role-less, non-privileged identity (used as a safe fallback when anonymous access is
    /// disabled and no credentials were supplied).
    pub fn empty() -> Self {
        Self {
            username: String::new(),
            roles: Vec::new(),
            is_super_user: false,
        }
    }

    /// A super-user identity for internal/system-initiated queries that bypass authorization.
    pub fn system() -> Self {
        Self {
            username: "system".to_string(),
            roles: Vec::new(),
            is_super_user: true,
        }
    }
}

/// Beacon's authorization context: a pluggable authentication provider plus the
/// Beacon-owned role model. Shared across requests via `Arc`.
pub struct AuthContext {
    role_provider: RoleProvider,
    auth_provider: Arc<dyn AuthProvider>,
    /// Username of the anonymous principal used for unauthenticated access, if enabled.
    anonymous_user: Option<String>,
}

impl AuthContext {
    pub fn new(auth_provider: Arc<dyn AuthProvider>) -> Self {
        Self::with_role_provider(auth_provider, RoleProvider::new())
    }

    /// Builds a context around a pre-built role provider, allowing a persistence-backed provider
    /// (hydrated from durable storage) to be supplied instead of the default in-memory one.
    pub fn with_role_provider(
        auth_provider: Arc<dyn AuthProvider>,
        role_provider: RoleProvider,
    ) -> Self {
        Self {
            role_provider,
            auth_provider,
            anonymous_user: None,
        }
    }

    /// Enables anonymous access, resolving unauthenticated requests to `username`'s roles.
    /// The named user must already exist in the provider.
    pub fn set_anonymous_user(&mut self, username: impl Into<String>) {
        self.anonymous_user = Some(username.into());
    }

    /// Whether anonymous access is enabled.
    pub fn anonymous_enabled(&self) -> bool {
        self.anonymous_user.is_some()
    }

    /// Resolves the anonymous principal's identity (empty password), erroring when disabled.
    pub async fn authenticate_anonymous(&self) -> anyhow::Result<AuthIdentity> {
        let username = self
            .anonymous_user
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("anonymous access is disabled"))?;
        self.authenticate(&Credential::basic(username, "")).await
    }

    pub fn role_provider(&self) -> &RoleProvider {
        &self.role_provider
    }

    pub fn auth_provider(&self) -> &Arc<dyn AuthProvider> {
        &self.auth_provider
    }

    /// Authenticates a credential and resolves the principal's roles into an identity.
    pub async fn authenticate(&self, credential: &Credential) -> anyhow::Result<AuthIdentity> {
        let authed = self.auth_provider.authenticate(credential).await?;
        let is_super_user = self.role_provider.has_global_all_grant(&authed.roles);
        Ok(AuthIdentity {
            username: authed.username,
            roles: authed.roles,
            is_super_user,
        })
    }

    /// Evaluates whether the given roles may perform `privilege` on `target`.
    pub fn is_allowed(
        &self,
        roles: &[String],
        privilege: Privilege,
        target: &ConcreteTarget,
    ) -> bool {
        self.role_provider.is_allowed(roles, privilege, target)
    }

    // --- Role management (delegated to the role provider) ---

    pub fn create_role(&self, name: &str) -> anyhow::Result<()> {
        if name == SUPERUSER_ROLE {
            anyhow::bail!(
                "'{SUPERUSER_ROLE}' is a reserved built-in role and always exists"
            );
        }
        self.role_provider.create_role(name)
    }

    pub fn drop_role(&self, name: &str) -> anyhow::Result<()> {
        if name == SUPERUSER_ROLE {
            anyhow::bail!("cannot drop the built-in super-user role '{SUPERUSER_ROLE}'");
        }
        self.role_provider.drop_role(name)
    }

    pub fn grant(&self, role: &str, rule: PrivilegeRule) -> anyhow::Result<()> {
        self.role_provider.grant(role, rule)
    }

    pub fn deny(&self, role: &str, rule: PrivilegeRule) -> anyhow::Result<()> {
        self.role_provider.deny(role, rule)
    }

    pub fn revoke(&self, role: &str, rule: &PrivilegeRule, is_deny: bool) -> anyhow::Result<()> {
        if role == SUPERUSER_ROLE && !is_deny && is_global_all(rule) {
            anyhow::bail!(
                "cannot revoke the global ALL grant from the built-in super-user role '{SUPERUSER_ROLE}'"
            );
        }
        self.role_provider.revoke(role, rule, is_deny)
    }

    // --- User management (delegated to the provider's user directory) ---

    fn user_directory(&self) -> anyhow::Result<Arc<dyn crate::provider::UserDirectory>> {
        self.auth_provider
            .user_directory()
            .ok_or_else(|| anyhow::anyhow!("the active auth provider does not support user management"))
    }

    /// Whether a user exists in the active provider's directory (false if it has none).
    pub fn user_exists(&self, username: &str) -> bool {
        self.auth_provider
            .user_directory()
            .map(|dir| dir.user_exists(username))
            .unwrap_or(false)
    }

    pub fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        self.user_directory()?.create_user(username, password)
    }

    pub fn drop_user(&self, username: &str) -> anyhow::Result<()> {
        self.user_directory()?.drop_user(username)
    }

    pub fn grant_role_to_user(&self, username: &str, role: &str) -> anyhow::Result<()> {
        if !self.role_provider.role_exists(role) {
            anyhow::bail!("role '{role}' does not exist");
        }
        self.user_directory()?.grant_role(username, role)
    }

    pub fn revoke_role_from_user(&self, username: &str, role: &str) -> anyhow::Result<()> {
        self.user_directory()?.revoke_role(username, role)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::BasicAuthProvider;
    use crate::role::PrivilegeTarget;

    fn admin_context() -> AuthContext {
        AuthContext::new(Arc::new(BasicAuthProvider::new()))
    }

    #[tokio::test]
    async fn end_to_end_user_role_flow() {
        let ctx = admin_context();
        ctx.create_role("reader").unwrap();
        ctx.create_user("alice", "secret").unwrap();
        ctx.grant_role_to_user("alice", "reader").unwrap();
        ctx.grant("reader", PrivilegeRule::new(Privilege::Select, None)).unwrap();
        ctx.deny(
            "reader",
            PrivilegeRule::new(
                Privilege::Select,
                Some(PrivilegeTarget::Path("example/*".to_string())),
            ),
        )
        .unwrap();

        let identity = ctx.authenticate(&Credential::basic("alice", "secret")).await.unwrap();
        assert_eq!(identity.username, "alice");
        assert_eq!(identity.roles, vec!["reader".to_string()]);
        assert!(!identity.is_super_user);

        assert!(!ctx.is_allowed(
            &identity.roles,
            Privilege::Select,
            &ConcreteTarget::Path("example/file.parquet".to_string())
        ));
        assert!(ctx.is_allowed(
            &identity.roles,
            Privilege::Select,
            &ConcreteTarget::Path("example_2/file.parquet".to_string())
        ));
    }

    #[tokio::test]
    async fn super_user_detected_from_global_all_grant() {
        // Any role with a global ALL grant confers super-user — not only the built-in role.
        let ctx = admin_context();
        ctx.create_role("owners").unwrap();
        ctx.create_user("root", "pw").unwrap();
        ctx.grant_role_to_user("root", "owners").unwrap();
        ctx.grant("owners", PrivilegeRule::new(Privilege::All, None)).unwrap();

        let identity = ctx.authenticate(&Credential::basic("root", "pw")).await.unwrap();
        assert!(identity.is_super_user);
    }

    #[test]
    fn grant_role_to_user_requires_existing_role() {
        let ctx = admin_context();
        ctx.create_user("alice", "secret").unwrap();
        assert!(ctx.grant_role_to_user("alice", "ghost").is_err());
    }

    #[tokio::test]
    async fn anonymous_user_resolves_to_its_roles() {
        let mut ctx = admin_context();
        ctx.create_role("public").unwrap();
        ctx.create_user(ANONYMOUS_USERNAME, "").unwrap();
        ctx.grant_role_to_user(ANONYMOUS_USERNAME, "public").unwrap();
        ctx.set_anonymous_user(ANONYMOUS_USERNAME);

        assert!(ctx.anonymous_enabled());
        let identity = ctx.authenticate_anonymous().await.unwrap();
        assert_eq!(identity.username, ANONYMOUS_USERNAME);
        assert_eq!(identity.roles, vec!["public".to_string()]);
        assert!(!identity.is_super_user);
    }

    #[tokio::test]
    async fn anonymous_disabled_by_default() {
        let ctx = admin_context();
        assert!(!ctx.anonymous_enabled());
        assert!(ctx.authenticate_anonymous().await.is_err());
    }

    /// The built-in super-user role is protected: it cannot be dropped, nor can its global `ALL`
    /// grant be revoked — so a principal holding it is always a super-user.
    #[tokio::test]
    async fn superuser_role_is_protected() {
        let ctx = admin_context();
        // Seed it the way bootstrap does — through the raw provider, which bypasses the reserved
        // guard on the SQL-facing `create_role`.
        ctx.role_provider().create_role(SUPERUSER_ROLE).unwrap();
        let all = PrivilegeRule::new(Privilege::All, None);
        ctx.grant(SUPERUSER_ROLE, all.clone()).unwrap();

        // The reserved name cannot be (re-)created through the SQL path.
        assert!(ctx.create_role(SUPERUSER_ROLE).is_err());
        // Cannot be dropped.
        assert!(ctx.drop_role(SUPERUSER_ROLE).is_err());
        // Cannot have its global ALL grant revoked.
        assert!(ctx.revoke(SUPERUSER_ROLE, &all, false).is_err());

        // A user holding it is still a super-user.
        ctx.create_user("root", "pw").unwrap();
        ctx.grant_role_to_user("root", SUPERUSER_ROLE).unwrap();
        let identity = ctx.authenticate(&Credential::basic("root", "pw")).await.unwrap();
        assert!(identity.is_super_user);

        // Other, scoped rules on the role can still be managed normally.
        let scoped = PrivilegeRule::new(
            Privilege::Select,
            Some(PrivilegeTarget::Table("t".to_string())),
        );
        ctx.grant(SUPERUSER_ROLE, scoped.clone()).unwrap();
        assert!(ctx.revoke(SUPERUSER_ROLE, &scoped, false).is_ok());
    }
}
