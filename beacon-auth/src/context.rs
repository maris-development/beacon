//! The central authorization context owned by Beacon.

use std::sync::Arc;

use crate::{
    credential::Credential,
    provider::{AuthProvider, UserRecord},
    role::{ConcreteTarget, Privilege, PrivilegeRule, Role, RoleProvider},
};

/// Default username of the built-in anonymous principal.
pub const ANONYMOUS_USERNAME: &str = "anonymous";

/// The single super-user, defined entirely by deployment configuration (the `BEACON_ADMIN_*`
/// environment variables).
///
/// The super-user is intentionally **not** a role or a stored user: it is a fixed credential checked
/// directly at authentication time. This makes it impossible to create another super-user through
/// SQL — every role and user created via `CREATE ROLE`/`CREATE USER` is read-only and never a
/// super-user — and impossible to change the super-user except via the environment.
#[derive(Debug, Clone)]
struct SuperUser {
    username: String,
    password: String,
}

/// Constant-time string equality, to avoid leaking the super-user credential via timing.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
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
    /// The single super-user credential, sourced from configuration. `None` disables super-user
    /// access entirely (used by some tests).
    super_user: Option<SuperUser>,
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
            super_user: None,
            anonymous_user: None,
        }
    }

    /// Configures the single super-user credential (from `BEACON_ADMIN_USERNAME`/`PASSWORD`).
    /// A Basic login matching these credentials is the only way to become a super-user.
    pub fn set_super_user(&mut self, username: impl Into<String>, password: impl Into<String>) {
        self.super_user = Some(SuperUser {
            username: username.into(),
            password: password.into(),
        });
    }

    /// The configured super-user's username, if any.
    fn super_user_username(&self) -> Option<&str> {
        self.super_user.as_ref().map(|su| su.username.as_str())
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

    /// The username resolving anonymous access, if enabled.
    pub fn anonymous_username(&self) -> Option<&str> {
        self.anonymous_user.as_deref()
    }

    /// Resolves the anonymous principal's identity (empty password), erroring when disabled.
    pub async fn authenticate_anonymous(&self) -> anyhow::Result<AuthIdentity> {
        let username = self
            .anonymous_user
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("anonymous access is disabled"))?;
        self.authenticate(&Credential::basic(username, "")).await
    }

    /// Enumerate all stored local users with their assigned roles. Returns empty when the
    /// authentication provider has no manageable user directory (e.g. an OIDC-only deployment).
    pub async fn list_users(&self) -> anyhow::Result<Vec<UserRecord>> {
        match self.auth_provider.user_directory() {
            Some(dir) => dir.list_users().await,
            None => Ok(Vec::new()),
        }
    }

    /// Snapshot of all roles with their grant/deny rules.
    pub fn list_roles(&self) -> Vec<Role> {
        self.role_provider.list_roles()
    }

    pub fn role_provider(&self) -> &RoleProvider {
        &self.role_provider
    }

    /// Rebuilds the in-memory working copies (roles and users) from durable storage.
    ///
    /// Called once at startup, after the backing session and its tables exist — the role/user
    /// stores are attached unhydrated at construction because the session they read through does
    /// not exist yet (see `runtime_builder`). A no-op for providers without a persistent backend.
    pub async fn hydrate(&self) -> anyhow::Result<()> {
        self.role_provider.hydrate().await?;
        if let Some(directory) = self.auth_provider.user_directory() {
            directory.hydrate().await?;
        }
        Ok(())
    }

    pub fn auth_provider(&self) -> &Arc<dyn AuthProvider> {
        &self.auth_provider
    }

    /// Authenticates a credential and resolves the principal into an identity.
    ///
    /// The configured super-user credential is checked first and short-circuits, returning a
    /// super-user identity without consulting the provider or role store. Every other principal is
    /// resolved by the provider and is **always non-super** — the only super-user is the configured
    /// one. Authorization for non-super principals comes entirely from their (read-only) roles.
    pub async fn authenticate(&self, credential: &Credential) -> anyhow::Result<AuthIdentity> {
        if let Credential::Basic { username, password } = credential {
            if let Some(super_user) = &self.super_user {
                if constant_time_eq(username, &super_user.username)
                    && constant_time_eq(password, &super_user.password)
                {
                    return Ok(AuthIdentity {
                        username: super_user.username.clone(),
                        roles: Vec::new(),
                        is_super_user: true,
                    });
                }
            }
        }

        let authed = self.auth_provider.authenticate(credential).await?;
        Ok(AuthIdentity {
            username: authed.username,
            roles: authed.roles,
            is_super_user: false,
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
    //
    // Roles are strictly read-only: only `SELECT` may be granted or denied. Write/management access
    // is reserved to the configured super-user and is never expressible as a role grant.

    pub async fn create_role(&self, name: &str) -> anyhow::Result<()> {
        self.role_provider.create_role(name).await
    }

    pub async fn drop_role(&self, name: &str) -> anyhow::Result<()> {
        self.role_provider.drop_role(name).await
    }

    pub async fn grant(&self, role: &str, rule: PrivilegeRule) -> anyhow::Result<()> {
        Self::ensure_read_only(&rule)?;
        self.role_provider.grant(role, rule).await
    }

    pub async fn deny(&self, role: &str, rule: PrivilegeRule) -> anyhow::Result<()> {
        Self::ensure_read_only(&rule)?;
        self.role_provider.deny(role, rule).await
    }

    pub async fn revoke(
        &self,
        role: &str,
        rule: &PrivilegeRule,
        is_deny: bool,
    ) -> anyhow::Result<()> {
        self.role_provider.revoke(role, rule, is_deny).await
    }

    /// Rejects any non-`SELECT` privilege: roles can only grant read access.
    fn ensure_read_only(rule: &PrivilegeRule) -> anyhow::Result<()> {
        if rule.privilege != Privilege::Select {
            anyhow::bail!(
                "roles are read-only: only SELECT may be granted (got {}). Write and management \
                 access is reserved to the configured super-user.",
                rule.privilege
            );
        }
        Ok(())
    }

    // --- User management (delegated to the provider's user directory) ---

    fn user_directory(&self) -> anyhow::Result<Arc<dyn crate::provider::UserDirectory>> {
        self.auth_provider
            .user_directory()
            .ok_or_else(|| anyhow::anyhow!("the active auth provider does not support user management"))
    }

    /// Whether a user exists in the active provider's directory (false if it has none).
    pub async fn user_exists(&self, username: &str) -> bool {
        match self.auth_provider.user_directory() {
            Some(dir) => dir.user_exists(username).await,
            None => false,
        }
    }

    pub async fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        self.ensure_not_super_user_name(username)?;
        self.user_directory()?.create_user(username, password).await
    }

    pub async fn drop_user(&self, username: &str) -> anyhow::Result<()> {
        self.ensure_not_super_user_name(username)?;
        // The anonymous user is Beacon-managed: while anonymous access is enabled it
        // must always exist, so it can't be deleted (disable it with
        // BEACON_AUTH_ANONYMOUS_ENABLED=false instead).
        if self.anonymous_user.as_deref() == Some(username) {
            anyhow::bail!(
                "'{username}' is the anonymous user and cannot be deleted while anonymous access \
                 is enabled (set BEACON_AUTH_ANONYMOUS_ENABLED=false to disable it)"
            );
        }
        self.user_directory()?.drop_user(username).await
    }

    /// Rejects operations on the reserved super-user username: the super-user is config-defined and
    /// cannot be shadowed, created, or dropped through SQL.
    fn ensure_not_super_user_name(&self, username: &str) -> anyhow::Result<()> {
        if self.super_user_username() == Some(username) {
            anyhow::bail!(
                "'{username}' is the reserved super-user (defined by configuration) and cannot be \
                 managed through SQL"
            );
        }
        Ok(())
    }

    pub async fn grant_role_to_user(&self, username: &str, role: &str) -> anyhow::Result<()> {
        if !self.role_provider.role_exists(role) {
            anyhow::bail!("role '{role}' does not exist");
        }
        self.user_directory()?.grant_role(username, role).await
    }

    pub async fn revoke_role_from_user(&self, username: &str, role: &str) -> anyhow::Result<()> {
        self.user_directory()?.revoke_role(username, role).await
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

    /// A context with the configured super-user `root`/`secret`.
    fn context_with_super_user() -> AuthContext {
        let mut ctx = admin_context();
        ctx.set_super_user("root", "secret");
        ctx
    }

    #[tokio::test]
    async fn anonymous_user_cannot_be_dropped_while_enabled() {
        let mut ctx = admin_context();
        ctx.create_user("anonymous", "").await.unwrap();
        ctx.create_user("alice", "pw").await.unwrap();
        ctx.set_anonymous_user("anonymous");

        // The anonymous user is protected; a regular user is not.
        assert!(ctx.drop_user("anonymous").await.is_err());
        assert!(ctx.user_exists("anonymous").await);
        ctx.drop_user("alice").await.unwrap();
        assert!(!ctx.user_exists("alice").await);

        // With anonymous access disabled, the user is droppable again.
        let mut ctx = admin_context();
        ctx.create_user("anonymous", "").await.unwrap();
        ctx.drop_user("anonymous").await.unwrap();
        assert!(!ctx.user_exists("anonymous").await);
    }

    #[tokio::test]
    async fn end_to_end_user_role_flow() {
        let ctx = admin_context();
        ctx.create_role("reader").await.unwrap();
        ctx.create_user("alice", "secret").await.unwrap();
        ctx.grant_role_to_user("alice", "reader").await.unwrap();
        ctx.grant("reader", PrivilegeRule::new(Privilege::Select, None))
            .await
            .unwrap();
        ctx.deny(
            "reader",
            PrivilegeRule::new(
                Privilege::Select,
                Some(PrivilegeTarget::Path("example/*".to_string())),
            ),
        )
        .await
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
    async fn only_the_configured_credential_is_super_user() {
        let ctx = context_with_super_user();
        // The configured credential authenticates as the super-user.
        let admin = ctx.authenticate(&Credential::basic("root", "secret")).await.unwrap();
        assert!(admin.is_super_user);
        assert_eq!(admin.username, "root");
        assert!(admin.roles.is_empty());

        // The right username with the wrong password is not super (and not in the store either).
        assert!(ctx.authenticate(&Credential::basic("root", "wrong")).await.is_err());
    }

    #[tokio::test]
    async fn created_users_and_roles_are_never_super_user() {
        let ctx = context_with_super_user();
        // A normal user with a normal role is never a super-user, whatever roles they hold.
        ctx.create_role("reader").await.unwrap();
        ctx.create_user("alice", "pw").await.unwrap();
        ctx.grant_role_to_user("alice", "reader").await.unwrap();
        ctx.grant("reader", PrivilegeRule::new(Privilege::Select, None))
            .await
            .unwrap();

        let identity = ctx.authenticate(&Credential::basic("alice", "pw")).await.unwrap();
        assert!(!identity.is_super_user);

        // Write/management privileges cannot be granted to a role at all — roles are read-only.
        for privilege in [
            Privilege::Insert,
            Privilege::Update,
            Privilege::Delete,
            Privilege::Create,
            Privilege::Drop,
            Privilege::All,
        ] {
            assert!(
                ctx.grant("reader", PrivilegeRule::new(privilege, None)).await.is_err(),
                "granting {privilege} to a role must be rejected"
            );
        }

        // The super-user username is reserved: it cannot be created or dropped as a stored user.
        assert!(ctx.create_user("root", "pw").await.is_err());
        assert!(ctx.drop_user("root").await.is_err());
    }

    #[tokio::test]
    async fn bearer_credentials_are_never_super_user() {
        // Even if an external token claimed the super-user's name, a non-Basic credential can never
        // be the super-user (the config check only matches Basic).
        let ctx = context_with_super_user();
        assert!(ctx.authenticate(&Credential::bearer("root")).await.is_err());
    }

    #[tokio::test]
    async fn grant_role_to_user_requires_existing_role() {
        let ctx = admin_context();
        ctx.create_user("alice", "secret").await.unwrap();
        assert!(ctx.grant_role_to_user("alice", "ghost").await.is_err());
    }

    #[tokio::test]
    async fn anonymous_user_resolves_to_its_roles() {
        let mut ctx = admin_context();
        ctx.create_role("public").await.unwrap();
        ctx.create_user(ANONYMOUS_USERNAME, "").await.unwrap();
        ctx.grant_role_to_user(ANONYMOUS_USERNAME, "public").await.unwrap();
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

    #[tokio::test]
    async fn super_user_is_not_stored_so_cannot_be_changed_via_sql() {
        // The super-user is not a stored user — `user_exists` is false even though it authenticates.
        let ctx = context_with_super_user();
        assert!(!ctx.user_exists("root").await);
        assert!(ctx.authenticate(&Credential::basic("root", "secret")).await.unwrap().is_super_user);
    }

    /// `constant_time_eq` must be a real equality check, not a length-only or
    /// prefix comparison — otherwise the super-user gate could be bypassed.
    #[test]
    fn constant_time_eq_is_true_equality() {
        assert!(constant_time_eq("", ""));
        assert!(constant_time_eq("secret", "secret"));
        assert!(!constant_time_eq("secret", "Secret"));
        assert!(!constant_time_eq("secret", "secre")); // differing length
        assert!(!constant_time_eq("secret", "secretx"));
        assert!(!constant_time_eq("secret", "xecret")); // first byte differs
        assert!(!constant_time_eq("secret", "secrex")); // last byte differs
    }

    /// A super-user configured with an empty password still requires an *exact*
    /// empty-password match; a non-empty password for that username is rejected,
    /// and an unrelated user is unaffected.
    #[tokio::test]
    async fn super_user_with_empty_password_matches_exactly() {
        let mut ctx = admin_context();
        ctx.set_super_user("root", "");
        assert!(
            ctx.authenticate(&Credential::basic("root", ""))
                .await
                .unwrap()
                .is_super_user
        );
        // Wrong (non-empty) password is not the super-user, and there is no such
        // stored user to fall through to.
        assert!(ctx.authenticate(&Credential::basic("root", "x")).await.is_err());
    }

    /// Without a configured super-user, no Basic credential is ever super — the
    /// principal falls through to the provider and is resolved (non-super) or
    /// rejected there.
    #[tokio::test]
    async fn no_super_user_configured_means_no_one_is_super() {
        let ctx = admin_context();
        ctx.create_user("root", "secret").await.unwrap();
        let identity = ctx
            .authenticate(&Credential::basic("root", "secret"))
            .await
            .unwrap();
        assert!(!identity.is_super_user);
        assert_eq!(identity.username, "root");
    }

    /// The super-user short-circuits *before* the provider is consulted, so it
    /// authenticates even when no stored user of that name exists. The provider
    /// still owns everyone else.
    #[tokio::test]
    async fn super_user_bypasses_the_provider() {
        let ctx = context_with_super_user();
        // No user directory entry for `root` at all.
        assert!(!ctx.user_exists("root").await);
        assert!(
            ctx.authenticate(&Credential::basic("root", "secret"))
                .await
                .unwrap()
                .is_super_user
        );
        // A wrong password does not short-circuit and finds no stored user.
        assert!(ctx.authenticate(&Credential::basic("root", "nope")).await.is_err());
    }

    /// `AuthIdentity::system` is the internal bypass identity; `empty` is the safe
    /// no-privilege fallback. These are relied on across the codebase.
    #[test]
    fn system_and_empty_identities_have_the_expected_shape() {
        let system = AuthIdentity::system();
        assert!(system.is_super_user);
        assert!(system.roles.is_empty());

        let empty = AuthIdentity::empty();
        assert!(!empty.is_super_user);
        assert!(empty.roles.is_empty());
        assert!(empty.username.is_empty());
    }

    /// `ensure_read_only` (via `grant`/`deny`) rejects a non-SELECT rule *before*
    /// touching the role store, so the role need not even exist for the rejection.
    #[tokio::test]
    async fn read_only_check_precedes_role_lookup() {
        let ctx = admin_context();
        // No such role, but the privilege check fails first with the read-only
        // message rather than a "role does not exist" error.
        let err = ctx
            .grant("ghost", PrivilegeRule::new(Privilege::Insert, None))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("read-only"), "got: {err}");
        // SELECT on a missing role does reach the store and fails there.
        assert!(
            ctx.grant("ghost", PrivilegeRule::new(Privilege::Select, None))
                .await
                .unwrap_err()
                .to_string()
                .contains("does not exist")
        );
    }

    /// User management is unavailable when the provider has no directory (e.g. an
    /// OIDC-only deployment): mutations error, and enumeration returns empty
    /// rather than failing.
    #[tokio::test]
    async fn user_management_without_a_directory() {
        use crate::oidc::{OidcAuthProvider, OidcConfig};
        use std::time::Duration;

        let oidc = Arc::new(OidcAuthProvider::new(OidcConfig {
            issuer: "https://issuer.example".to_string(),
            jwks_url: "https://issuer.example/jwks".to_string(),
            audience: None,
            roles_claim: "realm_access.roles".to_string(),
            username_claim: "preferred_username".to_string(),
            jwks_cache_ttl: Duration::from_secs(300),
        }));
        let ctx = AuthContext::new(oidc);

        assert!(ctx.create_user("alice", "pw").await.is_err());
        assert!(!ctx.user_exists("alice").await);
        assert!(ctx.list_users().await.unwrap().is_empty());
        // Roles are still Beacon-owned and manageable even without a user directory.
        ctx.create_role("reader").await.unwrap();
        assert!(ctx.list_roles().iter().any(|r| r.name == "reader"));
    }

    /// Enumeration reflects live state: newly created users and roles appear, and
    /// list_users carries each user's assigned roles.
    #[tokio::test]
    async fn enumeration_reflects_created_users_and_roles() {
        let ctx = admin_context();
        ctx.create_role("reader").await.unwrap();
        ctx.create_user("alice", "pw").await.unwrap();
        ctx.grant_role_to_user("alice", "reader").await.unwrap();

        let users = ctx.list_users().await.unwrap();
        let alice = users.iter().find(|u| u.username == "alice").unwrap();
        assert_eq!(alice.roles, vec!["reader".to_string()]);
    }

    /// Anonymous authentication resolves through the provider, so the named user
    /// must actually exist; enabling anonymous access to a missing user fails
    /// closed at authentication time.
    #[tokio::test]
    async fn anonymous_pointing_at_a_missing_user_fails_closed() {
        let mut ctx = admin_context();
        ctx.set_anonymous_user("nobody");
        assert!(ctx.anonymous_enabled());
        assert_eq!(ctx.anonymous_username(), Some("nobody"));
        assert!(ctx.authenticate_anonymous().await.is_err());
    }

    /// Granting a role to a non-existent user is rejected by the directory, even
    /// when the role itself exists.
    #[tokio::test]
    async fn grant_role_to_missing_user_is_rejected() {
        let ctx = admin_context();
        ctx.create_role("reader").await.unwrap();
        assert!(ctx.grant_role_to_user("ghost", "reader").await.is_err());
    }
}
