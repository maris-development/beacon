//! Which stored paths a caller may be shown.
//!
//! Read authorization answers one question about one path, and the query
//! planner asks it per scan: may this caller read this? A listing asks the same
//! question about every candidate, and about the directories above them, and
//! answers by dropping rows rather than by refusing the statement. Same rule,
//! different consequence.
//!
//! [`PathVisibility`] is that rule, in one place. It holds no storage types, so
//! it applies equally to an object listing, a directory browse, and a planner
//! target. Callers pass a path string; an object listing passes
//! `meta.location.as_ref()`.

use crate::{AuthContext, AuthIdentity, ConcreteTarget, Privilege};

/// Decides which paths a caller may be shown.
///
/// Two states rather than a context plus a flag. A caller who is not subject to
/// grants at all — enforcement is off, or they are the super-user — is a
/// different thing from one whose grants happen to allow everything, and a
/// caller of this type should not have to remember which it is holding.
#[derive(Clone, Copy)]
pub enum PathVisibility<'a> {
    /// Every path is visible.
    Unrestricted,
    /// Visibility follows the `Select` grants of `roles`.
    Restricted {
        auth: &'a AuthContext,
        roles: &'a [String],
    },
}

impl<'a> PathVisibility<'a> {
    /// The visibility `identity` has under `auth`.
    ///
    /// `enforce` is the deployment's grant-enforcement switch. With it off, or
    /// for the super-user, this is [`PathVisibility::Unrestricted`] — the same
    /// condition the read path applies, so a listing never hides what a query
    /// would have returned.
    pub fn for_identity(auth: &'a AuthContext, identity: &'a AuthIdentity, enforce: bool) -> Self {
        if !enforce || identity.is_super_user {
            return Self::Unrestricted;
        }
        Self::Restricted {
            auth,
            roles: &identity.roles,
        }
    }

    /// Whether any path is hidden. A caller that only needs to know whether to
    /// filter at all can skip building the filter.
    pub fn is_restricted(&self) -> bool {
        matches!(self, Self::Restricted { .. })
    }

    /// Whether the object at `path` may be shown.
    ///
    /// This is the question the read path asks, so a listing cannot name a file
    /// the caller would then be refused.
    pub fn allows_path(&self, path: &str) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::Restricted { auth, roles } => auth.is_allowed(
                roles,
                Privilege::Select,
                &ConcreteTarget::Path(path.to_string()),
            ),
        }
    }

    /// Whether the directory `prefix` may be shown.
    ///
    /// A directory is not a grantable target. It is worth showing when something
    /// inside it is readable, so this asks that instead. An empty `prefix` is the
    /// store root.
    pub fn allows_prefix(&self, prefix: &str) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::Restricted { auth, roles } => {
                auth.prefix_is_reachable(roles, Privilege::Select, prefix)
            }
        }
    }
}

impl std::fmt::Debug for PathVisibility<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unrestricted => write!(f, "PathVisibility::Unrestricted"),
            Self::Restricted { roles, .. } => f
                .debug_struct("PathVisibility::Restricted")
                .field("roles", roles)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::basic::BasicAuthProvider;
    use crate::{PrivilegeRule, PrivilegeTarget};

    fn identity(roles: &[&str], is_super_user: bool) -> AuthIdentity {
        AuthIdentity {
            username: "alice".to_string(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            is_super_user,
        }
    }

    async fn context_granting(pattern: &str) -> AuthContext {
        let ctx = AuthContext::new(Arc::new(BasicAuthProvider::new()));
        ctx.create_role("reader").await.unwrap();
        ctx.grant(
            "reader",
            PrivilegeRule {
                privilege: Privilege::Select,
                target: Some(PrivilegeTarget::Path(pattern.to_string())),
            },
        )
        .await
        .unwrap();
        ctx
    }

    /// Enforcement off is the deployment default, so it must show everything.
    #[tokio::test]
    async fn enforcement_off_is_unrestricted() {
        let ctx = context_granting("argo/**").await;
        let who = identity(&["reader"], false);
        let visible = PathVisibility::for_identity(&ctx, &who, false);

        assert!(!visible.is_restricted());
        assert!(visible.allows_path("secret/x.nc"));
        assert!(visible.allows_prefix("secret"));
    }

    /// The super-user is exempt however the deployment is configured.
    #[tokio::test]
    async fn the_super_user_is_unrestricted() {
        let ctx = context_granting("argo/**").await;
        let who = identity(&[], true);
        let visible = PathVisibility::for_identity(&ctx, &who, true);

        assert!(!visible.is_restricted());
        assert!(visible.allows_path("secret/x.nc"));
    }

    /// With enforcement on, a listing shows the grant and nothing beside it.
    #[tokio::test]
    async fn enforcement_on_follows_the_grant() {
        let ctx = context_granting("argo/**").await;
        let who = identity(&["reader"], false);
        let visible = PathVisibility::for_identity(&ctx, &who, true);

        assert!(visible.is_restricted());
        assert!(visible.allows_path("argo/floats/a.nc"));
        assert!(!visible.allows_path("secret/x.nc"));

        // Directories on the way to the grant stay navigable; siblings do not.
        assert!(visible.allows_prefix(""));
        assert!(visible.allows_prefix("argo"));
        assert!(!visible.allows_prefix("secret"));
    }

    /// A caller with no roles sees nothing: default-deny, as elsewhere.
    #[tokio::test]
    async fn no_roles_sees_nothing() {
        let ctx = context_granting("argo/**").await;
        let who = identity(&[], false);
        let visible = PathVisibility::for_identity(&ctx, &who, true);

        assert!(!visible.allows_path("argo/floats/a.nc"));
        assert!(!visible.allows_prefix("argo"));
    }
}
