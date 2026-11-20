//! Crate-boundary tests for [`AuthContext`]: the full authenticate → authorize
//! round trip against a real [`BasicAuthProvider`].
//!
//! The role model's evaluator is unit-tested in `role.rs`, and the whole thing
//! is exercised through SQL in beacon-core's `auth` suite. This sits in between:
//! it drives the *public* `AuthContext` API a host embeds (create user/role,
//! grant/deny, `authenticate`, `is_allowed`) so that contract is pinned without
//! a running runtime or the SQL layer.

use std::sync::Arc;

use beacon_auth::{
    AuthContext, BasicAuthProvider, ConcreteTarget, Credential, Privilege, PrivilegeRule,
    PrivilegeTarget,
};

const SUPER_USER: &str = "root";
const SUPER_PASS: &str = "root-secret";

/// An `AuthContext` over an in-memory basic provider with a configured
/// super-user, matching how beacon-core wires one up.
fn context() -> AuthContext {
    let mut ctx = AuthContext::new(Arc::new(BasicAuthProvider::new()));
    ctx.set_super_user(SUPER_USER, SUPER_PASS);
    ctx
}

fn table(name: &str) -> ConcreteTarget {
    ConcreteTarget::Table(name.to_string())
}
fn path(p: &str) -> ConcreteTarget {
    ConcreteTarget::Path(p.to_string())
}
fn select_on(target: PrivilegeTarget) -> PrivilegeRule {
    PrivilegeRule::new(Privilege::Select, Some(target))
}

/// Creates `user` in `role`, authenticates, and returns the resulting identity.
async fn user_in_role(ctx: &AuthContext, user: &str, pass: &str, role: &str) -> Vec<String> {
    ctx.create_user(user, pass).await.expect("create user");
    ctx.grant_role_to_user(user, role)
        .await
        .expect("grant role to user");
    let identity = ctx
        .authenticate(&Credential::basic(user, pass))
        .await
        .expect("authenticate");
    assert!(!identity.is_super_user, "a created user is never super");
    identity.roles
}

#[tokio::test]
async fn super_user_authenticates_only_with_the_exact_credential() {
    let ctx = context();

    let root = ctx
        .authenticate(&Credential::basic(SUPER_USER, SUPER_PASS))
        .await
        .expect("the configured credential should authenticate");
    assert!(root.is_super_user);
    assert!(root.roles.is_empty(), "the super-user holds no roles");

    assert!(
        ctx.authenticate(&Credential::basic(SUPER_USER, "wrong"))
            .await
            .is_err(),
        "a wrong password must not authenticate as super-user"
    );
    assert!(
        ctx.authenticate(&Credential::basic("root2", SUPER_PASS))
            .await
            .is_err(),
        "a different username with the super password must not authenticate"
    );
}

#[tokio::test]
async fn a_table_grant_allows_only_that_table() {
    let ctx = context();
    ctx.create_role("readers").await.expect("create role");
    ctx.grant("readers", select_on(PrivilegeTarget::Table("obs".into())))
        .await
        .expect("grant");

    let roles = user_in_role(&ctx, "alice", "pw", "readers").await;

    assert!(
        ctx.is_allowed(&roles, Privilege::Select, &table("obs")),
        "the granted table should be readable"
    );
    assert!(
        !ctx.is_allowed(&roles, Privilege::Select, &table("secret")),
        "a non-granted table must not be readable"
    );
    // Roles only ever carry SELECT; a write is never allowed through one.
    assert!(
        !ctx.is_allowed(&roles, Privilege::Insert, &table("obs")),
        "a role grant must not confer write access"
    );
}

#[tokio::test]
async fn deny_wins_over_a_broader_grant() {
    let ctx = context();
    ctx.create_role("mixed").await.expect("create role");
    // Grant everything, then carve out one table with a deny.
    ctx.grant("mixed", PrivilegeRule::new(Privilege::Select, None))
        .await
        .expect("grant all");
    ctx.deny("mixed", select_on(PrivilegeTarget::Table("classified".into())))
        .await
        .expect("deny one");

    let roles = user_in_role(&ctx, "bob", "pw", "mixed").await;

    assert!(
        ctx.is_allowed(&roles, Privilege::Select, &table("public_data")),
        "the broad grant should still allow other tables"
    );
    assert!(
        !ctx.is_allowed(&roles, Privilege::Select, &table("classified")),
        "the deny must override the broad grant"
    );
}

#[tokio::test]
async fn path_grants_are_segment_aware_globs() {
    let ctx = context();
    ctx.create_role("pathers").await.expect("create role");
    // `*` matches within a segment only (require_literal_separator).
    ctx.grant("pathers", select_on(PrivilegeTarget::Path("data/*.parquet".into())))
        .await
        .expect("grant path");

    let roles = user_in_role(&ctx, "carol", "pw", "pathers").await;

    assert!(
        ctx.is_allowed(&roles, Privilege::Select, &path("data/a.parquet")),
        "a file directly under the granted prefix should match"
    );
    assert!(
        !ctx.is_allowed(&roles, Privilege::Select, &path("data/sub/a.parquet")),
        "`*` must not cross a path separator"
    );
    assert!(
        !ctx.is_allowed(&roles, Privilege::Select, &path("other/a.parquet")),
        "a different prefix should not match"
    );
    // Recursive `**` crosses separators.
    ctx.grant("pathers", select_on(PrivilegeTarget::Path("tree/**".into())))
        .await
        .expect("grant recursive path");
    assert!(
        ctx.is_allowed(&roles, Privilege::Select, &path("tree/deep/nested/x.csv")),
        "`**` should match across segments"
    );
}

#[tokio::test]
async fn dropping_a_user_revokes_authentication() {
    let ctx = context();
    ctx.create_user("dave", "pw").await.expect("create user");
    assert!(ctx
        .authenticate(&Credential::basic("dave", "pw"))
        .await
        .is_ok());

    ctx.drop_user("dave").await.expect("drop user");
    assert!(
        ctx.authenticate(&Credential::basic("dave", "pw"))
            .await
            .is_err(),
        "a dropped user must no longer authenticate"
    );
}

#[tokio::test]
async fn revoking_a_role_from_a_user_removes_the_access() {
    let ctx = context();
    ctx.create_role("temp").await.expect("create role");
    ctx.grant("temp", select_on(PrivilegeTarget::Table("t".into())))
        .await
        .expect("grant");

    let roles = user_in_role(&ctx, "erin", "pw", "temp").await;
    assert!(ctx.is_allowed(&roles, Privilege::Select, &table("t")));

    ctx.revoke_role_from_user("erin", "temp")
        .await
        .expect("revoke role");

    // Re-authenticate: the identity should no longer carry the role.
    let roles = ctx
        .authenticate(&Credential::basic("erin", "pw"))
        .await
        .expect("still authenticates")
        .roles;
    assert!(
        !ctx.is_allowed(&roles, Privilege::Select, &table("t")),
        "revoking the role should drop the access it conferred"
    );
}

#[tokio::test]
async fn dropping_a_role_drops_its_grants() {
    let ctx = context();
    ctx.create_role("ephemeral").await.expect("create role");
    ctx.grant("ephemeral", select_on(PrivilegeTarget::Table("t".into())))
        .await
        .expect("grant");
    let roles = vec!["ephemeral".to_string()];
    assert!(ctx.is_allowed(&roles, Privilege::Select, &table("t")));

    ctx.drop_role("ephemeral").await.expect("drop role");
    assert!(
        !ctx.is_allowed(&roles, Privilege::Select, &table("t")),
        "a dropped role grants nothing, even if an identity still names it"
    );
}

#[tokio::test]
async fn an_unknown_role_name_grants_nothing() {
    let ctx = context();
    let roles = vec!["does-not-exist".to_string()];
    assert!(
        !ctx.is_allowed(&roles, Privilege::Select, &table("anything")),
        "naming a role that was never created must not grant access"
    );
}

#[tokio::test]
async fn a_bearer_token_is_rejected_without_an_oidc_provider() {
    let ctx = context();
    let err = ctx
        .authenticate(&Credential::bearer("some-jwt"))
        .await
        .err()
        .expect("a bearer credential should be rejected by the basic provider");
    assert!(!err.to_string().is_empty());
}
