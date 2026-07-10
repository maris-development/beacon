//! Side effect for the auth-management execution-plan node.
//!
//! Recovers the shared [`beacon_auth::AuthContext`] from the session extension (registered in
//! `Runtime::init_ctx`) and applies the statement. Reaching here implies super-user privileges:
//! the [`AuthNode`](super::logical::AuthNode) is an `Extension` node, which
//! [`validate_query_plan`](super::validate_query_plan) only admits for super-users.

use std::sync::Arc;

use beacon_auth::{AuthContext, PrivilegeRule};
use datafusion::prelude::SessionContext;

use crate::parser::statement::AuthStatement;

/// Recover the auth context from the session extension handle.
fn auth_context(session: &Arc<SessionContext>) -> anyhow::Result<Arc<AuthContext>> {
    session
        .state()
        .config()
        .get_extension::<AuthContext>()
        .ok_or_else(|| anyhow::anyhow!("auth subsystem is not available"))
}

/// Apply an auth-management statement to the shared [`AuthContext`].
pub(crate) fn apply_auth_statement(
    session: &Arc<SessionContext>,
    statement: &AuthStatement,
) -> anyhow::Result<()> {
    let auth = auth_context(session)?;

    match statement {
        AuthStatement::CreateUser { username, password } => auth.create_user(username, password),
        AuthStatement::DropUser { username } => auth.drop_user(username),
        AuthStatement::CreateRole { role } => auth.create_role(role),
        AuthStatement::DropRole { role } => auth.drop_role(role),
        AuthStatement::GrantRoleToUser { role, username } => {
            auth.grant_role_to_user(username, role)
        }
        AuthStatement::RevokeRoleFromUser { role, username } => {
            auth.revoke_role_from_user(username, role)
        }
        AuthStatement::GrantPrivilege {
            privilege,
            target,
            role,
        } => auth.grant(role, PrivilegeRule::new(*privilege, target.clone())),
        AuthStatement::DenyPrivilege {
            privilege,
            target,
            role,
        } => auth.deny(role, PrivilegeRule::new(*privilege, target.clone())),
        AuthStatement::RevokePrivilege {
            privilege,
            target,
            role,
            deny,
        } => auth.revoke(role, &PrivilegeRule::new(*privilege, target.clone()), *deny),
    }
}
