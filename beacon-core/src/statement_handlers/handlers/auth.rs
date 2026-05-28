use async_trait::async_trait;
use beacon_auth::PrivilegeRule;
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions};

use crate::{
    parser::statement::AuthStatement,
    statement_handlers::{
        context::HandlerContext,
        payload::{StatementKind, StatementPayload},
        traits::StatementHandler,
    },
};

/// Handles authentication/authorization management statements (users, roles, grants, denies).
///
/// These statements mutate the shared [`beacon_auth::AuthContext`]. Anonymous callers are already
/// rejected upstream by `Runtime::ensure_anonymous_statement_allowed`, so reaching this handler
/// implies super-user privileges.
pub(crate) struct AuthStatementHandler;

#[async_trait]
impl StatementHandler for AuthStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::Auth
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        _sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_auth()?;
        let auth = context.auth_context();

        match statement {
            AuthStatement::CreateUser { username, password } => {
                auth.create_user(&username, &password)?;
            }
            AuthStatement::DropUser { username } => {
                auth.drop_user(&username)?;
            }
            AuthStatement::CreateRole { role } => {
                auth.create_role(&role)?;
            }
            AuthStatement::DropRole { role } => {
                auth.drop_role(&role)?;
            }
            AuthStatement::GrantRoleToUser { role, username } => {
                auth.grant_role_to_user(&username, &role)?;
            }
            AuthStatement::RevokeRoleFromUser { role, username } => {
                auth.revoke_role_from_user(&username, &role)?;
            }
            AuthStatement::GrantPrivilege {
                privilege,
                target,
                role,
            } => {
                auth.grant(&role, PrivilegeRule::new(privilege, target))?;
            }
            AuthStatement::DenyPrivilege {
                privilege,
                target,
                role,
            } => {
                auth.deny(&role, PrivilegeRule::new(privilege, target))?;
            }
            AuthStatement::RevokePrivilege {
                privilege,
                target,
                role,
                deny,
            } => {
                auth.revoke(&role, &PrivilegeRule::new(privilege, target), deny)?;
            }
        }

        Ok(context.empty_record_batch_stream())
    }
}
