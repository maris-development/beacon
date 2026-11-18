//! The role rules of Beacon as a [`PlanValidator`]: checks the statement, the tables and the files
//! of a plan against the caller's roles. Deny-wins, default-deny (see [`beacon_auth`]).
//!
//! The facts come from [`PlanFacts`]. This module holds the rules only.
//!
//! - A registered table needs `Select` on its name, and no path deny may match one of its files.
//! - An ad-hoc file read needs `Select` on every file, and no deny on a table that holds one of
//!   them may apply.
//! - A source whose files the check cannot see is refused.
//! - DDL, DML and the Beacon extension statements need a super-user.

use std::sync::Arc;

use beacon_auth::{AuthContext, AuthIdentity, ConcreteTarget, Privilege, PrivilegeTarget};
use beacon_datafusion_ext::table_ext::INTERNAL_TABLE_PREFIX;
use object_store::path::Path as ObjectPath;

use beacon_core::plan_validator::{PlanFacts, PlanRead, PlanValidator, QueryLimits, TableFacts};
use beacon_core::query_cpu::CpuBudget;

/// The validator of one caller under the role model of [`beacon_auth`].
pub struct RoleValidator {
    auth: Arc<AuthContext>,
    identity: AuthIdentity,
    /// Whether table and path grants apply to a caller that is not a super-user.
    enforce: bool,
}

impl RoleValidator {
    /// Builds the validator of `identity`.
    ///
    /// # Arguments
    ///
    /// * `auth` - The roles and the rules
    /// * `identity` - The caller
    /// * `enforce` - Whether table and path grants apply to a caller that is not a super-user
    pub fn new(auth: Arc<AuthContext>, identity: AuthIdentity, enforce: bool) -> Self {
        Self {
            auth,
            identity,
            enforce,
        }
    }

    /// Whether the reads of the caller are checked against its roles.
    fn checks_reads(&self) -> bool {
        self.enforce && !self.identity.is_super_user
    }
}

#[async_trait::async_trait]
impl PlanValidator for RoleValidator {
    fn caller(&self) -> &str {
        &self.identity.username
    }

    fn limits(&self) -> QueryLimits {
        QueryLimits {
            cpu_budget: budget_for(&self.auth, &self.identity),
            output_row_limit: row_limit_for(&self.auth, &self.identity),
        }
    }

    async fn validate(&self, facts: &PlanFacts<'_>) -> anyhow::Result<()> {
        if !self.identity.is_super_user {
            facts.verify_unprivileged()?;
        }
        authorize_reads(facts, &self.auth, &self.identity, self.enforce).await
    }

    fn check_table(&self, table: &TableFacts) -> anyhow::Result<()> {
        if self.identity.is_super_user {
            return Ok(());
        }
        if table.is_internal() {
            anyhow::bail!(
                "permission denied: the internal '{INTERNAL_TABLE_PREFIX}*' tables are \
                 restricted to the super-user"
            );
        }
        if let Some(schema) = table.metadata_schema() {
            anyhow::bail!(
                "permission denied: the '{schema}' schema is restricted to the super-user"
            );
        }
        if self.enforce
            && !self.auth.is_allowed_any(
                &self.identity.roles,
                Privilege::Select,
                &table_targets(table),
            )
        {
            anyhow::bail!("permission denied: SELECT on table {}", table.name());
        }
        Ok(())
    }

    fn can_read_path(&self, path: &str) -> bool {
        !self.checks_reads()
            || self.auth.is_allowed(
                &self.identity.roles,
                Privilege::Select,
                &ConcreteTarget::Path(path.to_string()),
            )
    }
}

/// The [`CpuBudget`] of a query that `identity` runs.
///
/// 1. A super-user has no limit.
/// 2. Else the `query_cpu_limit_ms` setting of the identity's roles applies, set
///    with `ALTER ROLE <role> SET query_cpu_limit_ms = <n>`. The most generous
///    role wins, and `0` means no limit.
/// 3. Else there is no limit.
///
/// The roles are read live, so an `ALTER ROLE` applies from the next query.
fn budget_for(auth: &AuthContext, identity: &AuthIdentity) -> CpuBudget {
    if identity.is_super_user {
        return CpuBudget::Unlimited;
    }
    auth.query_cpu_limit_ms(&identity.roles)
        .map(CpuBudget::from_millis)
        .unwrap_or_default()
}

/// The most rows a query that `identity` runs may output. `None` means no limit.
///
/// 1. A super-user has no limit.
/// 2. Else the `query_output_row_limit` setting of the identity's roles applies,
///    set with `ALTER ROLE <role> SET query_output_row_limit = <n>`. The most
///    generous role wins, and `0` means no limit.
/// 3. Else there is no limit.
fn row_limit_for(auth: &AuthContext, identity: &AuthIdentity) -> Option<u64> {
    if identity.is_super_user {
        return None;
    }
    auth.query_output_row_limit(&identity.roles)
        .filter(|&limit| limit > 0)
}

/// Authorizes the reads of a plan for `identity`. Returns `Ok(())` when allowed.
///
/// The grant checks do not apply when `enforce` is false or the caller is a super-user. Only
/// `TableScan` reads are checked here.
async fn authorize_reads(
    facts: &PlanFacts<'_>,
    auth: &AuthContext,
    identity: &AuthIdentity,
    enforce: bool,
) -> anyhow::Result<()> {
    // Unconditional gate on the internal tables, checked BEFORE the enforcement/super-user early
    // return below. A database of an earlier version holds Argon2 hashes in `__beacon_users`, and
    // grant enforcement defaults OFF. Only the super-user may touch them, always.
    // `tests/auth_persistence.rs` pins that this fails closed with enforcement off.
    if !identity.is_super_user && facts.touches_internal_tables() {
        anyhow::bail!(
            "permission denied: the internal '{}*' tables are restricted to the super-user",
            INTERNAL_TABLE_PREFIX
        );
    }

    // Same treatment, same reason, for beacon's metadata schemas. `beacon.system`
    // is the auth directory plus what every user ran; `information_schema` names
    // every table in every catalog. Both describe the instance rather than hold
    // user data, so they belong to the super-user — and, exactly as above, that
    // cannot depend on enforcement being on. Regular callers do not query them:
    // they enumerate the catalog through `Runtime::visible_tables`, which returns
    // only the tables their roles grant.
    if !identity.is_super_user {
        if let Some(surface) = facts.metadata_surface() {
            anyhow::bail!("permission denied: {surface} is restricted to the super-user");
        }
    }

    if !enforce || identity.is_super_user {
        return Ok(());
    }

    // `DESCRIBE <table>` plans no scan and keeps no table name, so no grant can be checked.
    if facts.describes_table() {
        anyhow::bail!(
            "permission denied: DESCRIBE is restricted to the super-user when authorization is \
             enforced; read a table's schema with GET /api/table-schema"
        );
    }

    let checker = ReadChecker::new(facts, auth, identity);
    for read in &facts.reads() {
        checker.check(read).await?;
    }
    Ok(())
}

/// The spellings of `table` as rule targets.
fn table_targets(table: &TableFacts) -> Vec<ConcreteTarget> {
    table
        .names()
        .iter()
        .map(|name| ConcreteTarget::Table(name.clone()))
        .collect()
}

/// Checks the reads of one caller.
struct ReadChecker<'a> {
    facts: &'a PlanFacts<'a>,
    auth: &'a AuthContext,
    identity: &'a AuthIdentity,
    /// The tables a role of the caller denies, whose files a path read must not reach.
    denied_tables: Vec<String>,
}

impl<'a> ReadChecker<'a> {
    fn new(facts: &'a PlanFacts<'a>, auth: &'a AuthContext, identity: &'a AuthIdentity) -> Self {
        let denied_tables = auth
            .list_roles()
            .into_iter()
            .filter(|role| identity.roles.contains(&role.name))
            .flat_map(|role| role.denies.into_iter())
            .filter(|rule| matches!(rule.privilege, Privilege::Select | Privilege::All))
            .filter_map(|rule| match rule.target {
                Some(PrivilegeTarget::Table(name)) => Some(name),
                _ => None,
            })
            .collect();
        Self {
            facts,
            auth,
            identity,
            denied_tables,
        }
    }

    async fn check(&self, read: &PlanRead) -> anyhow::Result<()> {
        match read {
            PlanRead::Opaque(name) => {
                anyhow::bail!("permission denied: SELECT on '{name}': its files cannot be checked")
            }
            PlanRead::Table(table) => {
                if !self.auth.is_allowed_any(
                    &self.identity.roles,
                    Privilege::Select,
                    &table_targets(table),
                ) {
                    anyhow::bail!("permission denied: SELECT on table '{}'", table.name());
                }
                Ok(())
            }
            PlanRead::Files(locations) => {
                // The spelling can name a file the disk resolves in another case.
                for location in locations {
                    self.refuse_denied(&ConcreteTarget::Path(location.spelling()))?;
                }
                let files = self.facts.files(locations).await?;
                for file in &files {
                    self.require(&ConcreteTarget::Path(file.to_string()))?;
                }
                self.refuse_denied_tables(&files).await
            }
        }
    }

    /// Fails unless the caller may read `target`.
    fn require(&self, target: &ConcreteTarget) -> anyhow::Result<()> {
        if self
            .auth
            .is_allowed(&self.identity.roles, Privilege::Select, target)
        {
            Ok(())
        } else {
            anyhow::bail!("permission denied: SELECT on {}", describe_target(target))
        }
    }

    /// Fails when a deny of the caller matches `target`.
    fn refuse_denied(&self, target: &ConcreteTarget) -> anyhow::Result<()> {
        if self
            .auth
            .is_denied(&self.identity.roles, Privilege::Select, target)
        {
            anyhow::bail!("permission denied: SELECT on {}", describe_target(target))
        }
        Ok(())
    }

    /// Fails when one of `files` belongs to a table a role of the caller denies.
    async fn refuse_denied_tables(&self, files: &[ObjectPath]) -> anyhow::Result<()> {
        for table in &self.denied_tables {
            if self.facts.table_holds_any(table, files).await {
                anyhow::bail!("permission denied: SELECT on table '{table}'");
            }
        }
        Ok(())
    }
}

fn describe_target(target: &ConcreteTarget) -> String {
    match target {
        ConcreteTarget::Table(name) => format!("table '{name}'"),
        ConcreteTarget::Path(path) => format!("path '{path}'"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_auth::{AuthContext, AuthIdentity, BasicAuthProvider, Privilege, PrivilegeRule};
    use datafusion::{
        arrow::{
            array::Int32Array,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        datasource::MemTable,
        logical_expr::LogicalPlan,
        prelude::{SessionConfig, SessionContext},
    };

    use super::*;

    fn identity(roles: &[&str]) -> AuthIdentity {
        AuthIdentity {
            username: "alice".to_string(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            is_super_user: false,
        }
    }

    async fn ctx_with_table(name: &str) -> SessionContext {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(name, Arc::new(table)).unwrap();
        ctx
    }

    async fn plan_for(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        ctx.sql(sql).await.unwrap().into_unoptimized_plan()
    }

    async fn auth_with_reader_grant(grant: Option<PrivilegeRule>) -> AuthContext {
        let auth = AuthContext::new(Arc::new(BasicAuthProvider::new()));
        auth.create_role("reader").await.unwrap();
        if let Some(rule) = grant {
            auth.grant("reader", rule).await.unwrap();
        }
        auth
    }

    async fn authorize_logical_plan(
        plan: &LogicalPlan,
        ctx: &SessionContext,
        auth: &AuthContext,
        identity: &AuthIdentity,
        enforce: bool,
    ) -> anyhow::Result<()> {
        authorize_reads(&PlanFacts::new(plan, ctx), auth, identity, enforce).await
    }

    #[tokio::test]
    async fn named_table_denied_without_grant_allowed_with_grant() {
        let ctx = ctx_with_table("observations").await;
        let plan = plan_for(&ctx, "SELECT * FROM observations").await;

        let denied = auth_with_reader_grant(None).await;
        assert!(
            authorize_logical_plan(&plan, &ctx, &denied, &identity(&["reader"]), true)
                .await
                .is_err()
        );

        let allowed =
            auth_with_reader_grant(Some(PrivilegeRule::new(Privilege::Select, None))).await;
        assert!(
            authorize_logical_plan(&plan, &ctx, &allowed, &identity(&["reader"]), true)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn enforce_off_and_super_user_bypass() {
        let ctx = ctx_with_table("observations").await;
        let plan = plan_for(&ctx, "SELECT * FROM observations").await;
        let auth = auth_with_reader_grant(None).await;

        // enforce=false bypasses.
        assert!(
            authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), false)
                .await
                .is_ok()
        );

        // super-user bypasses even with enforce=true and no grants.
        let mut su = identity(&[]);
        su.is_super_user = true;
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &su, true)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn metadata_schemas_are_super_user_only_even_without_enforcement() {
        let ctx = ctx_with_table("observations").await;
        let auth = auth_with_reader_grant(Some(PrivilegeRule::new(Privilege::Select, None))).await;

        for sql in [
            "SELECT table_name FROM information_schema.tables",
            // `SHOW TABLES` is rewritten onto `information_schema.tables`, so the
            // scan-level gate catches it too.
            "SHOW TABLES",
            "SELECT count(*) FROM (SELECT * FROM information_schema.columns)",
        ] {
            let plan = plan_for(&ctx, sql).await;
            // Denied with enforcement on despite a blanket SELECT grant…
            let err = authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), true)
                .await
                .err()
                .unwrap_or_else(|| panic!("non-super read should be rejected: {sql}"));
            assert!(
                err.to_string().contains("super-user"),
                "expected a super-user error for `{sql}`, got: {err}"
            );
            // …and denied with enforcement off, where a grant-based gate would leak.
            assert!(
                authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), false)
                    .await
                    .is_err(),
                "`{sql}` must stay denied with enforcement off"
            );
        }

        // The super-user still reads them.
        let mut su = identity(&[]);
        su.is_super_user = true;
        let plan = plan_for(&ctx, "SELECT table_name FROM information_schema.tables").await;
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &su, true)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn the_validator_refuses_ddl_for_a_caller_that_is_not_a_super_user() {
        let ctx = ctx_with_table("observations").await;
        let auth = Arc::new(auth_with_reader_grant(None).await);
        let plan = ctx
            .state()
            .create_logical_plan("CREATE TABLE other (a INT)")
            .await
            .unwrap();
        let facts = PlanFacts::new(&plan, &ctx);
        let mut super_user = identity(&[]);
        super_user.is_super_user = true;

        let reader = RoleValidator::new(auth.clone(), identity(&["reader"]), false);
        let admin = RoleValidator::new(auth, super_user, false);

        assert!(reader.validate(&facts).await.is_err());
        assert!(admin.validate(&facts).await.is_ok());
    }

    #[tokio::test]
    async fn check_table_keeps_metadata_and_internal_tables_for_the_super_user() {
        let ctx = ctx_with_table("observations").await;
        let auth = Arc::new(auth_with_reader_grant(None).await);
        let reader = RoleValidator::new(auth, identity(&["reader"]), false);
        let table = |reference| TableFacts::new(reference, &ctx);

        let plain = reader.check_table(&table("observations".into()));
        let internal = reader.check_table(&table("__beacon_users".into()));
        let metadata = reader.check_table(&table("information_schema.tables".into()));

        assert!(plain.is_ok());
        assert!(internal.unwrap_err().to_string().contains("internal"));
        assert!(metadata
            .unwrap_err()
            .to_string()
            .contains("information_schema"));
    }

    #[tokio::test]
    async fn check_table_and_path_need_a_grant_when_enforcement_is_on() {
        let ctx = ctx_with_table("observations").await;
        let auth = Arc::new(auth_with_reader_grant(None).await);
        let table = TableFacts::new("observations".into(), &ctx);

        let open = RoleValidator::new(auth.clone(), identity(&["reader"]), false);
        let enforced = RoleValidator::new(auth, identity(&["reader"]), true);

        assert!(open.check_table(&table).is_ok());
        assert!(open.can_read_path("argo/a.nc"));
        assert!(enforced.check_table(&table).is_err());
        assert!(!enforced.can_read_path("argo/a.nc"));
    }

    #[tokio::test]
    async fn the_limits_come_from_the_role_settings() {
        use beacon_auth::{QUERY_CPU_LIMIT_MS, QUERY_OUTPUT_ROW_LIMIT};
        use std::time::Duration;

        let auth = Arc::new(AuthContext::new(Arc::new(BasicAuthProvider::new())));
        for role in ["analyst", "free", "plain"] {
            auth.create_role(role).await.unwrap();
        }
        for (role, cpu, rows) in [("analyst", "60000", "100"), ("free", "0", "0")] {
            auth.set_role_setting(role, QUERY_CPU_LIMIT_MS, cpu)
                .await
                .unwrap();
            auth.set_role_setting(role, QUERY_OUTPUT_ROW_LIMIT, rows)
                .await
                .unwrap();
        }
        let limits =
            |roles: &[&str]| RoleValidator::new(auth.clone(), identity(roles), false).limits();
        let limited = |secs, rows| QueryLimits {
            cpu_budget: CpuBudget::Limited(Duration::from_secs(secs)),
            output_row_limit: Some(rows),
        };
        let mut super_user = identity(&["analyst"]);
        super_user.is_super_user = true;

        assert_eq!(limits(&["analyst"]), limited(60, 100));
        // The most generous role wins, and `0` means no limit.
        assert_eq!(limits(&["analyst", "free"]), QueryLimits::default());
        assert_eq!(limits(&["plain"]), QueryLimits::default());
        assert_eq!(limits(&[]), QueryLimits::default());
        assert_eq!(
            RoleValidator::new(auth.clone(), super_user, false).limits(),
            QueryLimits::default()
        );

        // The roles are read live, so a change applies to the next query.
        auth.set_role_setting("analyst", QUERY_CPU_LIMIT_MS, "1000")
            .await
            .unwrap();
        assert_eq!(limits(&["analyst"]), limited(1, 100));
        auth.reset_role_setting("analyst", QUERY_CPU_LIMIT_MS)
            .await
            .unwrap();
        assert_eq!(limits(&["analyst"]).cpu_budget, CpuBudget::Unlimited);
    }
}
