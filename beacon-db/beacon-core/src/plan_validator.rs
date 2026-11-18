//! The access check that the engine calls for each statement.
//!
//! The engine knows no users and no roles. It plans a statement, gives the facts of the plan to a
//! [`PlanValidator`], and runs the plan only after the validator permits it. The caller of the
//! engine supplies the validator, so the caller owns the access rules.
//!
//! [`AllowAll`] is the validator of an embedded database: the process that opens the database has
//! full access.

use crate::query_cpu::CpuBudget;
pub use crate::statement_plan::{FileLocation, PlanFacts, PlanRead, TableFacts};

/// The limits that one query runs under.
///
/// The default is no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLimits {
    /// The CPU time that planning and execution can use together.
    pub cpu_budget: CpuBudget,
    /// The number of rows that the query can output. `None` means no limit.
    pub output_row_limit: Option<u64>,
}

/// Decides what one caller can do.
///
/// One validator stands for one caller. A server builds a validator for each request.
///
/// `Send + Sync` is necessary because a query moves to the executor threads. `async_trait` makes
/// an `async fn` usable through `dyn PlanValidator`, which plain Rust traits do not yet permit.
#[async_trait::async_trait]
pub trait PlanValidator: Send + Sync {
    /// The name of the caller, for the query metrics.
    fn caller(&self) -> &str;

    /// The limits of a query of this caller. The engine reads them before it plans the query.
    fn limits(&self) -> QueryLimits;

    /// Permits or refuses a planned statement.
    ///
    /// # Arguments
    ///
    /// * `facts` - The plan, the tables and the files that the statement reaches
    ///
    /// # Errors
    ///
    /// Returns an error when the caller cannot run the statement. The engine gives the message to
    /// the caller and does not run the plan.
    async fn validate(&self, facts: &PlanFacts<'_>) -> anyhow::Result<()>;

    /// Permits or refuses a read of the schema of `table`, and its place in a catalog list.
    ///
    /// # Errors
    ///
    /// Returns an error when the caller cannot read the table.
    fn check_table(&self, table: &TableFacts) -> anyhow::Result<()>;

    /// Whether the caller can read the file at `path`, relative to the datasets root.
    fn can_read_path(&self, path: &str) -> bool;
}

/// The validator that permits everything, with no limits.
///
/// # Examples
///
/// ```
/// use beacon_core::plan_validator::{AllowAll, PlanValidator, QueryLimits};
///
/// let validator = AllowAll;
/// assert_eq!(validator.limits(), QueryLimits::default());
/// assert!(validator.can_read_path("argo/profile.nc"));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AllowAll;

/// The caller name that [`AllowAll`] gives to the query metrics.
pub const ALLOW_ALL_CALLER: &str = "system";

#[async_trait::async_trait]
impl PlanValidator for AllowAll {
    fn caller(&self) -> &str {
        ALLOW_ALL_CALLER
    }

    fn limits(&self) -> QueryLimits {
        QueryLimits::default()
    }

    async fn validate(&self, _facts: &PlanFacts<'_>) -> anyhow::Result<()> {
        Ok(())
    }

    fn check_table(&self, _table: &TableFacts) -> anyhow::Result<()> {
        Ok(())
    }

    fn can_read_path(&self, _path: &str) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{prelude::SessionContext, sql::TableReference};

    use super::*;

    #[tokio::test]
    async fn allow_all_permits_a_privileged_statement() {
        let ctx = SessionContext::new();
        let plan = ctx
            .state()
            .create_logical_plan("CREATE TABLE t (a INT)")
            .await
            .unwrap();
        let facts = PlanFacts::new(&plan, &ctx);

        let verdict = AllowAll.validate(&facts).await;

        assert!(verdict.is_ok());
    }

    #[test]
    fn allow_all_permits_each_table_and_has_no_limits() {
        let ctx = SessionContext::new();
        let internal = TableFacts::new(TableReference::bare("__beacon_users"), &ctx);

        assert!(AllowAll.check_table(&internal).is_ok());
        assert!(AllowAll.can_read_path("any/path.nc"));
        assert_eq!(AllowAll.limits(), QueryLimits::default());
        assert_eq!(AllowAll.caller(), ALLOW_ALL_CALLER);
    }
}
