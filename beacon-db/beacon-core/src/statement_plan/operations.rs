//! Per-role rules for the operations a query may use, such as a join or a
//! `GROUP BY`. A role forbids one with
//! `ALTER ROLE <role> SET query_allow_<operation> = false`; see
//! [`QueryOperation`].
//!
//! The check reads the plan before optimization, as the user wrote it. The
//! optimizer turns a `DISTINCT` into an aggregate and a subquery into a join,
//! so a check after it would refuse operations the user never wrote. Views are
//! already inlined at this point, so a view does not hide the operations in it.

use std::collections::BTreeSet;

use beacon_auth::{AuthContext, AuthIdentity, QueryOperation};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::LogicalPlan;

/// Refuses `plan` when the roles of `identity` forbid an operation in it.
///
/// A super-user may use every operation.
pub(crate) fn authorize_operations(
    plan: &LogicalPlan,
    auth: &AuthContext,
    identity: &AuthIdentity,
) -> anyhow::Result<()> {
    if identity.is_super_user {
        return Ok(());
    }
    let forbidden: Vec<String> = plan_operations(plan)
        .into_iter()
        .filter(|&operation| !auth.operation_allowed(&identity.roles, operation))
        .map(|operation| operation.to_string())
        .collect();
    if !forbidden.is_empty() {
        anyhow::bail!(
            "operation not permitted: your roles do not allow {}",
            forbidden.join(", ")
        );
    }
    Ok(())
}

/// Every operation in `plan`, including the subqueries in its expressions.
pub(crate) fn plan_operations(plan: &LogicalPlan) -> BTreeSet<QueryOperation> {
    let mut operations = BTreeSet::new();
    collect(plan, false, &mut operations);
    operations
}

/// Adds the operations of `plan` and its inputs to `operations`.
///
/// `under_limit` is true when a `LIMIT` is above `plan` with only projections
/// between them, so a sort here is a cheap top-k and not a full `ORDER BY`.
fn collect(plan: &LogicalPlan, under_limit: bool, operations: &mut BTreeSet<QueryOperation>) {
    match plan {
        LogicalPlan::Join(_) => {
            operations.insert(QueryOperation::Join);
        }
        LogicalPlan::Aggregate(aggregate) if !aggregate.group_expr.is_empty() => {
            operations.insert(QueryOperation::GroupBy);
        }
        LogicalPlan::Distinct(_) => {
            operations.insert(QueryOperation::GroupBy);
        }
        LogicalPlan::Window(_) => {
            operations.insert(QueryOperation::Window);
        }
        LogicalPlan::Sort(sort) if !under_limit && sort.fetch.is_none() => {
            operations.insert(QueryOperation::OrderBy);
        }
        LogicalPlan::Union(_) => {
            operations.insert(QueryOperation::Union);
        }
        LogicalPlan::Subquery(_) => {
            operations.insert(QueryOperation::Subquery);
        }
        LogicalPlan::RecursiveQuery(_) => {
            operations.insert(QueryOperation::Recursive);
        }
        LogicalPlan::Unnest(_) => {
            operations.insert(QueryOperation::Unnest);
        }
        _ => {}
    }

    // A limit with a row count makes the sort below it a top-k.
    let child_under_limit = match plan {
        LogicalPlan::Limit(limit) => limit.fetch.is_some(),
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => under_limit,
        _ => false,
    };
    for input in plan.inputs() {
        collect(input, child_under_limit, operations);
    }
    // Subqueries in expressions arrive as `LogicalPlan::Subquery`.
    let _ = plan.apply_subqueries(|subquery| {
        collect(subquery, false, operations);
        Ok(TreeNodeRecursion::Continue)
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_auth::BasicAuthProvider;
    use datafusion::prelude::SessionContext;

    use super::*;
    use QueryOperation::*;

    async fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        for sql in [
            "CREATE TABLE a (k INT, v INT, l INT[])",
            "CREATE TABLE b (k INT, w INT)",
            "CREATE VIEW ab AS SELECT a.k, v, w FROM a JOIN b ON a.k = b.k",
        ] {
            ctx.sql(sql).await.unwrap();
        }
        ctx
    }

    async fn operations(ctx: &SessionContext, sql: &str) -> Vec<QueryOperation> {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        plan_operations(&plan).into_iter().collect()
    }

    #[tokio::test]
    async fn each_operation_maps_to_its_plan_node() {
        let ctx = ctx().await;
        for (sql, expected) in [
            ("SELECT k FROM a", vec![]),
            ("SELECT * FROM a JOIN b ON a.k = b.k", vec![Join]),
            ("SELECT * FROM a, b", vec![Join]),
            (
                "SELECT k FROM a INTERSECT SELECT k FROM b",
                vec![Join, GroupBy],
            ),
            ("SELECT * FROM ab", vec![Join]),
            ("SELECT k, count(*) FROM a GROUP BY k", vec![GroupBy]),
            ("SELECT DISTINCT k FROM a", vec![GroupBy]),
            ("SELECT count(*) FROM a", vec![]),
            (
                "SELECT k, row_number() OVER (ORDER BY v) FROM a",
                vec![Window],
            ),
            ("SELECT k FROM a ORDER BY v", vec![OrderBy]),
            ("SELECT k FROM a ORDER BY v LIMIT 5", vec![]),
            (
                "SELECT * FROM (SELECT k FROM a ORDER BY v LIMIT 5) t",
                vec![],
            ),
            ("SELECT k FROM a UNION ALL SELECT k FROM b", vec![Union]),
            (
                "SELECT k FROM a WHERE k IN (SELECT k FROM b)",
                vec![Subquery],
            ),
            (
                "SELECT k FROM a WHERE EXISTS (SELECT 1 FROM b)",
                vec![Subquery],
            ),
            ("WITH c AS (SELECT k FROM a) SELECT * FROM c", vec![]),
            ("SELECT * FROM (SELECT k FROM a) t", vec![]),
            (
                "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) \
                 SELECT * FROM r",
                // The recursive query holds its own `UNION ALL`.
                vec![Recursive],
            ),
            ("SELECT unnest(l) FROM a", vec![Unnest]),
        ] {
            assert_eq!(operations(&ctx, sql).await, expected, "{sql}");
        }
    }

    /// An operation inside a subquery counts, as the subquery runs it too.
    #[tokio::test]
    async fn an_operation_inside_a_subquery_counts() {
        let ctx = ctx().await;
        let sql = "SELECT k FROM a WHERE k IN (SELECT k FROM b GROUP BY k)";
        assert_eq!(operations(&ctx, sql).await, vec![GroupBy, Subquery]);
    }

    #[tokio::test]
    async fn the_roles_decide_and_the_super_user_is_free() {
        let ctx = ctx().await;
        let auth = AuthContext::new(Arc::new(BasicAuthProvider::new()));
        auth.create_role("strict").await.unwrap();
        for operation in [Join, Window] {
            auth.set_role_setting("strict", operation.setting_key(), "false")
                .await
                .unwrap();
        }
        let mut user = AuthIdentity::empty();
        user.roles = vec!["strict".to_string()];
        let plan = ctx
            .state()
            .create_logical_plan(
                "SELECT a.k, row_number() OVER (ORDER BY v) FROM a JOIN b ON a.k = b.k",
            )
            .await
            .unwrap();

        let error = authorize_operations(&plan, &auth, &user).unwrap_err();
        assert_eq!(
            error.to_string(),
            "operation not permitted: your roles do not allow JOIN, window functions"
        );
        authorize_operations(&plan, &auth, &AuthIdentity::system())
            .expect("a super-user may use every operation");
        authorize_operations(&plan, &auth, &AuthIdentity::empty())
            .expect("no role forbids anything");
    }
}
