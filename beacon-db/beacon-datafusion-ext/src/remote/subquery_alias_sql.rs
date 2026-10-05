//! Keep an aliased subquery a derived table when a federated plan becomes SQL.
//!
//! The DataFusion unparser merges a `SubqueryAlias` over a `Filter`, `Sort`, `Limit` or `Join`
//! into the enclosing `SELECT`, and puts the alias on the table. The inner expressions keep the
//! table name, so `(SELECT * FROM obs WHERE depth < 2) w WHERE w.depth < 1` becomes
//! `FROM obs AS w WHERE w.depth < 1 AND obs.depth < 2`, and the remote cannot resolve `obs.depth`.
//!
//! [`project_aliased_subqueries`] puts an identity projection under each such alias. The
//! unparser then writes a derived table, `FROM (SELECT ... WHERE obs.depth < 2) AS w`.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, SubqueryAlias};

/// Put an identity projection under every alias that the unparser would merge.
///
/// The projection keeps the input schema, so every node keeps its declared schema.
pub fn project_aliased_subqueries(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up(|node| match node {
        LogicalPlan::SubqueryAlias(alias) if merges_into_outer_select(&alias.input) => {
            let input = Arc::clone(&alias.input);
            let columns = input.schema().columns().into_iter().map(Expr::Column).collect();
            let projection = LogicalPlan::Projection(Projection::try_new(columns, input)?);
            let alias = SubqueryAlias::try_new(Arc::new(projection), alias.alias)?;
            Ok(Transformed::yes(LogicalPlan::SubqueryAlias(alias)))
        }
        other => Ok(Transformed::no(other)),
    })
    .map(|result| result.data)
}

fn merges_into_outer_select(input: &LogicalPlan) -> bool {
    matches!(
        input,
        LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::Join(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::functions_aggregate::count::count_all;
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};
    use datafusion::prelude::SessionContext;
    use datafusion::sql::unparser::Unparser;

    /// A session with `obs`, whose `depth` column holds 0 to 9.
    fn session() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![Field::new("depth", DataType::Int64, false)]));
        let depth = Int64Array::from_iter_values(0..10);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(depth)]).unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("obs", Arc::new(table)).unwrap();
        ctx
    }

    /// `SELECT count(*) FROM (SELECT * FROM obs WHERE depth < 2) w WHERE w.depth < 1`, in the
    /// shape `OptimizeProjections` leaves: no projection between the alias and the inner filter.
    async fn filtered_subquery_plan(ctx: &SessionContext) -> LogicalPlan {
        let source = datafusion::datasource::provider_as_source(ctx.table_provider("obs").await.unwrap());
        LogicalPlanBuilder::scan("obs", source, None)
            .unwrap()
            .filter(col("obs.depth").lt(lit(2i64)))
            .unwrap()
            .alias("w")
            .unwrap()
            .filter(col("w.depth").lt(lit(1i64)))
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![count_all()])
            .unwrap()
            .build()
            .unwrap()
    }

    async fn count(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<i64> {
        let batches = ctx.sql(sql).await?.collect().await?;
        Ok(batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64")
            .value(0))
    }

    #[tokio::test]
    async fn an_outer_filter_on_a_filtered_subquery_unparses_to_valid_sql() {
        let ctx = session();
        let plan = filtered_subquery_plan(&ctx).await;

        let merged = Unparser::default().plan_to_sql(&plan).unwrap().to_string();
        assert!(
            count(&ctx, &merged).await.is_err(),
            "the merged SQL should name a table that its alias hides: {merged}"
        );

        let projected = project_aliased_subqueries(plan.clone()).unwrap();
        assert_eq!(projected.schema(), plan.schema(), "the rewrite keeps the schema");
        let sql = Unparser::default().plan_to_sql(&projected).unwrap().to_string();
        assert_eq!(count(&ctx, &sql).await.unwrap(), 1, "only depth 0 matches: {sql}");
    }

    #[tokio::test]
    async fn the_rewrite_runs_once_per_alias() {
        let ctx = session();
        let once = project_aliased_subqueries(filtered_subquery_plan(&ctx).await).unwrap();
        let twice = project_aliased_subqueries(once.clone()).unwrap();
        assert_eq!(once, twice, "an alias over a projection stays as it is");
    }
}
