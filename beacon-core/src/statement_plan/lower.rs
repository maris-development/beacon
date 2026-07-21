//! Lowering of parsed DataFusion statements into logical plans.
//!
//! Most statements are DataFusion-standard `LogicalPlan` nodes that the custom
//! [`BeaconQueryPlanner`](super::query_planner::BeaconQueryPlanner) turns into
//! beacon execution plans directly. Only a few are rewritten here:
//!
//! - `ALTER TABLE` — DataFusion cannot plan it, so it is built from the AST.
//! - `DELETE`/`UPDATE` — lowered to a copy-on-write [`ReplaceTableContentsNode`].
//!   This must happen *before* optimization: the surviving/updated rows are
//!   derived from the predicate, which the optimizer may otherwise push into the
//!   table scan, changing the plan's shape.
//! - `COPY` — only its output path is rewritten into the datasets object store.
//!
//! Everything else passes through unchanged.

use std::sync::Arc;

use datafusion::{
    logical_expr::{
        dml::CopyTo, not, when, DmlStatement, Expr, Extension, Filter, LogicalPlan,
        LogicalPlanBuilder, WriteOp,
    },
    prelude::{lit, SessionContext},
    sql::sqlparser::ast::{AlterTableOperation, ObjectName, Statement as SqlAstStatement},
};

use super::logical::{AlterTableNode, AlterTableSpec, Keyed, Mutation, ReplaceTableContentsNode};

/// Render a DataFusion `Expr` back to a SQL string (best-effort). Used to derive
/// native Lance `DELETE`/`UPDATE` predicates and `SET` values; `None` if the
/// expression cannot be unparsed, in which case the caller falls back to the
/// copy-on-write path.
///
/// Column references are stripped of their table qualifier first: Lance parses
/// the predicate/value against the dataset schema, which has bare field names
/// (`id`), so a qualified `t.id` would not resolve.
fn unparse_expr(expr: &Expr) -> Option<String> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::common::Column;

    let unqualified = expr
        .clone()
        .transform(|e| {
            Ok(match e {
                Expr::Column(c) => Transformed::yes(Expr::Column(Column::new_unqualified(c.name))),
                other => Transformed::no(other),
            })
        })
        .ok()?
        .data;

    // Emit bare (unquoted) identifiers: `name = 'b'`, not `"name" = 'b'`. Lance's
    // predicate parser mis-evaluates a predicate that mixes a double-quoted
    // identifier with a single-quoted string literal (it matches zero rows),
    // whereas the bare form parses correctly. Column names are already simple
    // (dataset fields), so dropping the quotes is safe here.
    let dialect = datafusion::sql::unparser::dialect::CustomDialectBuilder::new().build();
    datafusion::sql::unparser::Unparser::new(&dialect)
        .expr_to_sql(&unqualified)
        .ok()
        .map(|ast| ast.to_string())
}

/// Lower a parsed DataFusion statement to a logical plan. Permission checks are
/// applied later by [`validate_query_plan`](super::validate_query_plan), once the
/// plan is fully lowered.
pub(crate) async fn lower_df_statement(
    session_ctx: &Arc<SessionContext>,
    statement: datafusion::sql::parser::Statement,
) -> anyhow::Result<LogicalPlan> {
    // DataFusion has no `ALTER TABLE` planning, so build the node from the AST.
    if let datafusion::sql::parser::Statement::Statement(sql_stmt) = &statement {
        if let SqlAstStatement::AlterTable(alter) = sql_stmt.as_ref() {
            return Ok(alter_table_plan(
                alter.name.clone(),
                alter.operations.clone(),
            ));
        }
    }

    let state = session_ctx.state();
    let plan = state.statement_to_plan(statement).await?;
    rewrite_logical_plan(plan)
}

/// Rewrite only the statements the planner cannot handle from their standard
/// form (copy-on-write `DELETE`/`UPDATE`, and `COPY`'s output path); pass
/// everything else (DDL, `INSERT`, `SELECT`, ...) through unchanged.
fn rewrite_logical_plan(plan: LogicalPlan) -> anyhow::Result<LogicalPlan> {
    match plan {
        LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Delete) => delete_plan(dml),
        LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Update) => update_plan(dml),
        LogicalPlan::Copy(copy) => Ok(rewrite_copy(copy)),
        other => Ok(other),
    }
}

fn extension(node: Arc<dyn datafusion::logical_expr::UserDefinedLogicalNode>) -> LogicalPlan {
    LogicalPlan::Extension(Extension { node })
}

fn alter_table_plan(name: ObjectName, operations: Vec<AlterTableOperation>) -> LogicalPlan {
    let key = name.to_string();
    extension(Arc::new(AlterTableNode {
        spec: Keyed::new(key, AlterTableSpec { name, operations }),
    }))
}

/// `DELETE FROM t [WHERE p]`: the surviving rows are `NOT p` (or none when there
/// is no `WHERE`), and they replace the table's contents.
fn delete_plan(dml: DmlStatement) -> anyhow::Result<LogicalPlan> {
    let (keep_plan, predicate) = match dml.input.as_ref() {
        LogicalPlan::Filter(filter) => {
            let keep = Filter::try_new(not(filter.predicate.clone()), filter.input.clone())?;
            (LogicalPlan::Filter(keep), Some(filter.predicate.clone()))
        }
        scan @ LogicalPlan::TableScan(_) => {
            // No WHERE clause: delete every row -> keep nothing.
            let keep = Filter::try_new(lit(false), Arc::new(scan.clone()))?;
            (LogicalPlan::Filter(keep), None)
        }
        other => {
            return Err(anyhow::anyhow!(
                "Unsupported DELETE plan shape: {}",
                other.display()
            ));
        }
    };

    // Native delete spec (best-effort). A WHERE whose predicate cannot be
    // unparsed yields `None` -> copy-on-write (never a native delete-all).
    let mutation = match &predicate {
        Some(expr) => unparse_expr(expr).map(|sql| Mutation::Delete {
            predicate: Some(sql),
        }),
        None => Some(Mutation::Delete { predicate: None }),
    };

    Ok(replace_contents_plan(dml.table_name, keep_plan, mutation))
}

/// `UPDATE t SET col = expr [WHERE p]`: rebuild the full post-update table as a
/// single `CASE WHEN p THEN <new> ELSE <old> END` projection over the unfiltered
/// scan, which then replaces the table's contents.
fn update_plan(dml: DmlStatement) -> anyhow::Result<LogicalPlan> {
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Err(anyhow::anyhow!(
            "Unsupported UPDATE plan shape: {}",
            dml.input.display()
        ));
    };

    let new_contents = match projection.input.as_ref() {
        LogicalPlan::Filter(filter) => {
            let scan = filter.input.as_ref();
            // Positional column mapping assumes the scan's columns are exactly
            // the table's columns (no FROM/join).
            if projection.expr.len() != scan.schema().fields().len() {
                return Err(anyhow::anyhow!("UPDATE ... FROM / joins are not supported"));
            }
            let scan_cols = scan.schema().columns();
            let predicate = &filter.predicate;

            let case_exprs = projection
                .expr
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    let name = projection.schema.field(i).name().clone();
                    let new_value = expr.clone().unalias();
                    let old_value = Expr::Column(scan_cols[i].clone());
                    Ok(when(predicate.clone(), new_value)
                        .otherwise(old_value)?
                        .alias(name))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            LogicalPlanBuilder::from(scan.clone())
                .project(case_exprs)?
                .build()?
        }
        // No WHERE clause: the projection already covers every row.
        _ => dml.input.as_ref().clone(),
    };

    let mutation = update_mutation(projection);
    Ok(replace_contents_plan(
        dml.table_name,
        new_contents,
        mutation,
    ))
}

/// Derive a native `UPDATE` spec from the planned projection (best-effort):
/// the changed columns become `SET` assignments and the filter becomes the
/// `WHERE`. Returns `None` (→ copy-on-write) if the shape is unexpected or any
/// expression cannot be unparsed.
fn update_mutation(projection: &datafusion::logical_expr::Projection) -> Option<Mutation> {
    let (scan, predicate) = match projection.input.as_ref() {
        LogicalPlan::Filter(filter) => (filter.input.as_ref(), Some(filter.predicate.clone())),
        scan => (scan, None),
    };
    if projection.expr.len() != scan.schema().fields().len() {
        return None;
    }
    let scan_cols = scan.schema().columns();

    let mut assignments = Vec::new();
    for (i, expr) in projection.expr.iter().enumerate() {
        let new_value = expr.clone().unalias();
        // Unchanged columns project the column through unchanged; skip those.
        if new_value == Expr::Column(scan_cols[i].clone()) {
            continue;
        }
        let name = projection.schema.field(i).name().clone();
        assignments.push((name, unparse_expr(&new_value)?));
    }

    let predicate = match predicate {
        Some(expr) => Some(unparse_expr(&expr)?),
        None => None,
    };
    Some(Mutation::Update {
        predicate,
        assignments,
    })
}

fn replace_contents_plan(
    table: datafusion::sql::TableReference,
    input: LogicalPlan,
    mutation: Option<Mutation>,
) -> LogicalPlan {
    extension(Arc::new(ReplaceTableContentsNode {
        table,
        input,
        mutation,
    }))
}

/// `COPY ... TO` stays a standard DataFusion node (which it can plan + execute);
/// only the output path is rewritten into the datasets object store.
fn rewrite_copy(mut copy: CopyTo) -> LogicalPlan {
    LogicalPlan::Copy(copy)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc as StdArc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col;

    use super::super::logical::ReplaceTableContentsNode;

    /// A session with a single `t(id BIGINT, name VARCHAR)` table, enough to plan
    /// the `DELETE`/`UPDATE` statements this module rewrites.
    fn session() -> SessionContext {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ctx = SessionContext::new();
        ctx.register_table("t", StdArc::new(MemTable::try_new(schema, vec![vec![]]).unwrap()))
            .unwrap();
        ctx
    }

    /// Plan `sql` and run it through the lowering rewrite, returning the resulting
    /// `ReplaceTableContentsNode` (every `DELETE`/`UPDATE` lowers to one).
    async fn replace_node(sql: &str) -> (LogicalPlan, Option<Mutation>) {
        let plan = session().state().create_logical_plan(sql).await.unwrap();
        let lowered = rewrite_logical_plan(plan).expect("statement should lower");
        let LogicalPlan::Extension(extension) = lowered else {
            panic!("`{sql}` did not lower to an extension node");
        };
        let node = extension
            .node
            .as_any()
            .downcast_ref::<ReplaceTableContentsNode>()
            .expect("expected a ReplaceTableContents node");
        (node.input.clone(), node.mutation.clone())
    }

    /// Lance parses the derived predicate against the *dataset* schema, whose
    /// fields are unqualified and unquoted. A qualified `t.id` would not resolve,
    /// and a double-quoted identifier makes Lance's parser match zero rows, so the
    /// unparsed SQL must be bare on both counts.
    #[test]
    fn unparsed_predicates_are_unqualified_and_unquoted() {
        let qualified = col("t.name").eq(lit("b"));
        assert_eq!(unparse_expr(&qualified).as_deref(), Some("(name = 'b')"));

        let compound = col("t.id").gt(lit(1i64)).and(col("t.name").is_not_null());
        assert_eq!(
            unparse_expr(&compound).as_deref(),
            Some("((id > 1) AND name IS NOT NULL)")
        );
    }

    /// `DELETE ... WHERE p` keeps the rows that do *not* match, and derives a
    /// native delete spec carrying the original predicate. Getting the negation
    /// backwards would delete exactly the wrong rows in the copy-on-write path.
    #[tokio::test]
    async fn delete_with_predicate_keeps_the_complement_and_derives_a_native_spec() {
        let (input, mutation) = replace_node("DELETE FROM t WHERE id = 1").await;

        let LogicalPlan::Filter(filter) = &input else {
            panic!("expected a filter over the scan, got {}", input.display());
        };
        assert!(
            matches!(&filter.predicate, Expr::Not(_)),
            "surviving rows must be the negated predicate, got {}",
            filter.predicate
        );
        assert_eq!(
            mutation,
            Some(Mutation::Delete {
                predicate: Some("(id = 1)".to_string())
            })
        );
    }

    /// `DELETE` with no `WHERE` removes every row: copy-on-write keeps nothing
    /// (`false` filter) and the native spec is an unconditional delete. A `None`
    /// predicate here must never be confused with "could not unparse".
    #[tokio::test]
    async fn delete_without_predicate_keeps_nothing() {
        let (input, mutation) = replace_node("DELETE FROM t").await;

        let LogicalPlan::Filter(filter) = &input else {
            panic!("expected a filter over the scan, got {}", input.display());
        };
        assert_eq!(filter.predicate, lit(false));
        assert_eq!(mutation, Some(Mutation::Delete { predicate: None }));
    }

    /// `UPDATE ... SET c = v WHERE p` rebuilds the whole table as one
    /// `CASE WHEN p THEN v ELSE c END` projection, and the native spec lists only
    /// the *changed* column — projecting an unchanged column through must not be
    /// mistaken for an assignment.
    #[tokio::test]
    async fn update_derives_only_the_changed_assignments() {
        let (input, mutation) = replace_node("UPDATE t SET name = 'b' WHERE id = 1").await;

        // The copy-on-write input projects every column over the *unfiltered* scan.
        let LogicalPlan::Projection(projection) = &input else {
            panic!("expected a projection, got {}", input.display());
        };
        assert_eq!(projection.expr.len(), 2);
        assert!(
            matches!(projection.input.as_ref(), LogicalPlan::TableScan(_)),
            "the projection must cover every row, not a filtered subset"
        );

        assert_eq!(
            mutation,
            Some(Mutation::Update {
                predicate: Some("(id = 1)".to_string()),
                assignments: vec![("name".to_string(), "'b'".to_string())],
            })
        );
    }

    /// `UPDATE` with no `WHERE` applies to every row, so there is no predicate to
    /// derive and the projection is used as planned.
    #[tokio::test]
    async fn update_without_predicate_has_no_native_predicate() {
        let (_, mutation) = replace_node("UPDATE t SET name = 'b'").await;
        assert_eq!(
            mutation,
            Some(Mutation::Update {
                predicate: None,
                assignments: vec![("name".to_string(), "'b'".to_string())],
            })
        );
    }

    /// Only `DELETE`/`UPDATE`/`COPY` are rewritten; a `SELECT` must pass through
    /// untouched, or every ordinary query would be reshaped by the lowering pass.
    #[tokio::test]
    async fn other_statements_pass_through_unchanged() {
        let plan = session()
            .state()
            .create_logical_plan("SELECT id FROM t")
            .await
            .unwrap();
        let lowered = rewrite_logical_plan(plan.clone()).unwrap();
        assert_eq!(format!("{}", lowered.display_indent()), format!("{}", plan.display_indent()));
    }
}
