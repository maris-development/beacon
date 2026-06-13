//! Lowering of parsed DataFusion statements into beacon's
//! [`LogicalPlan::Extension`] nodes (or, for `COPY`, a rewritten standard node).
//!
//! After lowering, the plan is executed through the single
//! `create_physical_plan` -> `execute_stream` pipeline like any query.

use std::sync::Arc;

use beacon_data_lake::DATASETS_OBJECT_STORE_URL;
use datafusion::{
    logical_expr::{
        dml::CopyTo, not, when, CreateExternalTable, CreateMemoryTable, CreateView, DdlStatement,
        DmlStatement, DropTable, Expr, Extension, Filter, LogicalPlan, LogicalPlanBuilder, WriteOp,
    },
    prelude::{lit, SessionContext, SQLOptions},
    sql::sqlparser::ast::{AlterTableOperation, ObjectName, Statement as SqlAstStatement},
};

use super::logical::{
    AlterTableNode, AlterTableSpec, CreateExternalTableNode, CreateTableNode, CreateViewNode,
    DropTableNode, InsertNode, Keyed, ReplaceTableContentsNode,
};

/// Lower a parsed DataFusion statement to a logical plan, applying beacon's
/// permission gating (`verify_plan`) on the **standard** plan before any rewrite
/// to extension nodes (which `verify_plan` cannot see).
pub(crate) async fn lower_df_statement(
    session_ctx: &Arc<SessionContext>,
    statement: datafusion::sql::parser::Statement,
    sql_options: &SQLOptions,
) -> anyhow::Result<LogicalPlan> {
    // DataFusion has no `ALTER TABLE` planning, so build the node from the AST.
    if let datafusion::sql::parser::Statement::Statement(sql_stmt) = &statement {
        if let SqlAstStatement::AlterTable(alter) = sql_stmt.as_ref() {
            return Ok(alter_table_plan(alter.name.clone(), alter.operations.clone()));
        }
    }

    let state = session_ctx.state();
    let plan = state.statement_to_plan(statement).await?;
    sql_options.verify_plan(&plan)?;
    rewrite_logical_plan(plan)
}

/// Replace the top-level DDL/DML/Copy node with a beacon extension node (or a
/// rewritten `Copy`); pass anything else (e.g. `SELECT`) through unchanged.
fn rewrite_logical_plan(plan: LogicalPlan) -> anyhow::Result<LogicalPlan> {
    match plan {
        LogicalPlan::Ddl(DdlStatement::DropTable(drop)) => Ok(drop_table_plan(drop)),
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => {
            Ok(create_external_table_plan(cmd))
        }
        LogicalPlan::Ddl(DdlStatement::CreateView(view)) => Ok(create_view_plan(view)),
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(table)) => Ok(create_table_plan(table)),
        LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Insert(_)) => Ok(insert_plan(dml)),
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

fn drop_table_plan(drop: DropTable) -> LogicalPlan {
    extension(Arc::new(DropTableNode {
        name: drop.name,
        if_exists: drop.if_exists,
    }))
}

fn create_external_table_plan(cmd: CreateExternalTable) -> LogicalPlan {
    let key = cmd.name.to_string();
    extension(Arc::new(CreateExternalTableNode {
        cmd: Keyed::new(key, cmd),
    }))
}

fn create_view_plan(view: CreateView) -> LogicalPlan {
    extension(Arc::new(CreateViewNode {
        name: view.name,
        input: view.input.as_ref().clone(),
        definition: view.definition,
    }))
}

fn create_table_plan(table: CreateMemoryTable) -> LogicalPlan {
    // A bare `CREATE TABLE t (cols...)` lowers to an empty input relation; CTAS
    // carries a real query plan to populate the table from.
    let is_ctas = !matches!(table.input.as_ref(), LogicalPlan::EmptyRelation(_));
    extension(Arc::new(CreateTableNode {
        name: table.name,
        input: table.input.as_ref().clone(),
        is_ctas,
        if_not_exists: table.if_not_exists,
    }))
}

fn insert_plan(dml: DmlStatement) -> LogicalPlan {
    let op = match dml.op {
        WriteOp::Insert(op) => op,
        _ => unreachable!("guarded by rewrite_logical_plan"),
    };
    extension(Arc::new(InsertNode {
        table: dml.table_name,
        op,
        input: dml.input.as_ref().clone(),
    }))
}

/// `DELETE FROM t [WHERE p]`: the surviving rows are `NOT p` (or none when there
/// is no `WHERE`), and they replace the table's contents.
fn delete_plan(dml: DmlStatement) -> anyhow::Result<LogicalPlan> {
    let keep_plan = match dml.input.as_ref() {
        LogicalPlan::Filter(filter) => {
            let keep = Filter::try_new(not(filter.predicate.clone()), filter.input.clone())?;
            LogicalPlan::Filter(keep)
        }
        scan @ LogicalPlan::TableScan(_) => {
            // No WHERE clause: delete every row -> keep nothing.
            let keep = Filter::try_new(lit(false), Arc::new(scan.clone()))?;
            LogicalPlan::Filter(keep)
        }
        other => {
            return Err(anyhow::anyhow!(
                "Unsupported DELETE plan shape: {}",
                other.display()
            ));
        }
    };

    Ok(replace_contents_plan(dml.table_name, keep_plan))
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

    Ok(replace_contents_plan(dml.table_name, new_contents))
}

fn replace_contents_plan(table: datafusion::sql::TableReference, input: LogicalPlan) -> LogicalPlan {
    extension(Arc::new(ReplaceTableContentsNode { table, input }))
}

/// `COPY ... TO` stays a standard DataFusion node (which it can plan + execute);
/// only the output path is rewritten into the datasets object store.
fn rewrite_copy(mut copy: CopyTo) -> LogicalPlan {
    copy.output_url = format!("{}{}", *DATASETS_OBJECT_STORE_URL, copy.output_url);
    LogicalPlan::Copy(copy)
}
