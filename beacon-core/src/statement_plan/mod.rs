//! Beacon's custom physical-planner path.
//!
//! The long-term goal is a single execution pipeline for everything beacon
//! runs: statements are lowered to DataFusion [`LogicalPlan::Extension`] nodes
//! and executed through `create_physical_plan` -> `execute_stream`, exactly like
//! ordinary `SELECT` queries (see `beacon-planner`), instead of being dispatched
//! to hand-written handlers that imperatively perform their side effects.
//!
//! `CREATE MATERIALIZED VIEW` and `REFRESH` are lowered here (see [`logical`] /
//! [`physical`]); other statements still run through the legacy handlers until
//! they are migrated in later phases.
//!
//! [`LogicalPlan::Extension`]: datafusion::logical_expr::LogicalPlan::Extension
//! [`QueryPlanner`]: datafusion::execution::context::QueryPlanner

mod logical;
pub(crate) mod materialized_view;
mod physical;
mod query_planner;

use std::sync::{Arc, OnceLock, Weak};

use datafusion::{
    execution::SendableRecordBatchStream,
    logical_expr::{Extension, LogicalPlan},
    prelude::SessionContext,
};

use crate::parser::statement::{CreateMaterializedViewStatement, RefreshStatement};

pub(crate) use query_planner::BeaconQueryPlanner;

/// Late-initialized, weak handle to the [`SessionContext`] shared with the
/// custom planner.
///
/// The context is built *from* the session state that owns the planner, so the
/// planner is constructed with an empty cell that is filled with a [`Weak`]
/// reference immediately after the context exists (see `Runtime::init_ctx`).
/// Beacon's custom execution-plan nodes only receive a `TaskContext` at
/// execution time, but their side effects (e.g. `register_table`, catalog
/// access) need the full `SessionContext`; this cell is how they recover it. A
/// `Weak` avoids the context -> state -> planner -> context reference cycle.
pub(crate) type SessionCell = Arc<OnceLock<Weak<SessionContext>>>;

/// Create an empty [`SessionCell`] to be filled once the context exists.
pub(crate) fn new_session_cell() -> SessionCell {
    Arc::new(OnceLock::new())
}

/// Build the logical plan for `CREATE MATERIALIZED VIEW <name> AS <query>`.
pub(crate) fn create_materialized_view_plan(
    statement: CreateMaterializedViewStatement,
) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::CreateMaterializedViewNode::new(
            statement.view_name.to_string(),
            statement.query_sql,
        )),
    })
}

/// Build the logical plan for `REFRESH [TABLE] <name>`.
pub(crate) fn refresh_plan(statement: RefreshStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::RefreshNode::new(statement.name.to_string())),
    })
}

/// Plan and execute a beacon statement logical plan through the single
/// `create_physical_plan` -> `execute_stream` pipeline, coalescing the result the
/// same way the legacy statement executor does.
pub(crate) async fn execute_statement_plan(
    session_ctx: &Arc<SessionContext>,
    plan: LogicalPlan,
) -> anyhow::Result<SendableRecordBatchStream> {
    let physical_plan = session_ctx.state().create_physical_plan(&plan).await?;
    let stream = datafusion::physical_plan::execute_stream(physical_plan, session_ctx.task_ctx())?;
    Ok(crate::statement_handlers::coalesce_sql_stream(stream))
}
