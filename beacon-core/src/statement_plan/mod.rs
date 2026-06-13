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

mod actions;
mod logical;
mod lower;
pub(crate) mod materialized_view;
mod physical;
mod query_planner;
mod stream_coalescer;

use std::sync::{Arc, OnceLock, Weak};

use datafusion::{
    execution::SendableRecordBatchStream,
    logical_expr::{Extension, LogicalPlan},
    prelude::SessionContext,
};

use crate::parser::statement::{CreateMaterializedViewStatement, RefreshStatement};

pub(crate) use lower::lower_df_statement;
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
///
/// Side-effecting statements (DDL, `DELETE`/`UPDATE`, materialized-view ops)
/// produce no rows, i.e. an empty output schema. Those are driven to completion
/// here so the side effect is performed and any error surfaces eagerly from
/// `run_sql` — as the legacy handlers did — rather than only when the caller
/// drains the stream. Row-producing statements (`SELECT`, `INSERT`, `COPY`) keep
/// streaming lazily.
pub(crate) async fn execute_statement_plan(
    session_ctx: &Arc<SessionContext>,
    plan: LogicalPlan,
) -> anyhow::Result<SendableRecordBatchStream> {
    use futures::TryStreamExt;

    let physical_plan = session_ctx.state().create_physical_plan(&plan).await?;
    let stream = datafusion::physical_plan::execute_stream(physical_plan, session_ctx.task_ctx())?;
    let stream = stream_coalescer::coalesce_sql_stream(stream);

    if stream.schema().fields().is_empty() {
        let schema = stream.schema();
        stream.try_collect::<Vec<_>>().await?;
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            ),
        ))
    } else {
        Ok(stream)
    }
}
