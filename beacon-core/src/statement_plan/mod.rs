//! Beacon's custom physical-planner path.
//!
//! The long-term goal is a single execution pipeline for everything beacon
//! runs: statements are lowered to DataFusion [`LogicalPlan::Extension`] nodes
//! and executed through `create_physical_plan` -> `execute_stream`, exactly like
//! ordinary `SELECT` queries (see `beacon-planner`), instead of being dispatched
//! to hand-written handlers that imperatively perform their side effects.
//!
//! Phase A (this module's initial form) only wires a custom [`QueryPlanner`] into
//! the session as a transparent pass-through, proving the bootstrap works.
//! Later phases lower individual statements onto it.
//!
//! [`LogicalPlan::Extension`]: datafusion::logical_expr::LogicalPlan::Extension
//! [`QueryPlanner`]: datafusion::execution::context::QueryPlanner

mod query_planner;

use std::sync::{Arc, OnceLock, Weak};

use datafusion::prelude::SessionContext;

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
