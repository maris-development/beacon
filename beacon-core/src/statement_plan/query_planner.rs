//! The custom [`QueryPlanner`] and [`ExtensionPlanner`] that lower beacon's
//! statement logical nodes to physical execution plans.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};

use super::{logical, physical, SessionCell};

/// Beacon's [`QueryPlanner`]: delegates to DataFusion's
/// [`DefaultPhysicalPlanner`] but wires in [`BeaconExtensionPlanner`], which
/// lowers beacon's custom [`LogicalPlan::Extension`] statement nodes to their
/// physical counterparts. Any plan without beacon extension nodes is planned
/// exactly as the default planner would, so this is transparent for ordinary
/// queries.
pub(crate) struct BeaconQueryPlanner {
    session: SessionCell,
}

impl BeaconQueryPlanner {
    pub(crate) fn new(session: SessionCell) -> Self {
        Self { session }
    }
}

impl fmt::Debug for BeaconQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BeaconQueryPlanner").finish()
    }
}

#[async_trait]
impl QueryPlanner for BeaconQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            BeaconExtensionPlanner::new(self.session.clone()),
        )]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Lowers beacon's custom statement logical nodes to physical execution plans.
///
/// Returns `Ok(None)` for any node it does not recognize so the default planner
/// (and any other extension planners) handle the rest.
pub(crate) struct BeaconExtensionPlanner {
    // Injected into each lowered exec node so it can recover the SessionContext
    // (for `register_table`, catalog access) at execution time.
    session: SessionCell,
}

impl BeaconExtensionPlanner {
    pub(crate) fn new(session: SessionCell) -> Self {
        Self { session }
    }
}

#[async_trait]
impl ExtensionPlanner for BeaconExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let session = self.session.clone();

        if let Some(create) = any.downcast_ref::<logical::CreateMaterializedViewNode>() {
            return Ok(Some(Arc::new(physical::CreateMaterializedViewExec::new(
                create.view_name.clone(),
                create.query_sql.clone(),
                session,
            ))));
        }

        if let Some(refresh) = any.downcast_ref::<logical::RefreshNode>() {
            return Ok(Some(Arc::new(physical::RefreshExec::new(
                refresh.name.clone(),
                session,
            ))));
        }

        if let Some(drop) = any.downcast_ref::<logical::DropTableNode>() {
            return Ok(Some(Arc::new(physical::DropTableExec::new(
                drop.name.clone(),
                drop.if_exists,
                session,
            ))));
        }

        if let Some(create) = any.downcast_ref::<logical::CreateExternalTableNode>() {
            return Ok(Some(Arc::new(physical::CreateExternalTableExec::new(
                create.cmd.payload.clone(),
                session,
            ))));
        }

        if let Some(view) = any.downcast_ref::<logical::CreateViewNode>() {
            return Ok(Some(Arc::new(physical::CreateViewExec::new(
                view.name.clone(),
                view.input.clone(),
                view.definition.clone(),
                session,
            ))));
        }

        if let Some(alter) = any.downcast_ref::<logical::AlterTableNode>() {
            return Ok(Some(Arc::new(physical::AlterTableExec::new(
                alter.spec.payload.clone(),
                session,
            ))));
        }

        if let Some(create) = any.downcast_ref::<logical::CreateTableNode>() {
            return Ok(Some(Arc::new(physical::CreateTableExec::new(
                create.name.clone(),
                create.is_ctas,
                create.if_not_exists,
                physical_inputs[0].clone(),
                session,
            ))));
        }

        if let Some(insert) = any.downcast_ref::<logical::InsertNode>() {
            return Ok(Some(Arc::new(physical::InsertExec::new(
                insert.table.clone(),
                insert.op,
                physical_inputs[0].clone(),
                session,
            ))));
        }

        if let Some(replace) = any.downcast_ref::<logical::ReplaceTableContentsNode>() {
            return Ok(Some(Arc::new(physical::ReplaceTableContentsExec::new(
                replace.table.clone(),
                physical_inputs[0].clone(),
                session,
            ))));
        }

        // Unrecognized node: let the default planner handle it.
        Ok(None)
    }
}
