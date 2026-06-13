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

use super::SessionCell;

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
    // Filled once the context exists; read by node lowering in later phases.
    #[allow(dead_code)]
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
        _node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Phase A: beacon defines no extension nodes yet; everything is handled
        // by the default planner.
        Ok(None)
    }
}
