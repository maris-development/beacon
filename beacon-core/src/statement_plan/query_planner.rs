//! The custom [`QueryPlanner`] and [`ExtensionPlanner`] that lower beacon's
//! statement logical nodes to physical execution plans.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{dml::InsertOp, DdlStatement, LogicalPlan, UserDefinedLogicalNode, WriteOp},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};

use super::{logical, physical, SessionCell};

/// Beacon's [`QueryPlanner`].
///
/// DataFusion's parser produces standard [`LogicalPlan::Ddl`]/[`LogicalPlan::Dml`]
/// nodes for most statements, but its default physical planner would execute
/// them against in-memory tables rather than beacon's Iceberg/catalog backends.
/// This planner therefore intercepts those standard nodes and builds beacon's own
/// execution plans (planning their inputs with the default planner), and wires in
/// [`BeaconExtensionPlanner`] for the few operations DataFusion has no logical
/// plan for (materialized views, `REFRESH`, `ALTER TABLE`, copy-on-write
/// replacement). Everything else — `SELECT`, `COPY`, the planned inputs — is left
/// to the default planner.
pub(crate) struct BeaconQueryPlanner {
    session: SessionCell,
}

impl BeaconQueryPlanner {
    pub(crate) fn new(session: SessionCell) -> Self {
        Self { session }
    }

    fn default_planner(&self) -> DefaultPhysicalPlanner {
        DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(BeaconExtensionPlanner::new(self.session.clone())),
            // Lowers `datafusion-federation`'s `Federated` extension node (the
            // pushed-down remote sub-plan) into its virtual Flight SQL scan.
            Arc::new(datafusion_federation::FederatedPlanner::new()),
        ])
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
        let default = self.default_planner();
        let session = self.session.clone();

        match logical_plan {
            LogicalPlan::Ddl(DdlStatement::DropTable(drop)) => Ok(Arc::new(
                physical::DropTableExec::new(drop.name.clone(), drop.if_exists, session),
            )),

            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => Ok(Arc::new(
                physical::CreateExternalTableExec::new(cmd.clone(), session),
            )),

            LogicalPlan::Ddl(DdlStatement::CreateView(view)) => {
                Ok(Arc::new(physical::CreateViewExec::new(
                    view.name.clone(),
                    view.input.as_ref().clone(),
                    view.definition.clone(),
                    session,
                )))
            }

            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(table)) => {
                // The optimizer has already optimized the (CTAS) input; plan it as
                // the child whose rows populate the table.
                let child = default
                    .create_physical_plan(table.input.as_ref(), session_state)
                    .await?;
                let is_ctas = !matches!(table.input.as_ref(), LogicalPlan::EmptyRelation(_));
                Ok(Arc::new(physical::CreateTableExec::new(
                    table.name.clone(),
                    is_ctas,
                    table.if_not_exists,
                    child,
                    session,
                )))
            }

            LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Insert(_)) => {
                let WriteOp::Insert(op) = &dml.op else {
                    unreachable!("guarded by matches! above")
                };
                let op: InsertOp = *op;
                let child = default
                    .create_physical_plan(dml.input.as_ref(), session_state)
                    .await?;
                Ok(Arc::new(physical::InsertExec::new(
                    dml.table_name.clone(),
                    op,
                    child,
                    session,
                )))
            }

            // SELECT, COPY, beacon extension nodes (MV/REFRESH/ALTER/replace), and
            // the planned inputs above are all handled by the default planner.
            other => default.create_physical_plan(other, session_state).await,
        }
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

        if let Some(alter) = any.downcast_ref::<logical::AlterTableNode>() {
            return Ok(Some(Arc::new(physical::AlterTableExec::new(
                alter.spec.payload.clone(),
                session,
            ))));
        }

        if let Some(replace) = any.downcast_ref::<logical::ReplaceTableContentsNode>() {
            return Ok(Some(Arc::new(physical::ReplaceTableContentsExec::new(
                replace.table.clone(),
                physical_inputs[0].clone(),
                replace.mutation.clone(),
                session,
            ))));
        }

        if let Some(create) = any.downcast_ref::<logical::CreateCrawlerNode>() {
            return Ok(Some(Arc::new(physical::CreateCrawlerExec::new(
                create.name.clone(),
                create.target_prefix.clone(),
                create.options.clone(),
                session,
            ))));
        }

        if let Some(run) = any.downcast_ref::<logical::RunCrawlerNode>() {
            return Ok(Some(Arc::new(physical::RunCrawlerExec::new(
                run.name.clone(),
                session,
            ))));
        }

        if let Some(drop) = any.downcast_ref::<logical::DropCrawlerNode>() {
            return Ok(Some(Arc::new(physical::DropCrawlerExec::new(
                drop.name.clone(),
                session,
            ))));
        }

        if any.downcast_ref::<logical::ShowCrawlersNode>().is_some() {
            return Ok(Some(Arc::new(physical::ShowCrawlersExec::new(session))));
        }

        if let Some(create) = any.downcast_ref::<logical::CreateIndexNode>() {
            return Ok(Some(Arc::new(physical::CreateIndexExec::new(
                create.table.clone(),
                create.column.clone(),
                create.name.clone(),
                create.using.clone(),
                session,
            ))));
        }

        if let Some(drop) = any.downcast_ref::<logical::DropIndexNode>() {
            return Ok(Some(Arc::new(physical::DropIndexExec::new(
                drop.table.clone(),
                drop.name.clone(),
                session,
            ))));
        }

        if let Some(show) = any.downcast_ref::<logical::ShowIndexesNode>() {
            return Ok(Some(Arc::new(physical::ShowIndexesExec::new(
                show.table.clone(),
                session,
            ))));
        }

        // Unrecognized node: let the default planner handle it.
        Ok(None)
    }
}
