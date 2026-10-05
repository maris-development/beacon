//! The federation optimizer rule, with the plan roots that `datafusion-federation` misses.
//!
//! The upstream [`FederationOptimizerRule`] sends the whole plan to the remote when all of its
//! scans are remote. Two root shapes break that:
//!
//! - A statement root (`COPY`, `INSERT`, `CREATE TABLE AS`) has no SQL for the remote. It must
//!   stay local, and only its query input goes to the remote.
//! - A bare scan root is a leaf, and the upstream rule never federates a leaf. Beacon runs
//!   `OptimizeProjections` first, so `SELECT * FROM remote` reaches the rule as a bare scan.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_federation::{FederationOptimizerRule, get_table_source};

/// DataFusion's default logical rules, with [`BeaconFederationRule`] after `push_down_filter`.
///
/// A federated sub-plan is a sealed node, so no rule can move a filter into it later. The
/// upstream rule runs before `push_down_filter`, so a filter above a join with a local table
/// stays local. After `push_down_filter`, the filter is already below the join, and it goes to
/// the remote with the scan.
pub fn federation_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = Optimizer::new().rules;
    let position = rules
        .iter()
        .position(|rule| rule.name() == "push_down_filter")
        .map_or(rules.len(), |position| position + 1);
    rules.insert(position, Arc::new(BeaconFederationRule::default()));
    rules
}

/// [`FederationOptimizerRule`] that keeps statement roots local and federates a bare scan.
#[derive(Debug, Default)]
pub struct BeaconFederationRule {
    inner: FederationOptimizerRule,
}

impl OptimizerRule for BeaconFederationRule {
    fn name(&self) -> &str {
        "beacon_federation"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if is_statement(&plan) {
            return plan.map_children(|input| self.rewrite(input, config));
        }
        if let LogicalPlan::TableScan(scan) = &plan
            && get_table_source(&scan.source)?.is_some()
        {
            // An identity projection makes the scan an inner node, which the upstream rule federates.
            let columns = plan.schema().columns().into_iter().map(Expr::Column).collect();
            let projection = Projection::try_new(columns, Arc::new(plan.clone()))?;
            let federated = self.inner.rewrite(LogicalPlan::Projection(projection), config)?;
            return Ok(if federated.transformed {
                federated
            } else {
                Transformed::no(plan)
            });
        }
        self.inner.rewrite(plan, config)
    }
}

/// Is this a node with no SQL for the remote, whose inputs are queries of their own?
fn is_statement(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Copy(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::DescribeTable(_)
    )
}
