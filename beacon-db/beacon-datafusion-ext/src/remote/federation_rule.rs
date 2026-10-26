//! The federation optimizer rule, with the plan roots that `datafusion-federation` misses.
//!
//! The upstream [`FederationOptimizerRule`] sends the whole plan to the remote when all of its
//! scans are remote. Two root shapes break that:
//!
//! - A statement root (`COPY`, `INSERT`, `CREATE TABLE AS`) has no SQL for the remote. It must
//!   stay local, and only its query input goes to the remote.
//! - A bare scan root is a leaf, and the upstream rule never federates a leaf. Beacon runs
//!   `OptimizeProjections` first, so `SELECT * FROM remote` reaches the rule as a bare scan.
//!
//! A sub-plan that calls a native plugin function also stays local, because the remote may not
//! have the plugin. The upstream rule has no hook for this, so this rule handles such a node
//! itself and federates only the sub-plans below it. A filter with such a call is split first:
//! its other conjuncts go into a filter below, which goes to the remote.

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{Expr, Filter, LogicalPlan, Projection};
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_federation::{FederationOptimizerRule, get_table_source};

/// The scalar functions that must run on this server, such as native plugin functions.
pub trait LocalFunctions: Debug + Send + Sync {
    fn contains(&self, name: &str) -> bool;

    fn is_empty(&self) -> bool;
}

impl LocalFunctions for HashSet<String> {
    fn contains(&self, name: &str) -> bool {
        HashSet::contains(self, name)
    }

    fn is_empty(&self) -> bool {
        HashSet::is_empty(self)
    }
}

/// Does `expr` call one of `functions`? Subquery plans are not part of `expr`.
fn expr_calls_any(expr: &Expr, functions: &dyn LocalFunctions) -> Result<bool> {
    expr.exists(|e| {
        Ok(matches!(e, Expr::ScalarFunction(call) if functions.contains(call.func.name())))
    })
}

/// Does any node of `plan`, or of its subqueries, call one of `functions`?
fn calls_any(plan: &LogicalPlan, functions: &dyn LocalFunctions) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        for expr in node.expressions() {
            if expr_calls_any(&expr, functions)? {
                found = true;
                break;
            }
        }
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

/// DataFusion's default logical rules, with [`BeaconFederationRule`] after `push_down_filter`.
///
/// A federated sub-plan is a sealed node, so no rule can move a filter into it later. The
/// upstream rule runs before `push_down_filter`, so a filter above a join with a local table
/// stays local. After `push_down_filter`, the filter is already below the join, and it goes to
/// the remote with the scan.
pub fn federation_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    federation_optimizer_rules_with(Arc::new(HashSet::new()))
}

/// [`federation_optimizer_rules`], with calls to `local_functions` kept local.
pub fn federation_optimizer_rules_with(
    local_functions: Arc<dyn LocalFunctions>,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = Optimizer::new().rules;
    let position = rules
        .iter()
        .position(|rule| rule.name() == "push_down_filter")
        .map_or(rules.len(), |position| position + 1);
    rules.insert(
        position,
        Arc::new(BeaconFederationRule::new(local_functions)),
    );
    rules
}

/// [`FederationOptimizerRule`] that keeps statement roots and local function calls local, and
/// federates a bare scan.
#[derive(Debug)]
pub struct BeaconFederationRule {
    inner: FederationOptimizerRule,
    local_functions: Arc<dyn LocalFunctions>,
}

impl Default for BeaconFederationRule {
    fn default() -> Self {
        Self::new(Arc::new(HashSet::new()))
    }
}

impl BeaconFederationRule {
    pub fn new(local_functions: Arc<dyn LocalFunctions>) -> Self {
        Self {
            inner: FederationOptimizerRule::default(),
            local_functions,
        }
    }
}

impl BeaconFederationRule {
    /// Splits a filter that calls a local function into a local filter above a filter
    /// with the other conjuncts, and federates the lower filter.
    ///
    /// Gives `None` when the filter has only local or only other conjuncts, or when no
    /// part of the lower filter goes to a remote.
    fn split_filter(
        &self,
        filter: &Filter,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<Transformed<LogicalPlan>>> {
        let mut local = Vec::new();
        let mut remote = Vec::new();
        for conjunct in split_conjunction(&filter.predicate) {
            // A subquery can call a local function in its own plan, so it stays local.
            if conjunct.contains_outer()
                || has_subquery(conjunct)?
                || expr_calls_any(conjunct, self.local_functions.as_ref())?
            {
                local.push(conjunct.clone());
            } else {
                remote.push(conjunct.clone());
            }
        }
        let (Some(local), Some(remote)) = (conjunction(local), conjunction(remote)) else {
            return Ok(None);
        };
        let lower = LogicalPlan::Filter(Filter::try_new(remote, Arc::clone(&filter.input))?);
        let lower = self.rewrite(lower, config)?;
        // Without a remote below, the split only adds a node.
        if !lower.transformed {
            return Ok(None);
        }
        let upper = Filter::try_new(local, Arc::new(lower.data))?;
        Ok(Some(Transformed::yes(LogicalPlan::Filter(upper))))
    }
}

/// Does `expr` hold a subquery?
fn has_subquery(expr: &Expr) -> Result<bool> {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
        ))
    })
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
        if !self.local_functions.is_empty() && calls_any(&plan, self.local_functions.as_ref())? {
            if let LogicalPlan::Filter(filter) = &plan
                && let Some(split) = self.split_filter(filter, config)?
            {
                return Ok(split);
            }
            // This node or one below it calls a plugin function, so only its inputs can go remote.
            return plan
                .map_children(|input| self.rewrite(input, config))?
                .transform_data(|plan| {
                    plan.map_subqueries(|subquery| {
                        subquery.map_children(|p| self.rewrite(p, config))
                    })
                });
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::MemTable;
    use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::optimizer::optimize_projections::OptimizeProjections;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
    use datafusion_federation::sql::{
        RemoteTable, SQLExecutor, SQLFederationProvider, SQLTableSource,
    };
    use datafusion_federation::{FederatedPlanNode, FederatedTableProviderAdaptor};

    /// A remote that only plans. The tests never execute a query.
    struct PlanOnlyRemote;

    #[async_trait]
    impl SQLExecutor for PlanOnlyRemote {
        fn name(&self) -> &str {
            "plan_only_remote"
        }

        fn compute_context(&self) -> Option<String> {
            Some("plan_only_remote".to_string())
        }

        fn dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(DefaultDialect {})
        }

        fn execute(
            &self,
            _query: &str,
            _schema: SchemaRef,
            _filters: &[Arc<dyn PhysicalExpr>],
        ) -> Result<SendableRecordBatchStream> {
            datafusion::common::not_impl_err!("plan only")
        }

        async fn table_names(&self) -> Result<Vec<String>> {
            Ok(vec!["obs".to_string()])
        }

        async fn get_table_schema(&self, _table_name: &str) -> Result<SchemaRef> {
            Ok(obs_schema())
        }
    }

    fn obs_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]))
    }

    /// A local function in the place of a native plugin function.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct PluginFn {
        signature: Signature,
    }

    impl ScalarUDFImpl for PluginFn {
        fn name(&self) -> &str {
            "plugin_fn"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(args.args[0].clone())
        }
    }

    fn context(names: Arc<dyn LocalFunctions>) -> SessionContext {
        let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
            vec![Arc::new(OptimizeProjections::new())];
        rules.extend(federation_optimizer_rules_with(names));
        let state = SessionStateBuilder::new()
            .with_optimizer_rules(rules)
            .with_default_features()
            .build();
        let context = SessionContext::new_with_state(state);
        let provider = Arc::new(SQLFederationProvider::new(Arc::new(PlanOnlyRemote)));
        let remote = RemoteTable::new(TableReference::parse_str("obs").into(), obs_schema());
        let source = SQLTableSource::new_with_table(provider, Arc::new(remote));
        let table = FederatedTableProviderAdaptor::new(Arc::new(source));
        context
            .register_table("remote_obs", Arc::new(table))
            .unwrap();
        let local = MemTable::try_new(obs_schema(), vec![vec![]]).unwrap();
        context
            .register_table("local_obs", Arc::new(local))
            .unwrap();
        context.register_udf(ScalarUDF::from(PluginFn {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }));
        context
    }

    /// The optimized plan, and whether each federated sub-plan calls `plugin_fn`.
    async fn optimize(names: Arc<dyn LocalFunctions>, sql: &str) -> (LogicalPlan, Vec<bool>) {
        let plan = context(names)
            .sql(sql)
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let probe = HashSet::from(["plugin_fn".to_string()]);
        let mut federated = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Extension(extension) = node
                && let Some(node) = extension.node.as_any().downcast_ref::<FederatedPlanNode>()
            {
                federated.push(calls_any(node.plan(), &probe)?);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        (plan, federated)
    }

    fn plugin_names() -> HashSet<String> {
        HashSet::from(["plugin_fn".to_string(), "plugin_alias".to_string()])
    }

    const QUERIES: [&str; 2] = [
        "SELECT plugin_fn(x) FROM remote_obs WHERE x > 1",
        "SELECT x FROM remote_obs WHERE plugin_fn(x) > 1",
    ];

    #[tokio::test]
    async fn a_plugin_call_stays_above_the_remote_scan() {
        for sql in QUERIES {
            let (plan, federated) = optimize(Arc::new(plugin_names()), sql).await;
            assert_eq!(federated, [false], "{sql}\n{plan}");
            assert!(calls_any(&plan, &plugin_names()).unwrap(), "{sql}\n{plan}");
        }
    }

    /// The federated sub-plans of `plan`, as indented text.
    fn federated_plans(plan: &LogicalPlan) -> Vec<String> {
        let mut plans = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Extension(extension) = node
                && let Some(node) = extension.node.as_any().downcast_ref::<FederatedPlanNode>()
            {
                plans.push(node.plan().display_indent().to_string());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        plans
    }

    #[tokio::test]
    async fn the_other_conjuncts_of_a_plugin_filter_go_to_the_remote() {
        let sql = "SELECT x FROM remote_obs WHERE plugin_fn(x) > 1 AND x > 5";
        let (plan, federated) = optimize(Arc::new(plugin_names()), sql).await;
        assert_eq!(federated, [false], "{plan}");
        let remote = federated_plans(&plan);
        assert!(remote[0].contains("x > Float64(5)"), "{plan}");
        let text = plan.display_indent().to_string();
        assert!(
            text.contains("Filter: plugin_fn(remote_obs.x) > Float64(1)\n"),
            "{plan}"
        );
    }

    #[tokio::test]
    async fn a_plugin_filter_on_a_local_table_is_not_split() {
        let sql = "SELECT x FROM local_obs WHERE plugin_fn(x) > 1 AND x > 5";
        let (plan, federated) = optimize(Arc::new(plugin_names()), sql).await;
        assert!(federated.is_empty(), "{plan}");
        let text = plan.display_indent().to_string();
        assert_eq!(text.matches("Filter:").count(), 1, "{plan}");
    }

    #[tokio::test]
    async fn a_subquery_conjunct_stays_local() {
        let sql = "SELECT x FROM remote_obs \
                   WHERE plugin_fn(x) > 1 AND x IN (SELECT x FROM local_obs)";
        let (plan, _) = optimize(Arc::new(plugin_names()), sql).await;
        for remote in federated_plans(&plan) {
            assert!(!remote.contains("local_obs"), "{plan}");
        }
    }

    #[tokio::test]
    async fn a_function_that_is_not_a_plugin_goes_to_the_remote() {
        for sql in QUERIES {
            let (plan, federated) = optimize(Arc::new(HashSet::new()), sql).await;
            assert_eq!(federated, [true], "{sql}\n{plan}");
        }
    }
}
