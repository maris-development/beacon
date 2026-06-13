//! Beacon's custom statement [`LogicalPlan::Extension`] nodes.
//!
//! Each node carries just enough to perform its side effect; they produce no
//! rows, so they share an empty output schema. The matching physical nodes live
//! in [`super::physical`].
//!
//! [`LogicalPlan::Extension`]: datafusion::logical_expr::LogicalPlan::Extension

use std::sync::{Arc, OnceLock};

use datafusion::{
    common::{DFSchema, DFSchemaRef},
    error::Result,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
};

/// Shared empty schema returned by beacon's side-effecting statement nodes,
/// which produce no rows. `schema()` must return a reference, so the schema is
/// stored once rather than rebuilt per node.
fn empty_schema() -> &'static DFSchemaRef {
    static EMPTY: OnceLock<DFSchemaRef> = OnceLock::new();
    EMPTY.get_or_init(|| Arc::new(DFSchema::empty()))
}

/// Logical node for `CREATE MATERIALIZED VIEW <name> AS <query>`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct CreateMaterializedViewNode {
    pub(crate) view_name: String,
    pub(crate) query_sql: String,
}

impl CreateMaterializedViewNode {
    pub(crate) fn new(view_name: String, query_sql: String) -> Self {
        Self {
            view_name,
            query_sql,
        }
    }
}

impl UserDefinedLogicalNodeCore for CreateMaterializedViewNode {
    fn name(&self) -> &str {
        "CreateMaterializedView"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateMaterializedView: name={}", self.view_name)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            view_name: self.view_name.clone(),
            query_sql: self.query_sql.clone(),
        })
    }
}

/// Logical node for `REFRESH [TABLE] <name>`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct RefreshNode {
    pub(crate) name: String,
}

impl RefreshNode {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl UserDefinedLogicalNodeCore for RefreshNode {
    fn name(&self) -> &str {
        "Refresh"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Refresh: name={}", self.name)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::Extension;

    /// Wrapping a node in `LogicalPlan::Extension` and displaying the plan
    /// exercises `fmt_for_explain`; this is what `EXPLAIN` would render once the
    /// statement reaches the logical-plan layer.
    #[test]
    fn create_materialized_view_node_explains() {
        let node = CreateMaterializedViewNode::new("mv1".to_string(), "SELECT 1".to_string());
        assert_eq!(node.name(), "CreateMaterializedView");
        assert!(node.inputs().is_empty());
        assert_eq!(node.schema().fields().len(), 0);

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        let display = format!("{}", plan.display_indent());
        assert!(
            display.contains("CreateMaterializedView: name=mv1"),
            "unexpected explain output: {display}"
        );
    }

    #[test]
    fn refresh_node_explains() {
        let node = RefreshNode::new("my_table".to_string());
        assert_eq!(node.name(), "Refresh");
        assert!(node.inputs().is_empty());
        assert_eq!(node.schema().fields().len(), 0);

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        let display = format!("{}", plan.display_indent());
        assert!(
            display.contains("Refresh: name=my_table"),
            "unexpected explain output: {display}"
        );
    }
}
