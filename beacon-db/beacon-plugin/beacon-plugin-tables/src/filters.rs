//! Decides which filters can cross to a plugin.
//!
//! The plugin decodes a filter with the DataFusion default functions only. A filter that
//! calls another function, such as a Beacon function, fails to decode in the plugin.

use std::collections::HashSet;
use std::sync::LazyLock;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;

/// The function names of a default DataFusion session.
static DEFAULT_FUNCTIONS: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let state = SessionContext::new().state();
    state
        .scalar_functions()
        .keys()
        .chain(state.aggregate_functions().keys())
        .chain(state.window_functions().keys())
        .cloned()
        .collect()
});

/// True when every function that `expr` calls is a DataFusion default function.
pub(crate) fn pushable(expr: &Expr) -> bool {
    let mut ok = true;
    let _ = expr.apply(|node| {
        let name = match node {
            Expr::ScalarFunction(call) => Some(call.func.name()),
            Expr::AggregateFunction(call) => Some(call.func.name()),
            Expr::WindowFunction(call) => Some(call.fun.name()),
            _ => None,
        };
        if name.is_some_and(|name| !DEFAULT_FUNCTIONS.contains(name)) {
            ok = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    ok
}
