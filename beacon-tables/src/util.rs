use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::prelude::col;
use std::collections::HashMap;

pub fn remap_filter(expr: Expr, rename_map: &HashMap<String, String>) -> Result<Expr> {
    // expr.transform returns Result<Transformed<Expr>, DataFusionError>
    let transformed: Transformed<Expr> = expr.transform(&|e| {
        Ok(match e {
            // For any column reference...
            Expr::Column(ref c) => {
                // look up the real name
                if let Some(real) = rename_map.get(c.name()) {
                    // yes, replace this Expr with col(real)
                    Transformed::yes(col(real))
                } else {
                    // no change
                    Transformed::no(e.clone())
                }
            }
            // leave everything else alone
            _ => Transformed::no(e.clone()),
        })
    })?;
    // Turn the Transformed<Expr> back into an Expr
    Ok(transformed.data)
}

