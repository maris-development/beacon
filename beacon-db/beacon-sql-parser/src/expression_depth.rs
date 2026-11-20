//! A limit on how deeply the expressions of a SQL statement nest.
//!
//! DataFusion plans an expression tree recursively. A deep enough tree, such as a chain of
//! thousands of `+` or `OR` operands, overflows the thread stack and ends the process. The
//! parser builds such a chain without recursion, so the statement reaches the planner. This
//! check refuses it first, with an error the client can read.

use std::ops::ControlFlow;

use datafusion::sql::parser::{CopyToSource, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{Expr, Visit, Visitor};

/// The deepest expression nesting a statement can hold.
pub const MAX_EXPRESSION_DEPTH: usize = 1_000;

/// Counts the open expressions on the way down and stops past the limit.
struct DepthVisitor {
    depth: usize,
}

impl Visitor for DepthVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<()> {
        self.depth += 1;
        if self.depth > MAX_EXPRESSION_DEPTH {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }
}

/// Whether `node` holds an expression nested deeper than [`MAX_EXPRESSION_DEPTH`].
fn too_deep<V: Visit>(node: &V) -> bool {
    node.visit(&mut DepthVisitor { depth: 0 }).is_break()
}

/// Refuses `statement` when an expression in it nests deeper than [`MAX_EXPRESSION_DEPTH`].
pub fn check_expression_depth(statement: &DFStatement) -> anyhow::Result<()> {
    let deep = match statement {
        DFStatement::Statement(statement) => too_deep(statement.as_ref()),
        DFStatement::CopyTo(copy) => match &copy.source {
            CopyToSource::Query(query) => too_deep(query.as_ref()),
            CopyToSource::Relation(_) => false,
        },
        DFStatement::Explain(explain) => {
            return check_expression_depth(explain.statement.as_ref());
        }
        DFStatement::CreateExternalTable(_) | DFStatement::Reset(_) => false,
    };
    if deep {
        anyhow::bail!(
            "the statement nests an expression deeper than the limit of \
             {MAX_EXPRESSION_DEPTH} levels (expression nesting depth); \
             write a long chain of OR or = comparisons as IN (...)"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::parser::DFParser;

    fn statement(sql: &str) -> DFStatement {
        DFParser::parse_sql(sql)
            .expect("valid SQL")
            .pop_front()
            .expect("one statement")
    }

    fn chain(terms: usize) -> String {
        format!("SELECT {}", vec!["1"; terms].join("+"))
    }

    #[test]
    fn a_chain_at_the_limit_passes() {
        // `n` operands nest `n - 1` operators above one leaf: `n` levels.
        assert!(check_expression_depth(&statement(&chain(MAX_EXPRESSION_DEPTH))).is_ok());
    }

    #[test]
    fn a_chain_past_the_limit_is_refused() {
        let err = check_expression_depth(&statement(&chain(MAX_EXPRESSION_DEPTH + 1)))
            .expect_err("one level past the limit");
        assert!(err.to_string().contains("nesting depth"), "{err}");
    }

    #[test]
    fn an_explain_is_checked_through() {
        let sql = format!("EXPLAIN {}", chain(MAX_EXPRESSION_DEPTH + 1));
        assert!(check_expression_depth(&statement(&sql)).is_err());
    }
}
