//! Give every `SELECT` in a federated statement a select list.
//!
//! A count needs rows, not columns, so `OptimizeProjections` leaves a projection with no
//! expression under the aggregate. The SQL unparser writes that projection as `SELECT FROM t`.
//! PostgreSQL accepts an empty select list. MySQL and SQL Server reject it with a syntax error.
//! [`fill_empty_select_lists`] writes `SELECT 1 FROM t` instead, which every engine accepts.

use std::ops::ControlFlow;

use datafusion::error::Result;
use datafusion::sql::sqlparser::ast::{
    Expr, Query, SelectItem, SetExpr, Statement, Value, VisitMut, VisitorMut,
};

/// Add the constant `1` to each select list that has no item.
///
/// The constant adds a column that the plan does not expect. Under an aggregate or a join the
/// outer query does not read it. At the root of the statement, the executor drops it (see
/// [`crate::executor::FilteringSqlExecutor`]).
pub(crate) fn fill_empty_select_lists(mut statement: Statement) -> Result<Statement> {
    let _ = statement.visit(&mut EmptySelectListFiller);
    Ok(statement)
}

struct EmptySelectListFiller;

impl VisitorMut for EmptySelectListFiller {
    type Break = ();

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        fill_set_expr(&mut query.body);
        ControlFlow::Continue(())
    }
}

/// Fill the selects of one query body. A nested query is a separate [`Query`] node and the
/// visitor reaches it on its own.
fn fill_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) if select.projection.is_empty() => {
            let one = "1".parse().expect("1 is a valid SQL number");
            select
                .projection
                .push(SelectItem::UnnamedExpr(Expr::value(Value::Number(
                    one, false,
                ))));
        }
        SetExpr::SetOperation { left, right, .. } => {
            fill_set_expr(left);
            fill_set_expr(right);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn rewrite(sql: &str) -> String {
        let mut statements =
            Parser::parse_sql(&GenericDialect {}, sql).expect("the SQL should parse");
        let statement = statements.pop().expect("one statement");
        fill_empty_select_lists(statement).unwrap().to_string()
    }

    /// The statement that a count with a filter sends to the database.
    #[test]
    fn a_derived_table_without_columns_selects_one() {
        assert_eq!(
            rewrite(
                "SELECT count(1) AS `count(*)` FROM (SELECT FROM `companies` \
                 WHERE (`companies`.`revenue` > 40.0)) AS `derived_projection`"
            ),
            "SELECT count(1) AS `count(*)` FROM (SELECT 1 FROM `companies` \
             WHERE (`companies`.`revenue` > 40.0)) AS `derived_projection`"
        );
    }

    #[test]
    fn a_root_select_without_columns_selects_one() {
        assert_eq!(
            rewrite("SELECT FROM `obs` AS `o`"),
            "SELECT 1 FROM `obs` AS `o`"
        );
    }

    #[test]
    fn both_sides_of_a_set_operation_select_one() {
        assert_eq!(
            rewrite("SELECT FROM a UNION ALL SELECT FROM b"),
            "SELECT 1 FROM a UNION ALL SELECT 1 FROM b"
        );
    }

    #[test]
    fn a_select_list_with_items_does_not_change() {
        let sql = "SELECT `id` FROM (SELECT `id` FROM `obs`) AS `o`";
        assert_eq!(rewrite(sql), sql);
    }
}
