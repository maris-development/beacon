//! tabledap: SQL filters as ERDDAP constraints, the request query, and the parquet decode.

use arrow::datatypes::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Between, BinaryExpr, Cast, Expr, Like, Operator};

use crate::encode::{
    LiteralValue, erddap_string, java_regex_literal, like_to_regex, literal_value, query_part,
};

/// The constraints `expr` becomes, or `None` when it cannot be pushed.
///
/// Each result is a superset filter: DataFusion applies `expr` again above the scan.
pub fn translate(expr: &Expr, schema: &Schema) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let (Some(column), Expr::Literal(value, _)) = (column_name(left), right.as_ref()) {
                return comparison(schema, column, *op, value).map(|c| vec![c]);
            }
            if let (Expr::Literal(value, _), Some(column)) = (left.as_ref(), column_name(right)) {
                return comparison(schema, column, op.swap()?, value).map(|c| vec![c]);
            }
            None
        }
        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => {
            let column = column_name(expr)?;
            let (Expr::Literal(low, _), Expr::Literal(high, _)) = (low.as_ref(), high.as_ref())
            else {
                return None;
            };
            Some(vec![
                comparison(schema, column, Operator::GtEq, low)?,
                comparison(schema, column, Operator::LtEq, high)?,
            ])
        }
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) => in_list(schema, column_name(expr)?, list),
        Expr::Like(Like {
            negated: false,
            expr,
            pattern,
            escape_char: None,
            case_insensitive: false,
        }) => {
            let column = column_name(expr)?;
            if !is_string(schema, column)? {
                return None;
            }
            let Expr::Literal(value, _) = pattern.as_ref() else {
                return None;
            };
            let Some(LiteralValue::Text(p)) = literal_value(value) else {
                return None;
            };
            Some(vec![format!(
                "{column}=~{}",
                erddap_string(&like_to_regex(&p)?)
            )])
        }
        Expr::IsNull(inner) => nan_test(schema, inner, "="),
        Expr::IsNotNull(inner) => nan_test(schema, inner, "!="),
        _ => None,
    }
}

/// `vars` and `constraints` as an encoded query, with no leading `?`.
pub fn request_query(vars: &[String], constraints: &[String]) -> String {
    let mut query = vars
        .iter()
        .map(|v| query_part(v))
        .collect::<Vec<_>>()
        .join(",");
    for constraint in constraints {
        query.push('&');
        query.push_str(&query_part(constraint));
    }
    query
}

/// A column, also behind a numeric widening cast that DataFusion adds in coercion.
fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Cast(Cast { expr, data_type }) if data_type.is_numeric() => match expr.as_ref() {
            Expr::Column(column) => Some(column.name.as_str()),
            _ => None,
        },
        _ => None,
    }
}

/// `Some(true)` for a string column, `Some(false)` for numeric or time, `None` otherwise.
fn is_string(schema: &Schema, column: &str) -> Option<bool> {
    match schema.field_with_name(column).ok()?.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(true),
        t if t.is_numeric() || matches!(t, DataType::Timestamp(_, _)) => Some(false),
        _ => None,
    }
}

fn comparison(schema: &Schema, column: &str, op: Operator, value: &ScalarValue) -> Option<String> {
    let symbol = match op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return None,
    };
    match (is_string(schema, column)?, literal_value(value)?) {
        (true, LiteralValue::Text(s)) if matches!(op, Operator::Eq | Operator::NotEq) => {
            Some(format!("{column}{symbol}{}", erddap_string(&s)))
        }
        (false, LiteralValue::Number(n)) | (false, LiteralValue::Time(n)) => {
            Some(format!("{column}{symbol}{n}"))
        }
        _ => None,
    }
}

fn in_list(schema: &Schema, column: &str, list: &[Expr]) -> Option<Vec<String>> {
    let values: Vec<LiteralValue> = list
        .iter()
        .map(|e| match e {
            Expr::Literal(v, _) => literal_value(v),
            _ => None,
        })
        .collect::<Option<_>>()?;
    if values.is_empty() {
        return None;
    }
    if is_string(schema, column)? {
        let alternatives: Vec<String> = values
            .iter()
            .map(|v| match v {
                LiteralValue::Text(s) => Some(java_regex_literal(s)),
                _ => None,
            })
            .collect::<Option<_>>()?;
        return Some(vec![format!(
            "{column}=~{}",
            erddap_string(&alternatives.join("|"))
        )]);
    }
    // A numeric list becomes its min..max range, a superset of the list.
    let scalars: Vec<&ScalarValue> = list
        .iter()
        .filter_map(|e| match e {
            Expr::Literal(v, _) => Some(v),
            _ => None,
        })
        .collect();
    let by_order =
        |a: &&ScalarValue, b: &&ScalarValue| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal);
    let min = scalars.iter().copied().min_by(by_order)?;
    let max = scalars.iter().copied().max_by(by_order)?;
    Some(vec![
        comparison(schema, column, Operator::GtEq, min)?,
        comparison(schema, column, Operator::LtEq, max)?,
    ])
}

fn nan_test(schema: &Schema, inner: &Expr, op: &str) -> Option<Vec<String>> {
    let column = column_name(inner)?;
    if is_string(schema, column)? {
        return None;
    }
    Some(vec![format!("{column}{op}NaN")])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{cast, col, lit};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("temp", DataType::Float32, true),
            Field::new("depth", DataType::Int32, true),
            Field::new("ship", DataType::Utf8, true),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ])
    }

    fn t(expr: Expr) -> Option<Vec<String>> {
        translate(&expr, &schema())
    }

    #[test]
    fn numeric_comparisons_both_sides() {
        assert_eq!(t(col("depth").gt_eq(lit(5))), Some(vec!["depth>=5".into()]));
        assert_eq!(t(lit(5).lt(col("depth"))), Some(vec!["depth>5".into()]));
        assert_eq!(
            t(col("depth").not_eq(lit(5))),
            Some(vec!["depth!=5".into()])
        );
        assert_eq!(
            t(cast(col("temp"), DataType::Float64).gt(lit(1.5))),
            Some(vec!["temp>1.5".into()])
        );
    }

    #[test]
    fn time_comparison_uses_iso() {
        let ts = lit(ScalarValue::TimestampNanosecond(
            Some(1_577_836_800_000_000_000),
            None,
        ));
        assert_eq!(
            t(col("time").lt_eq(ts)),
            Some(vec!["time<=2020-01-01T00:00:00Z".into()])
        );
    }

    #[test]
    fn string_equality_only() {
        assert_eq!(
            t(col("ship").eq(lit("New \"Horizon\""))),
            Some(vec![r#"ship="New \"Horizon\"""#.into()])
        );
        assert_eq!(
            t(col("ship").not_eq(lit("x"))),
            Some(vec![r#"ship!="x""#.into()])
        );
        assert_eq!(t(col("ship").gt(lit("x"))), None);
    }

    #[test]
    fn in_lists() {
        assert_eq!(
            t(col("ship").in_list(vec![lit("a.b"), lit("c")], false)),
            Some(vec![r#"ship=~"a\\.b|c""#.into()])
        );
        assert_eq!(
            t(col("depth").in_list(vec![lit(9), lit(1), lit(5)], false)),
            Some(vec!["depth>=1".into(), "depth<=9".into()])
        );
        assert_eq!(t(col("depth").in_list(vec![lit(1)], true)), None);
    }

    #[test]
    fn like_and_nulls_and_between() {
        assert_eq!(
            t(col("ship").like(lit("Ne%"))),
            Some(vec![r#"ship=~"Ne.*""#.into()])
        );
        assert_eq!(t(col("ship").ilike(lit("ne%"))), None);
        assert_eq!(t(col("temp").is_null()), Some(vec!["temp=NaN".into()]));
        assert_eq!(t(col("temp").is_not_null()), Some(vec!["temp!=NaN".into()]));
        assert_eq!(t(col("ship").is_null()), None);
        assert_eq!(
            t(col("depth").between(lit(1), lit(3))),
            Some(vec!["depth>=1".into(), "depth<=3".into()])
        );
    }

    #[test]
    fn unsupported_shapes() {
        assert_eq!(t(col("depth").gt(lit(1)).or(col("depth").lt(lit(0)))), None);
        assert_eq!(t(col("depth").gt(col("temp"))), None);
        assert_eq!(t(col("nope").gt(lit(1))), None);
        assert_eq!(t(col("temp").gt(lit(f64::NAN))), None);
    }

    #[test]
    fn request_query_encodes_vars_and_constraints() {
        let q = request_query(
            &["ship".into(), "time".into()],
            &["time>=2020-01-01T00:00:00Z".into()],
        );
        assert_eq!(q, "ship,time&time%3E%3D2020-01-01T00%3A00%3A00Z");
    }
}
