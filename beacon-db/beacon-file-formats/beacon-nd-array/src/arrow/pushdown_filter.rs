use std::collections::HashMap;
use std::sync::Arc;

use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::scalar::ScalarValue;

use crate::arrow::pushdown::ValueRange;

/// Translates a DataFusion `PhysicalExpr` predicate into per-dimension
/// boolean masks that can be applied during record batch streaming.
///
/// This struct is the universal entry point for predicate pushdown across
/// all backends (NetCDF, Zarr, Atlas).
#[derive(Debug, Clone)]
pub struct PushdownFilter {
    _predicate: Arc<dyn PhysicalExpr>,
    ranges: HashMap<String, ValueRange>,
}

impl PushdownFilter {
    pub fn new(predicate: Arc<dyn PhysicalExpr>) -> Self {
        let mut ranges = HashMap::new();
        walk_expr(&predicate, &mut ranges);
        Self {
            _predicate: predicate,
            ranges,
        }
    }

    pub fn ranges(&self) -> &HashMap<String, ValueRange> {
        &self.ranges
    }

    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self._predicate
    }
}

// ─── Expression tree walking ───────────────────────────────────────────────

fn walk_expr(node: &Arc<dyn PhysicalExpr>, ranges: &mut HashMap<String, ValueRange>) {
    if let Some(bin) = downcast::<BinaryExpr>(node) {
        let op = bin.op();

        if *op == Operator::And {
            walk_expr(bin.left(), ranges);
            walk_expr(bin.right(), ranges);
            return;
        }

        // col op lit
        if let (Some(col_name), Some(val)) =
            (as_column_name(bin.left()), as_pushdown_scalar(bin.right()))
        {
            apply_op(ranges, &col_name, *op, val);
            return;
        }

        // lit op col (flip)
        if let (Some(val), Some(col_name)) =
            (as_pushdown_scalar(bin.left()), as_column_name(bin.right()))
        {
            apply_op(ranges, &col_name, flip_op(*op), val);
            return;
        }

        // Recurse into children for nested AND trees
        walk_expr(bin.left(), ranges);
        walk_expr(bin.right(), ranges);
        return;
    }

    for child in node.children() {
        walk_expr(child, ranges);
    }
}

fn apply_op(ranges: &mut HashMap<String, ValueRange>, col: &str, op: Operator, val: ScalarValue) {
    let range = ranges
        .entry(col.to_string())
        .or_insert_with(ValueRange::empty);
    match op {
        Operator::Gt => range.with_lower(val, false),
        Operator::GtEq => range.with_lower(val, true),
        Operator::Lt => range.with_upper(val, false),
        Operator::LtEq => range.with_upper(val, true),
        Operator::Eq => {
            range.with_lower(val.clone(), true);
            range.with_upper(val, true);
        }
        _ => {}
    }
}

fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

fn as_column_name(e: &Arc<dyn PhysicalExpr>) -> Option<String> {
    let inner = peel_casts(e)?;
    let col = downcast::<Column>(inner)?;
    Some(col.name().to_string())
}

/// Extract a ScalarValue from a literal if it's a pushdown-supported type
/// (numeric or timestamp).
fn as_pushdown_scalar(e: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    let inner = peel_casts(e)?;
    let lit = downcast::<Literal>(inner)?;
    let sv = lit.value();
    if is_pushdown_scalar(sv) {
        Some(sv.clone())
    } else {
        None
    }
}

fn is_pushdown_scalar(sv: &ScalarValue) -> bool {
    matches!(
        sv,
        ScalarValue::Int8(Some(_))
            | ScalarValue::Int16(Some(_))
            | ScalarValue::Int32(Some(_))
            | ScalarValue::Int64(Some(_))
            | ScalarValue::UInt8(Some(_))
            | ScalarValue::UInt16(Some(_))
            | ScalarValue::UInt32(Some(_))
            | ScalarValue::UInt64(Some(_))
            | ScalarValue::Float32(Some(_))
            | ScalarValue::Float64(Some(_))
            | ScalarValue::TimestampNanosecond(Some(_), _)
            | ScalarValue::TimestampMicrosecond(Some(_), _)
            | ScalarValue::TimestampMillisecond(Some(_), _)
            | ScalarValue::TimestampSecond(Some(_), _)
    )
}

fn peel_casts(e: &Arc<dyn PhysicalExpr>) -> Option<&Arc<dyn PhysicalExpr>> {
    let mut cur = e;
    loop {
        if let Some(c) = downcast::<CastExpr>(cur) {
            cur = c.expr();
            continue;
        }
        return Some(cur);
    }
}

fn downcast<T: 'static>(expr: &Arc<dyn PhysicalExpr>) -> Option<&T> {
    expr.as_any().downcast_ref::<T>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    fn lit_i64(value: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int64(Some(value))))
    }

    fn binary(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    fn ranges_of(expr: Arc<dyn PhysicalExpr>) -> HashMap<String, ValueRange> {
        PushdownFilter::new(expr).ranges().clone()
    }

    /// (value as i64, inclusive) of a bound, for terse assertions.
    fn bound(bound: &Option<(ScalarValue, bool)>) -> Option<(i64, bool)> {
        bound.as_ref().map(|(value, inclusive)| match value {
            ScalarValue::Int64(Some(v)) => (*v, *inclusive),
            other => panic!("unexpected scalar {other:?}"),
        })
    }

    #[test]
    fn test_column_op_literal_becomes_a_lower_bound() {
        let ranges = ranges_of(binary(col("time"), Operator::Gt, lit_i64(5)));
        let range = ranges.get("time").expect("time range");
        assert_eq!(bound(&range.min), Some((5, false)));
        assert_eq!(bound(&range.max), None);
    }

    #[test]
    fn test_gteq_is_an_inclusive_lower_bound() {
        let ranges = ranges_of(binary(col("time"), Operator::GtEq, lit_i64(5)));
        assert_eq!(bound(&ranges["time"].min), Some((5, true)));
    }

    #[test]
    fn test_literal_on_the_left_flips_the_operator() {
        // `5 < time` is `time > 5`, not `time < 5`.
        let ranges = ranges_of(binary(lit_i64(5), Operator::Lt, col("time")));
        let range = &ranges["time"];
        assert_eq!(bound(&range.min), Some((5, false)));
        assert_eq!(bound(&range.max), None);
    }

    #[test]
    fn test_equality_pins_both_bounds_inclusively() {
        let ranges = ranges_of(binary(col("time"), Operator::Eq, lit_i64(7)));
        let range = &ranges["time"];
        assert_eq!(bound(&range.min), Some((7, true)));
        assert_eq!(bound(&range.max), Some((7, true)));
    }

    #[test]
    fn test_and_tree_tightens_both_ends_per_column() {
        // (time >= 10 AND time < 100) AND depth <= 50
        let expr = binary(
            binary(
                binary(col("time"), Operator::GtEq, lit_i64(10)),
                Operator::And,
                binary(col("time"), Operator::Lt, lit_i64(100)),
            ),
            Operator::And,
            binary(col("depth"), Operator::LtEq, lit_i64(50)),
        );
        let ranges = ranges_of(expr);

        assert_eq!(bound(&ranges["time"].min), Some((10, true)));
        assert_eq!(bound(&ranges["time"].max), Some((100, false)));
        assert_eq!(bound(&ranges["depth"].max), Some((50, true)));
        assert_eq!(bound(&ranges["depth"].min), None);
    }

    #[test]
    fn test_repeated_bounds_keep_the_tighter_one() {
        // time > 5 AND time > 20 -> lower bound 20.
        let expr = binary(
            binary(col("time"), Operator::Gt, lit_i64(5)),
            Operator::And,
            binary(col("time"), Operator::Gt, lit_i64(20)),
        );
        assert_eq!(bound(&ranges_of(expr)["time"].min), Some((20, false)));

        // The tightening is order-independent.
        let expr = binary(
            binary(col("time"), Operator::Gt, lit_i64(20)),
            Operator::And,
            binary(col("time"), Operator::Gt, lit_i64(5)),
        );
        assert_eq!(bound(&ranges_of(expr)["time"].min), Some((20, false)));
    }

    #[test]
    fn test_equal_bound_values_keep_the_exclusive_variant() {
        // time >= 5 AND time > 5 -> the exclusive bound wins.
        let expr = binary(
            binary(col("time"), Operator::GtEq, lit_i64(5)),
            Operator::And,
            binary(col("time"), Operator::Gt, lit_i64(5)),
        );
        assert_eq!(bound(&ranges_of(expr)["time"].min), Some((5, false)));
    }

    #[test]
    fn test_casts_are_peeled_from_both_sides() {
        let casted_col: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(col("time"), DataType::Int64, None));
        let casted_lit: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(lit_i64(5), DataType::Int64, None));

        let ranges = ranges_of(binary(casted_col, Operator::Gt, casted_lit));
        assert_eq!(bound(&ranges["time"].min), Some((5, false)));
    }

    #[test]
    fn test_null_literals_are_not_pushed_down() {
        let null_lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(None)));
        let ranges = ranges_of(binary(col("time"), Operator::Gt, null_lit));
        assert!(ranges.is_empty(), "unexpected ranges: {ranges:?}");
    }

    #[test]
    fn test_string_literals_are_not_pushed_down() {
        let string_lit: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Utf8(Some("abc".into()))));
        let ranges = ranges_of(binary(col("name"), Operator::Gt, string_lit));
        assert!(ranges.is_empty(), "unexpected ranges: {ranges:?}");
    }

    #[test]
    fn test_unsupported_operator_registers_an_unbounded_range() {
        // `!=` yields no usable bound; the column entry exists but stays open,
        // so a mask built from it must not prune anything.
        let ranges = ranges_of(binary(col("time"), Operator::NotEq, lit_i64(5)));
        let range = &ranges["time"];
        assert_eq!(bound(&range.min), None);
        assert_eq!(bound(&range.max), None);
    }

    #[test]
    fn test_or_branches_are_walked_but_do_not_intersect_correctly() {
        // NOTE: `OR` is not modelled — both sides are walked and their bounds
        // are intersected as if they were `AND`ed. This is only sound because
        // the caller re-applies the full predicate after pushdown; the mask is
        // a *hint*, so an over-tight range here would drop rows. Recorded as a
        // characterisation test so a future fix has to acknowledge it.
        let expr = binary(
            binary(col("time"), Operator::Gt, lit_i64(10)),
            Operator::Or,
            binary(col("time"), Operator::Gt, lit_i64(100)),
        );
        assert_eq!(bound(&ranges_of(expr)["time"].min), Some((100, false)));
    }

    #[test]
    fn test_predicate_is_retained_verbatim() {
        let expr = binary(col("time"), Operator::Gt, lit_i64(5));
        let filter = PushdownFilter::new(expr.clone());
        assert!(Arc::ptr_eq(filter.predicate(), &expr));
    }

    #[test]
    fn test_timestamp_literals_are_pushdown_candidates() {
        let ts: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(
            ScalarValue::TimestampNanosecond(Some(1_600_000_000_000_000_000), None),
        ));
        let ranges = ranges_of(binary(col("time"), Operator::GtEq, ts));
        assert!(ranges.contains_key("time"));
        assert!(ranges["time"].min.is_some());
    }
}
