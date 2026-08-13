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

/// Collect the bounds the predicate *implies*.
///
/// Every range this produces has to be satisfied by every row that satisfies the
/// predicate. A reader is then free to skip a chunk no row of which can meet the
/// ranges, because no row of it could have met the predicate either.
///
/// That is why only two shapes contribute: a comparison against a literal, and
/// an `AND` of things that contribute. A row can satisfy `a OR b` while failing
/// `a`, and `NOT (time > 5)` means the opposite of what its child says, so
/// nothing is taken from either. Ranges are only ever dropped by that rule, and
/// dropping a range prunes less, never more.
fn walk_expr(node: &Arc<dyn PhysicalExpr>, ranges: &mut HashMap<String, ValueRange>) {
    let Some(bin) = downcast::<BinaryExpr>(node) else {
        // A `NOT`, a `CASE`, an `IN` list: each gives its children a meaning this
        // walk cannot read off them, so none of it is used.
        return;
    };
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
    }

    // Anything else — an `OR`, an arithmetic subtree, a comparison between two
    // columns — implies no bound on its own, and its children imply none either.
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

    /// `OR` yields no bound at all.
    ///
    /// Both sides used to be walked and intersected as if they were `AND`ed, so
    /// `time > 10 OR time > 100` produced `time > 100`. A reader that skipped a
    /// chunk on that range would drop the rows between 10 and 100, which satisfy
    /// the predicate. Re-applying the predicate above the scan cannot bring back
    /// a chunk the scan never read, so the range has to be implied by the
    /// predicate, not merely suggested by part of it.
    #[test]
    fn test_or_yields_no_bound() {
        let expr = binary(
            binary(col("time"), Operator::Gt, lit_i64(10)),
            Operator::Or,
            binary(col("time"), Operator::Gt, lit_i64(100)),
        );
        assert!(
            ranges_of(expr).is_empty(),
            "a disjunction implies no bound on either side"
        );
    }

    /// The same, for a negation.
    ///
    /// `NOT (time > 5)` is `time <= 5`. The walk used to descend through the
    /// `NOT` into its child and read off `time > 5` — the exact complement of
    /// the rows the query wants.
    #[test]
    fn test_negation_yields_no_bound() {
        use datafusion::physical_expr::expressions::NotExpr;

        let inner = binary(col("time"), Operator::Gt, lit_i64(5));
        let negated: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(inner));
        assert!(
            ranges_of(negated).is_empty(),
            "a negation implies no bound on its child"
        );
    }

    /// An `AND` that holds an `OR` still contributes its own comparisons.
    ///
    /// Dropping the disjunction must not cost the bounds beside it: those are
    /// implied by the predicate whatever the `OR` does.
    #[test]
    fn test_a_conjunction_keeps_the_bounds_beside_a_disjunction() {
        // time >= 10 AND (depth > 1 OR depth > 900)
        let expr = binary(
            binary(col("time"), Operator::GtEq, lit_i64(10)),
            Operator::And,
            binary(
                binary(col("depth"), Operator::Gt, lit_i64(1)),
                Operator::Or,
                binary(col("depth"), Operator::Gt, lit_i64(900)),
            ),
        );
        let ranges = ranges_of(expr);
        assert_eq!(bound(&ranges["time"].min), Some((10, true)));
        assert!(
            !ranges.contains_key("depth"),
            "the disjunction gives nothing"
        );
    }

    #[test]
    fn test_predicate_is_retained_verbatim() {
        let expr = binary(col("time"), Operator::Gt, lit_i64(5));
        let filter = PushdownFilter::new(expr.clone());
        assert!(Arc::ptr_eq(filter.predicate(), &expr));
    }

    #[test]
    fn test_timestamp_literals_are_pushdown_candidates() {
        let ts: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
            Some(1_600_000_000_000_000_000),
            None,
        )));
        let ranges = ranges_of(binary(col("time"), Operator::GtEq, ts));
        assert!(ranges.contains_key("time"));
        assert!(ranges["time"].min.is_some());
    }
}
