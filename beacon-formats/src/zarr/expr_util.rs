use arrow::array::{ArrayRef, Scalar};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

/// A single interval: (value, inclusive?)
#[derive(Debug, Clone, Default)]
pub struct ZarrFilterRange {
    pub min: Option<(ScalarValue, bool)>,
    pub max: Option<(ScalarValue, bool)>,
}

impl ZarrFilterRange {
    pub fn new(min: Option<(ScalarValue, bool)>, max: Option<(ScalarValue, bool)>) -> Self {
        Self { min, max }
    }

    pub fn as_f64_max(&self) -> Option<f64> {
        self.max.as_ref().and_then(|(v, _)| {
            if let ScalarValue::Float64(Some(f)) = v {
                Some(*f)
            } else {
                None
            }
        })
    }

    pub fn as_f64_min(&self) -> Option<f64> {
        self.min.as_ref().and_then(|(v, _)| {
            if let ScalarValue::Float64(Some(f)) = v {
                Some(*f)
            } else {
                None
            }
        })
    }

    pub fn as_timestamp_max(&self) -> Option<chrono::NaiveDateTime> {
        self.max.as_ref().and_then(|(v, _)| match v {
            ScalarValue::TimestampNanosecond(Some(ts), None) => {
                chrono::NaiveDateTime::from_timestamp_nanos(*ts)
            }
            ScalarValue::TimestampMicrosecond(Some(ts), None) => {
                chrono::NaiveDateTime::from_timestamp_micros(*ts)
            }
            ScalarValue::TimestampMillisecond(Some(ts), None) => {
                chrono::NaiveDateTime::from_timestamp_millis(*ts)
            }
            ScalarValue::TimestampSecond(Some(ts), None) => {
                Some(chrono::NaiveDateTime::from_timestamp(*ts, 0))
            }
            _ => None,
        })
    }

    pub fn as_timestamp_min(&self) -> Option<chrono::NaiveDateTime> {
        self.min.as_ref().and_then(|(v, _)| match v {
            ScalarValue::TimestampNanosecond(Some(ts), None) => {
                chrono::NaiveDateTime::from_timestamp_nanos(*ts)
            }
            ScalarValue::TimestampMicrosecond(Some(ts), None) => {
                chrono::NaiveDateTime::from_timestamp_micros(*ts)
            }
            ScalarValue::TimestampMillisecond(Some(ts), None) => {
                chrono::NaiveDateTime::from_timestamp_millis(*ts)
            }
            ScalarValue::TimestampSecond(Some(ts), None) => {
                Some(chrono::NaiveDateTime::from_timestamp(*ts, 0))
            }
            _ => None,
        })
    }

    pub fn max_value_as_arrow_scalar(&self) -> Option<Scalar<ArrayRef>> {
        self.max.as_ref().and_then(|(v, _)| v.to_scalar().ok())
    }

    pub fn min_value_as_arrow_scalar(&self) -> Option<Scalar<ArrayRef>> {
        self.min.as_ref().and_then(|(v, _)| v.to_scalar().ok())
    }

    pub fn min_value(&self) -> Option<&ScalarValue> {
        self.min.as_ref().map(|(v, _)| v)
    }

    pub fn max_value(&self) -> Option<&ScalarValue> {
        self.max.as_ref().map(|(v, _)| v)
    }

    pub fn is_max_inclusive(&self) -> Option<bool> {
        self.max.as_ref().map(|(_, inc)| *inc)
    }
    pub fn is_min_inclusive(&self) -> Option<bool> {
        self.min.as_ref().map(|(_, inc)| *inc)
    }

    fn tighten_lower(&mut self, val: ScalarValue, inclusive: bool) {
        match &self.min {
            None => self.min = Some((val, inclusive)),
            Some((cur, cur_inc)) => {
                if let Some(ord) = cur.partial_cmp(&val) {
                    use std::cmp::Ordering::*;
                    match ord {
                        Less => self.min = Some((val, inclusive)),
                        Equal => self.min = Some((val, *cur_inc && inclusive)),
                        Greater => {} // keep tighter existing lower
                    }
                }
            }
        }
    }
    fn tighten_upper(&mut self, val: ScalarValue, inclusive: bool) {
        match &self.max {
            None => self.max = Some((val, inclusive)),
            Some((cur, cur_inc)) => {
                if let Some(ord) = cur.partial_cmp(&val) {
                    use std::cmp::Ordering::*;
                    match ord {
                        Greater => self.max = Some((val, inclusive)),
                        Equal => self.max = Some((val, *cur_inc && inclusive)),
                        Less => {} // keep tighter existing upper
                    }
                }
            }
        }
    }
}

/// Entry point: extract tightest range for `target_col` from a set of
/// physical filter expressions (possibly complex AND trees).
/// Returns None if no constraints found.
pub fn extract_range_from_physical_filters(
    filters: &[Arc<dyn PhysicalExpr>],
    target_col: &str,
) -> Option<ZarrFilterRange> {
    let mut out = ZarrFilterRange::default();
    for f in filters {
        walk_physical_expr(f, target_col, &mut out);
    }

    tracing::debug!(
        "Extracted ZarrFilterRange for column '{}': {:?}",
        target_col,
        out
    );

    if out.min.is_some() || out.max.is_some() {
        Some(out)
    } else {
        None
    }
}

fn walk_physical_expr(node: &Arc<dyn PhysicalExpr>, target_col: &str, out: &mut ZarrFilterRange) {
    if let Some(bin) = downcast::<BinaryExpr>(node) {
        let op = bin.op();

        // AND = BinaryExpr with Operator::And: visit both sides
        if *op == Operator::And {
            walk_physical_expr(bin.left(), target_col, out);
            walk_physical_expr(bin.right(), target_col, out);
            return;
        }

        // Comparison: try (col vs lit), then (lit vs col) with flipped op
        if let (Some(_c), Some(lv)) = (
            as_target_col(bin.left(), target_col),
            as_literal(bin.right()),
        ) {
            apply_cmp(out, *op, lv);
            return;
        }
        if let (Some(lv), Some(_c)) = (
            as_literal(bin.left()),
            as_target_col(bin.right(), target_col),
        ) {
            apply_cmp(out, flip_op(*op), lv);
            return;
        }

        // Not a direct pattern: still walk children (there might be nested pieces)
        walk_physical_expr(bin.left(), target_col, out);
        walk_physical_expr(bin.right(), target_col, out);
        return;
    }

    // Generic: peel through any children (e.g., some wrappers/functions you may want to inspect)
    for child in node.children() {
        walk_physical_expr(child, target_col, out);
    }
}

fn apply_cmp(out: &mut ZarrFilterRange, op: Operator, lit: &ScalarValue) {
    match op {
        Operator::Gt => out.tighten_lower(lit.clone(), false),
        Operator::GtEq => out.tighten_lower(lit.clone(), true),
        Operator::Lt => out.tighten_upper(lit.clone(), false),
        Operator::LtEq => out.tighten_upper(lit.clone(), true),
        Operator::Eq => {
            out.tighten_lower(lit.clone(), true);
            out.tighten_upper(lit.clone(), true);
        }
        _ => {} // ignore OR, NotEq, etc. (not a single-interval constraint)
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

/// ---- helpers ----

fn as_literal(e: &Arc<dyn PhysicalExpr>) -> Option<&ScalarValue> {
    match peel_casts(e) {
        Some(l) if downcast::<Literal>(l).is_some() => {
            downcast::<Literal>(l).map(|lit| lit.value())
        }
        _ => None,
    }
}

fn as_target_col<'l>(e: &'l Arc<dyn PhysicalExpr>, target: &str) -> Option<&'l Column> {
    match peel_casts(e) {
        Some(c) if downcast::<Column>(c).is_some() => {
            let c = downcast::<Column>(c).unwrap();
            // Column::name() is the leaf name at physical layer; qualify if you pass "table.col"
            if c.name() == target || format!("{}", c).ends_with(&format!(".{}", target)) {
                Some(c)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Remove any number of CAST(...) wrappers around an expression.
fn peel_casts(mut e: &Arc<dyn PhysicalExpr>) -> Option<&Arc<dyn PhysicalExpr>> {
    let mut cur = e;
    loop {
        if let Some(c) = downcast::<CastExpr>(cur) {
            cur = c.expr();
            continue;
        }
        return Some(cur);
    }
}

/// Convenience downcast
fn downcast<T: 'static>(expr: &Arc<dyn PhysicalExpr>) -> Option<&T> {
    expr.as_any().downcast_ref::<T>()
}
