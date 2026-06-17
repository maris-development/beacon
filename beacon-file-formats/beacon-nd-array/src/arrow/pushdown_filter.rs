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
