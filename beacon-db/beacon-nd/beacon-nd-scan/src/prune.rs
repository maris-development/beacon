//! The chunk prune: the chunks of a file that no row of the predicate can come from.
//!
//! The prune only skips work. The filter above the scan applies the predicate
//! again, so a chunk that stays is always correct. When a mask is in doubt,
//! the prune builds no mask.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, Scalar};
use arrow::compute::kernels::cmp::{gt, gt_eq, lt, lt_eq};
use arrow::compute::{and, cast, prep_null_mask_filter};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;
use beacon_nd_dataset::{ChunkGrid, NdDataset, NdVariable, Region};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::scalar::ScalarValue;

use crate::cast_rule::{keeps_values, unit_nanos};

/// The bounds that a predicate implies for one column.
#[derive(Debug, Clone)]
pub(crate) struct ValueRange {
    /// The lower bound and whether it is inclusive.
    pub min: Option<(ScalarValue, bool)>,
    /// The upper bound and whether it is inclusive.
    pub max: Option<(ScalarValue, bool)>,
    /// The target types of the casts around the column, from the column out.
    pub column_casts: Vec<DataType>,
    /// True when two compares of the column have different casts around it.
    pub mixed_casts: bool,
}

impl ValueRange {
    /// A range with no bound, for a column with `column_casts` around it.
    fn empty(column_casts: Vec<DataType>) -> Self {
        Self {
            min: None,
            max: None,
            column_casts,
            mixed_casts: false,
        }
    }

    /// Tighten this range with a lower bound.
    fn with_lower(&mut self, value: ScalarValue, inclusive: bool) {
        match &self.min {
            None => self.min = Some((value, inclusive)),
            Some((existing, existing_inclusive)) => match existing.partial_cmp(&value) {
                Some(Ordering::Less) => self.min = Some((value, inclusive)),
                Some(Ordering::Equal) => self.min = Some((value, *existing_inclusive && inclusive)),
                // The existing bound is tighter, or the two do not compare.
                Some(Ordering::Greater) | None => {}
            },
        }
    }

    /// Tighten this range with an upper bound.
    fn with_upper(&mut self, value: ScalarValue, inclusive: bool) {
        match &self.max {
            None => self.max = Some((value, inclusive)),
            Some((existing, existing_inclusive)) => match existing.partial_cmp(&value) {
                Some(Ordering::Greater) => self.max = Some((value, inclusive)),
                Some(Ordering::Equal) => self.max = Some((value, *existing_inclusive && inclusive)),
                // The existing bound is tighter, or the two do not compare.
                Some(Ordering::Less) | None => {}
            },
        }
    }

    /// True when the range has no bound.
    fn is_unbounded(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }
}

/// The bounds from the `column op literal` conjuncts of `predicate`.
///
/// Each range holds for every row that meets the predicate. Only a comparison
/// with a literal and an `AND` give a bound. An `OR`, a `NOT`, an `IN` and a
/// `CASE` give nothing, so a missing rule prunes less and never more.
/// A range records the casts around its column. [`coordinate_masks`] decides
/// if they keep values. A cast around the literal is evaluated.
pub(crate) fn value_ranges(predicate: &Arc<dyn PhysicalExpr>) -> HashMap<String, ValueRange> {
    let mut ranges = HashMap::new();
    walk_expr(predicate, &mut ranges);
    ranges
}

/// Add the bounds that `node` implies to `ranges`.
fn walk_expr(node: &Arc<dyn PhysicalExpr>, ranges: &mut HashMap<String, ValueRange>) {
    // A `NOT`, a `CASE` or an `IN` list changes the meaning of its children.
    let Some(binary) = node.downcast_ref::<BinaryExpr>() else {
        return;
    };
    let op = *binary.op();

    if op == Operator::And {
        walk_expr(binary.left(), ranges);
        walk_expr(binary.right(), ranges);
        return;
    }

    if let (Some((column, casts)), Some(value)) =
        (as_column(binary.left()), as_pushdown_scalar(binary.right()))
    {
        apply_op(ranges, &column, casts, op, value);
        return;
    }

    // `literal op column` is `column flip(op) literal`.
    if let (Some(value), Some((column, casts))) =
        (as_pushdown_scalar(binary.left()), as_column(binary.right()))
    {
        apply_op(ranges, &column, casts, flip_op(op), value);
    }

    // An `OR`, an arithmetic tree or a compare of two columns implies no bound.
}

/// Tighten the range of `column` with `CAST(column AS casts..) op value`.
fn apply_op(
    ranges: &mut HashMap<String, ValueRange>,
    column: &str,
    casts: Vec<DataType>,
    op: Operator,
    value: ScalarValue,
) {
    let range = ranges
        .entry(column.to_string())
        .or_insert_with(|| ValueRange::empty(casts.clone()));
    range.mixed_casts |= range.column_casts != casts;
    match op {
        Operator::Gt => range.with_lower(value, false),
        Operator::GtEq => range.with_lower(value, true),
        Operator::Lt => range.with_upper(value, false),
        Operator::LtEq => range.with_upper(value, true),
        Operator::Eq => {
            range.with_lower(value.clone(), true);
            range.with_upper(value, true);
        }
        _ => {}
    }
}

/// The operator with its two sides swapped.
fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// The column name of `expr`, and the target types of the casts around it from the column out.
fn as_column(expr: &Arc<dyn PhysicalExpr>) -> Option<(String, Vec<DataType>)> {
    let mut casts = Vec::new();
    let mut current = expr;
    while let Some(cast) = current.downcast_ref::<CastExpr>() {
        casts.push(cast.cast_type().clone());
        current = cast.expr();
    }
    let column = current.downcast_ref::<Column>()?;
    casts.reverse();
    Some((column.name().to_string(), casts))
}

/// The value of a numeric or timestamp literal, with its casts evaluated.
fn as_pushdown_scalar(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    let value = literal_value(expr)?;
    is_pushdown_scalar(&value).then_some(value)
}

/// The value of a literal with the casts around it. `None` when a cast fails.
fn literal_value(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        let value = literal_value(cast.expr())?;
        return value
            .cast_to_with_options(cast.cast_type(), cast.cast_options())
            .ok();
    }
    Some(expr.downcast_ref::<Literal>()?.value().clone())
}

/// True for a numeric or timestamp value that is not null.
fn is_pushdown_scalar(value: &ScalarValue) -> bool {
    matches!(
        value,
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

/// Why the casts from the file values to the compare of `range` can change which rows match.
///
/// The chain runs from the variable type to the table type, then through the
/// casts around the column. `None` when each step keeps values.
fn cast_problem(
    variable_type: &DataType,
    table_type: Option<&DataType>,
    range: &ValueRange,
) -> Option<String> {
    let Some(table_type) = table_type else {
        return Some("the scan schema does not hold it".to_string());
    };
    if range.mixed_casts {
        return Some("its compares have different casts around it".to_string());
    }
    let mut from = variable_type;
    for to in std::iter::once(table_type).chain(&range.column_casts) {
        if !keeps_values(from, to) {
            return Some(format!("a cast from {from} to {to} can change values"));
        }
        from = to;
    }
    None
}

/// The keep mask of each bounded 1-D coordinate, with the name of its dimension.
pub(crate) type Masks = Vec<(String, BooleanArray)>;

/// Read the 1-D coordinates that `ranges` bounds, and build their keep masks.
///
/// A variable gets a mask when it has rank 1, at least one element, a numeric
/// or timestamp type, and a range with a bound. Each cast from the variable
/// type to its type in `table_types`, and then through the casts of the range,
/// must keep values. A mask that cannot be built is left out, so the scan
/// reads each chunk of its dimension.
pub(crate) async fn coordinate_masks(
    dataset: &dyn NdDataset,
    ranges: &HashMap<String, ValueRange>,
    table_types: &HashMap<String, DataType>,
) -> Masks {
    let mut masks = Masks::new();
    for name in dataset.variable_names() {
        let Some(range) = ranges.get(&*name) else {
            continue;
        };
        let Some(variable) = dataset.variable(&name) else {
            continue;
        };
        let [dimension] = variable.dimensions().as_slice() else {
            continue;
        };
        // An empty coordinate or an open range prunes nothing, so it is not read.
        if dimension.size() == 0 || range.is_unbounded() {
            continue;
        }
        if let Some(problem) = cast_problem(variable.data_type(), table_types.get(&*name), range) {
            tracing::debug!("no prune mask for '{name}': {problem}");
            continue;
        }
        // The compare sees the type of the outer column cast, else the table type.
        let compare_type = range.column_casts.last().or(table_types.get(&*name));
        let raw_int64 = compare_type == Some(variable.data_type());
        let Some(compare) = Compare::try_new(variable.data_type(), range, raw_int64) else {
            tracing::debug!(
                "no prune mask for '{name}': its type {} does not fit the bounds {range:?}",
                variable.data_type()
            );
            continue;
        };
        match read_mask(variable.as_ref(), &compare).await {
            Ok(mask) if mask.len() == dimension.size() => {
                masks.push((dimension.name().to_string(), mask));
            }
            Ok(mask) => tracing::debug!(
                "no prune mask for '{name}': it has {} values for a dimension of size {}",
                mask.len(),
                dimension.size()
            ),
            Err(error) => tracing::debug!("no prune mask for '{name}': {error}"),
        }
    }
    masks
}

/// True when no row of `chunk` can meet the predicate.
///
/// The chunk is pruned when the mask of one of its axes has no `true` in the
/// range of the chunk on that axis. The first mask of a dimension counts.
pub(crate) fn is_pruned(masks: &Masks, grid: &ChunkGrid, chunk: &Region) -> bool {
    grid.dimensions()
        .iter()
        .enumerate()
        .any(|(axis, dimension)| {
            let Some((_, mask)) = masks.iter().find(|(name, _)| name == dimension.name()) else {
                return false;
            };
            let (Some(&start), Some(&length)) = (chunk.start.get(axis), chunk.shape.get(axis))
            else {
                return false;
            };
            // A chunk outside the mask is not pruned.
            match start.checked_add(length) {
                Some(end) if end <= mask.len() => {
                    mask.values().slice(start, length).count_set_bits() == 0
                }
                _ => false,
            }
        })
}

/// One bound in the compare type of a coordinate.
#[derive(Debug)]
struct Bound {
    /// The bound value, as a one-element array of the compare type.
    value: Scalar<ArrayRef>,
    /// True when the bound value itself is kept.
    inclusive: bool,
}

/// How to compare the values of one coordinate with its bounds.
#[derive(Debug)]
struct Compare {
    /// The type that the coordinate is cast to before the compare.
    target: DataType,
    /// The lower bound.
    min: Option<Bound>,
    /// The upper bound.
    max: Option<Bound>,
}

impl Compare {
    /// The compare of a coordinate of `data_type` with `range`.
    ///
    /// `raw_int64` is true when the compare sees `data_type` itself.
    /// Only then is an Int64 bound a raw value in the unit of a timestamp coordinate.
    /// `None` when a bound does not fit the type, or when no bound is left.
    fn try_new(data_type: &DataType, range: &ValueRange, raw_int64: bool) -> Option<Self> {
        let compare = match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => {
                // Float64 holds each value of the other types, but not each Int64 or UInt64.
                let exact = !matches!(data_type, DataType::Int64 | DataType::UInt64);
                Self {
                    target: DataType::Float64,
                    min: float_bound(range.min.as_ref(), exact)?,
                    max: float_bound(range.max.as_ref(), exact)?,
                }
            }
            DataType::Timestamp(unit, _) => Self {
                target: DataType::Int64,
                min: time_bound(range.min.as_ref(), *unit, true, raw_int64)?,
                max: time_bound(range.max.as_ref(), *unit, false, raw_int64)?,
            },
            _ => return None,
        };
        (compare.min.is_some() || compare.max.is_some()).then_some(compare)
    }

    /// The keep mask of `values`. A null value is not kept.
    fn mask(&self, values: &ArrayRef) -> Result<BooleanArray, ArrowError> {
        let values = cast(values, &self.target)?;
        let lower = self
            .min
            .as_ref()
            .map(|bound| match bound.inclusive {
                true => gt_eq(&values, &bound.value),
                false => gt(&values, &bound.value),
            })
            .transpose()?;
        let upper = self
            .max
            .as_ref()
            .map(|bound| match bound.inclusive {
                true => lt_eq(&values, &bound.value),
                false => lt(&values, &bound.value),
            })
            .transpose()?;
        let mask = match (lower, upper) {
            (Some(lower), Some(upper)) => and(&lower, &upper)?,
            (Some(mask), None) | (None, Some(mask)) => mask,
            (None, None) => BooleanArray::from(vec![true; values.len()]),
        };
        // `prep_null_mask_filter` turns each null into false, but needs a null buffer.
        Ok(match mask.nulls() {
            Some(_) => prep_null_mask_filter(&mask),
            None => mask,
        })
    }
}

/// Read the whole of `variable` and build its keep mask.
async fn read_mask(
    variable: &dyn NdVariable,
    compare: &Compare,
) -> Result<BooleanArray, Box<dyn std::error::Error + Send + Sync>> {
    let array = variable.read(&Region::full(variable.dimensions())).await?;
    Ok(compare.mask(array.values())?)
}

/// A bound for a Float64 compare. `Some(None)` for no bound, `None` when it does not fit.
///
/// A strict bound becomes inclusive when Float64 does not hold the two sides
/// exactly, because the round of two different values can give one value.
fn float_bound(bound: Option<&(ScalarValue, bool)>, exact_values: bool) -> Option<Option<Bound>> {
    let Some((value, inclusive)) = bound else {
        return Some(None);
    };
    let (value, exact_literal) = scalar_to_f64(value)?;
    let value: ArrayRef = Arc::new(Float64Array::from(vec![value]));
    Some(Some(Bound {
        value: Scalar::new(value),
        inclusive: *inclusive || !(exact_values && exact_literal),
    }))
}

/// A numeric value in Float64, and true when Float64 holds it exactly.
fn scalar_to_f64(value: &ScalarValue) -> Option<(f64, bool)> {
    Some(match value {
        ScalarValue::Float64(Some(v)) => (*v, true),
        ScalarValue::Float32(Some(v)) => (*v as f64, true),
        ScalarValue::Int8(Some(v)) => (*v as f64, true),
        ScalarValue::Int16(Some(v)) => (*v as f64, true),
        ScalarValue::Int32(Some(v)) => (*v as f64, true),
        ScalarValue::Int64(Some(v)) => (*v as f64, (*v as f64) as i128 == *v as i128),
        ScalarValue::UInt8(Some(v)) => (*v as f64, true),
        ScalarValue::UInt16(Some(v)) => (*v as f64, true),
        ScalarValue::UInt32(Some(v)) => (*v as f64, true),
        ScalarValue::UInt64(Some(v)) => (*v as f64, (*v as f64) as u128 == *v as u128),
        _ => return None,
    })
}

/// A bound for a timestamp coordinate in `unit`. `Some(None)` for no bound, `None` when it does not fit.
fn time_bound(
    bound: Option<&(ScalarValue, bool)>,
    unit: TimeUnit,
    lower: bool,
    raw_int64: bool,
) -> Option<Option<Bound>> {
    let Some((value, inclusive)) = bound else {
        return Some(None);
    };
    let value = scalar_to_unit(value, unit, lower, *inclusive, raw_int64)?;
    let value: ArrayRef = Arc::new(Int64Array::from(vec![value]));
    Some(Some(Bound {
        value: Scalar::new(value),
        inclusive: *inclusive,
    }))
}

/// A timestamp literal as a value in `unit`, with the same rows on its side of the bound.
///
/// - An Int64 literal is a raw value in `unit`, only when `raw_int64` is true. Else it gives `None`.
/// - A literal in the same unit or a coarser unit is multiplied to `unit`.
/// - A literal in a finer unit rounds, so that the bound keeps each whole value that meets it.
/// - `v > L` is `v > floor(L)`.
/// - `v >= L` is `v >= ceil(L)`.
/// - `v < L` is `v < ceil(L)`.
/// - `v <= L` is `v <= floor(L)`.
fn scalar_to_unit(
    value: &ScalarValue,
    unit: TimeUnit,
    lower: bool,
    inclusive: bool,
    raw_int64: bool,
) -> Option<i64> {
    let (value, literal_nanos) = match value {
        ScalarValue::TimestampNanosecond(Some(v), _) => (*v, 1),
        ScalarValue::TimestampMicrosecond(Some(v), _) => (*v, 1_000),
        ScalarValue::TimestampMillisecond(Some(v), _) => (*v, 1_000_000),
        ScalarValue::TimestampSecond(Some(v), _) => (*v, 1_000_000_000),
        ScalarValue::Int64(Some(v)) if raw_int64 => return Some(*v),
        _ => return None,
    };
    let unit_nanos = unit_nanos(unit);
    if literal_nanos >= unit_nanos {
        return value.checked_mul(literal_nanos / unit_nanos);
    }
    let factor = unit_nanos / literal_nanos;
    let floor = value.div_euclid(factor);
    let ceil = floor + i64::from(value.rem_euclid(factor) != 0);
    Some(if lower == inclusive { ceil } else { floor })
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Float32Array, Int32Array, StringArray, TimestampNanosecondArray, TimestampSecondArray,
        UInt64Array,
    };
    use arrow::datatypes::{Field, Schema};
    use beacon_nd_dataset::{Dimension, Dimensions, MemoryDataset, MemoryVariable, NdArrowArray};
    use datafusion::physical_expr::expressions::{NotExpr, binary, cast as cast_expr, col, lit};

    use super::*;
    use crate::testing::grid_dataset;

    /// `column > threshold`, over a schema that holds `column` with `data_type`.
    fn above(column: &str, data_type: DataType, threshold: ScalarValue) -> Arc<dyn PhysicalExpr> {
        let schema = Schema::new(vec![Field::new(column, data_type, true)]);
        binary(
            col(column, &schema).unwrap(),
            Operator::Gt,
            lit(threshold),
            &schema,
        )
        .unwrap()
    }

    /// `left op right` for two predicates over `lat` and `lon` in Float64.
    fn join(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        let schema = Schema::new(vec![
            Field::new("lat", DataType::Float64, true),
            Field::new("lon", DataType::Float64, true),
        ]);
        binary(left, op, right, &schema).unwrap()
    }

    /// `lat op value` over a Float64 `lat`.
    fn lat(op: Operator, value: f64) -> Arc<dyn PhysicalExpr> {
        let schema = Schema::new(vec![Field::new("lat", DataType::Float64, true)]);
        binary(col("lat", &schema).unwrap(), op, lit(value), &schema).unwrap()
    }

    /// A dataset with one 1-D variable `name` on the dimension `name`.
    fn coordinate(name: &str, values: ArrayRef) -> MemoryDataset {
        let dimensions = Dimensions::try_new([Dimension::new(name, values.len())]).unwrap();
        let variable =
            MemoryVariable::new(name, NdArrowArray::try_new(values, dimensions).unwrap());
        MemoryDataset::try_new("test.nd", vec![variable]).unwrap()
    }

    /// The masks of `predicate` on `dataset`, with the file type of each variable as its table type.
    async fn masks_of(dataset: &MemoryDataset, predicate: &Arc<dyn PhysicalExpr>) -> Masks {
        let table_types = dataset
            .variable_names()
            .iter()
            .filter_map(|name| {
                let variable = dataset.variable(name)?;
                Some((name.to_string(), variable.data_type().clone()))
            })
            .collect();
        coordinate_masks(dataset, &value_ranges(predicate), &table_types).await
    }

    /// The masks of `predicate` on `dataset`, with the table type `table_type` for `column`.
    async fn masks_in(
        dataset: &MemoryDataset,
        predicate: &Arc<dyn PhysicalExpr>,
        column: &str,
        table_type: DataType,
    ) -> Masks {
        let table_types = HashMap::from([(column.to_string(), table_type)]);
        coordinate_masks(dataset, &value_ranges(predicate), &table_types).await
    }

    /// `CAST(column AS cast_type) op value`, over a schema that holds `column` with `data_type`.
    fn cast_compare(
        column: &str,
        data_type: DataType,
        cast_type: DataType,
        op: Operator,
        value: ScalarValue,
    ) -> Arc<dyn PhysicalExpr> {
        let schema = Schema::new(vec![Field::new(column, data_type, true)]);
        let casted = cast_expr(col(column, &schema).unwrap(), &schema, cast_type).unwrap();
        binary(casted, op, lit(value), &schema).unwrap()
    }

    /// The one mask of `masks`, as booleans.
    fn only_mask(masks: &Masks, dimension: &str) -> Vec<bool> {
        assert_eq!(masks.len(), 1, "{masks:?}");
        assert_eq!(masks[0].0, dimension);
        assert_eq!(masks[0].1.null_count(), 0);
        masks[0].1.iter().map(Option::unwrap).collect()
    }

    /// The starts of the chunks of `grid_dataset` at batch size 12 that `masks` prunes.
    fn pruned_starts(masks: &Masks) -> Vec<Vec<usize>> {
        let grid = ChunkGrid::try_new(&grid_dataset("a.nd"), 12).unwrap();
        grid.chunks()
            .iter()
            .filter(|chunk| is_pruned(masks, &grid, chunk))
            .map(|chunk| chunk.start.clone())
            .collect()
    }

    const F: bool = false;
    const T: bool = true;

    // `lat` of the grid fixture runs from 0.0 to 7.0, in 2 chunks of 4.

    #[tokio::test]
    async fn a_filter_no_row_meets_prunes_every_chunk() {
        let dataset = grid_dataset("a.nd");
        let predicate = above("lat", DataType::Float64, ScalarValue::from(100.0));
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(pruned_starts(&masks).len(), 4);
    }

    #[tokio::test]
    async fn a_filter_every_row_meets_prunes_nothing() {
        let dataset = grid_dataset("a.nd");
        let predicate = above("lat", DataType::Float64, ScalarValue::from(-1.0));
        let masks = masks_of(&dataset, &predicate).await;
        assert!(pruned_starts(&masks).is_empty());
    }

    #[tokio::test]
    async fn a_filter_on_the_upper_half_keeps_half_the_chunks() {
        let dataset = grid_dataset("a.nd");
        let predicate = above("lat", DataType::Float64, ScalarValue::from(3.5));
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(pruned_starts(&masks), vec![vec![0, 0, 0], vec![0, 0, 3]]);
    }

    #[tokio::test]
    async fn a_filter_on_a_column_the_scan_does_not_hold_prunes_nothing() {
        let dataset = grid_dataset("a.nd");
        let predicate = above("depth", DataType::Float64, ScalarValue::from(5.0));
        let masks = masks_of(&dataset, &predicate).await;
        assert!(masks.is_empty());
        assert!(pruned_starts(&masks).is_empty());
        assert_eq!(
            dataset.reads("temp") + dataset.reads("lat") + dataset.reads("lon"),
            0
        );
    }

    #[tokio::test]
    async fn a_bound_is_inclusive_or_exclusive_as_the_operator_says() {
        let dataset = grid_dataset("a.nd");
        let masks = masks_of(&dataset, &lat(Operator::GtEq, 4.0)).await;
        assert_eq!(only_mask(&masks, "lat"), [F, F, F, F, T, T, T, T]);
        let masks = masks_of(&dataset, &lat(Operator::Gt, 4.0)).await;
        assert_eq!(only_mask(&masks, "lat"), [F, F, F, F, F, T, T, T]);
    }

    #[tokio::test]
    async fn two_bounds_on_one_column_meet() {
        let dataset = grid_dataset("a.nd");
        let predicate = join(
            lat(Operator::Gt, 1.0),
            Operator::And,
            lat(Operator::LtEq, 5.0),
        );
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(only_mask(&masks, "lat"), [F, F, T, T, T, T, F, F]);
    }

    #[test]
    fn an_or_gives_no_bound() {
        let predicate = join(
            above("lat", DataType::Float64, ScalarValue::from(100.0)),
            Operator::Or,
            above("lon", DataType::Float64, ScalarValue::from(100.0)),
        );
        assert!(value_ranges(&predicate).is_empty());
    }

    #[tokio::test]
    async fn a_literal_of_another_type_does_not_prune_a_matching_row() {
        let dataset = coordinate("n", Arc::new(Int32Array::from(vec![0, 1, 2, 3])));
        let predicate = above("n", DataType::Int32, ScalarValue::from(0.5));
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(only_mask(&masks, "n"), [F, T, T, T]);
    }

    #[tokio::test]
    async fn a_null_coordinate_value_is_not_kept() {
        let values = Float64Array::from(vec![Some(1.0), None, Some(9.0)]);
        let dataset = coordinate("x", Arc::new(values));
        let predicate = above("x", DataType::Float64, ScalarValue::from(0.0));
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(only_mask(&masks, "x"), [T, F, T]);
    }

    #[tokio::test]
    async fn a_nan_follows_the_total_order() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![1.0, f64::NAN])));
        let predicate = above("x", DataType::Float64, ScalarValue::from(5.0));
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(only_mask(&masks, "x"), [F, T]);
    }

    #[tokio::test]
    async fn a_rank_2_variable_gets_no_mask_and_no_read() {
        let dataset = grid_dataset("a.nd");
        let predicate = above("temp", DataType::Float64, ScalarValue::from(5.0));
        let masks = masks_of(&dataset, &predicate).await;
        assert!(masks.is_empty());
        assert_eq!(dataset.reads("temp"), 0);
    }

    #[tokio::test]
    async fn a_string_coordinate_gets_no_mask() {
        let dataset = coordinate("s", Arc::new(StringArray::from(vec!["a", "b"])));
        let predicate = above("s", DataType::Utf8, ScalarValue::from("a"));
        let masks = masks_of(&dataset, &predicate).await;
        assert!(masks.is_empty());
        assert_eq!(dataset.reads("s"), 0);
    }

    #[test]
    fn a_chunk_with_no_mask_on_its_axes_is_not_pruned() {
        let masks: Masks = vec![("depth".to_string(), BooleanArray::from(vec![F; 4]))];
        assert!(pruned_starts(&masks).is_empty());
    }

    #[tokio::test]
    async fn a_timestamp_literal_compares_in_the_unit_of_the_coordinate() {
        let values = TimestampSecondArray::from(vec![0, 1, 2, 3]);
        let dataset = coordinate("t", Arc::new(values));
        let data_type = DataType::Timestamp(TimeUnit::Second, None);
        let cases = [
            (
                ScalarValue::TimestampMillisecond(Some(1500), None),
                [F, F, T, T],
            ),
            (
                ScalarValue::TimestampMillisecond(Some(2000), None),
                [F, F, F, T],
            ),
            (
                ScalarValue::TimestampNanosecond(Some(-1), None),
                [T, T, T, T],
            ),
            (ScalarValue::Int64(Some(1)), [F, F, T, T]),
        ];
        for (threshold, expected) in cases {
            let predicate = above("t", data_type.clone(), threshold);
            let masks = masks_of(&dataset, &predicate).await;
            assert_eq!(only_mask(&masks, "t"), expected);
        }
        let schema = Schema::new(vec![Field::new("t", data_type, true)]);
        let at_most = binary(
            col("t", &schema).unwrap(),
            Operator::LtEq,
            lit(ScalarValue::TimestampMicrosecond(Some(2_500_000), None)),
            &schema,
        )
        .unwrap();
        let masks = masks_of(&dataset, &at_most).await;
        assert_eq!(only_mask(&masks, "t"), [T, T, T, F]);
    }

    #[tokio::test]
    async fn an_int64_literal_on_a_timestamp_with_a_unit_change_gives_no_mask() {
        let dataset = coordinate("t", Arc::new(TimestampSecondArray::from(vec![0, 1, 2, 3])));
        let table_type = DataType::Timestamp(TimeUnit::Millisecond, None);
        // In the table unit, 1 is one millisecond, so the rows 1 to 3 meet the filter.
        let predicate = above("t", table_type.clone(), ScalarValue::Int64(Some(1)));
        let masks = masks_in(&dataset, &predicate, "t", table_type).await;
        assert!(masks.is_empty(), "{masks:?}");
        assert_eq!(dataset.reads("t"), 0);
    }

    #[tokio::test]
    async fn a_uint64_coordinate_above_two_to_the_63_keeps_each_matching_row() {
        let big = 1u64 << 63;
        let values = UInt64Array::from(vec![0, big + 10, u64::MAX]);
        let dataset = coordinate("n", Arc::new(values));
        let predicate = above("n", DataType::UInt64, ScalarValue::UInt64(Some(big)));
        let masks = masks_of(&dataset, &predicate).await;
        let mask = only_mask(&masks, "n");
        assert!(
            mask[1] && mask[2],
            "each row above the bound is kept: {mask:?}"
        );
    }

    #[tokio::test]
    async fn a_timestamp_with_a_time_zone_compares_as_one_with_no_zone() {
        let values = TimestampSecondArray::from(vec![0, 1, 2, 3]).with_timezone("UTC");
        let dataset = coordinate("t", Arc::new(values));
        let data_type = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
        let threshold = ScalarValue::TimestampMillisecond(Some(1500), Some("UTC".into()));
        let predicate = above("t", data_type, threshold);
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(only_mask(&masks, "t"), [F, F, T, T]);
    }

    #[test]
    fn a_chunk_at_the_edge_is_checked_and_a_chunk_past_the_mask_is_kept() {
        let grid = ChunkGrid::try_new(&grid_dataset("a.nd"), 12).unwrap();
        let masks: Masks = vec![(
            "lat".to_string(),
            BooleanArray::from(vec![T, T, T, T, F, F, F, F]),
        )];
        // The chunk ends at lat 8, the edge of the dimension.
        assert!(is_pruned(
            &masks,
            &grid,
            &Region::new(vec![0, 4, 0], vec![1, 4, 3])
        ));
        assert!(!is_pruned(
            &masks,
            &grid,
            &Region::new(vec![0, 6, 0], vec![1, 4, 3])
        ));
        assert!(!is_pruned(
            &masks,
            &grid,
            &Region::new(vec![0, 9, 0], vec![1, 1, 3])
        ));
        assert!(!is_pruned(
            &masks,
            &grid,
            &Region::new(vec![0, usize::MAX, 0], vec![1, 2, 3])
        ));
    }

    #[tokio::test]
    async fn a_timestamp_literal_on_a_numeric_coordinate_gets_no_mask() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![1.0, 2.0])));
        let threshold = ScalarValue::TimestampSecond(Some(1), None);
        let predicate = above("x", DataType::Float64, threshold);
        assert!(masks_of(&dataset, &predicate).await.is_empty());
        assert_eq!(dataset.reads("x"), 0);
    }

    #[tokio::test]
    async fn a_strict_bound_that_float64_cannot_hold_keeps_the_row() {
        let big = 1i64 << 53;
        let dataset = coordinate("n", Arc::new(Int64Array::from(vec![big - 1, big, big + 1])));
        let predicate = above("n", DataType::Int64, ScalarValue::from(big));
        let masks = masks_of(&dataset, &predicate).await;
        let mask = only_mask(&masks, "n");
        assert!(!mask[0]);
        // The strict bound becomes inclusive, so the row at the bound stays too.
        assert!(mask[1], "the row at the bound is kept: {mask:?}");
        assert!(mask[2], "the row above the bound is kept: {mask:?}");
    }

    #[tokio::test]
    async fn a_cast_that_truncates_gives_no_mask() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![5.0, 5.5, 6.0])));
        let predicate = cast_compare(
            "x",
            DataType::Float64,
            DataType::Int32,
            Operator::Eq,
            ScalarValue::Int32(Some(5)),
        );
        assert!(masks_of(&dataset, &predicate).await.is_empty());
        assert_eq!(dataset.reads("x"), 0);
    }

    #[tokio::test]
    async fn a_cast_to_a_wider_type_is_looked_through() {
        let dataset = coordinate("x", Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])));
        let predicate = cast_compare(
            "x",
            DataType::Float32,
            DataType::Float64,
            Operator::Gt,
            ScalarValue::from(1.5),
        );
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(only_mask(&masks, "x"), [F, T, T]);
    }

    #[tokio::test]
    async fn an_integer_column_cast_to_float32_gives_no_mask() {
        let dataset = coordinate("n", Arc::new(Int32Array::from(vec![16_777_219])));
        let predicate = cast_compare(
            "n",
            DataType::Int32,
            DataType::Float32,
            Operator::GtEq,
            ScalarValue::Float32(Some(16_777_220.0)),
        );
        assert!(masks_of(&dataset, &predicate).await.is_empty());
    }

    #[tokio::test]
    async fn a_timestamp_cast_to_a_coarser_unit_gives_no_mask() {
        let values = TimestampNanosecondArray::from(vec![0, 1_500_000_000]);
        let dataset = coordinate("t", Arc::new(values));
        let predicate = cast_compare(
            "t",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Second, None),
            Operator::LtEq,
            ScalarValue::TimestampSecond(Some(1), None),
        );
        assert!(masks_of(&dataset, &predicate).await.is_empty());
    }

    #[tokio::test]
    async fn a_cast_on_the_literal_is_evaluated() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![5.0, 5.5, 6.0])));
        let literal: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
            lit(ScalarValue::from(5.7)),
            DataType::Int32,
            None,
        ));
        let predicate = compare(column("x"), Operator::GtEq, literal);
        let ranges = value_ranges(&predicate);
        assert_eq!(ranges["x"].min, Some((ScalarValue::Int32(Some(5)), true)));
        let masks = masks_of(&dataset, &predicate).await;
        assert_eq!(only_mask(&masks, "x"), [T, T, T]);
    }

    #[tokio::test]
    async fn two_compares_with_different_casts_give_no_mask() {
        let dataset = coordinate("x", Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])));
        let wide = cast_compare(
            "x",
            DataType::Float32,
            DataType::Float64,
            Operator::Gt,
            ScalarValue::from(1.5),
        );
        let bare = above("x", DataType::Float32, ScalarValue::Float32(Some(0.5)));
        let predicate = compare(wide, Operator::And, bare);
        assert!(value_ranges(&predicate)["x"].mixed_casts);
        assert!(masks_of(&dataset, &predicate).await.is_empty());
    }

    #[tokio::test]
    async fn a_table_type_narrower_than_the_file_type_gives_no_mask() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])));
        let predicate = above("x", DataType::Float32, ScalarValue::Float32(Some(1.5)));
        let masks = masks_in(&dataset, &predicate, "x", DataType::Float32).await;
        assert!(masks.is_empty());
        assert_eq!(dataset.reads("x"), 0);
    }

    #[tokio::test]
    async fn a_table_type_wider_than_the_file_type_keeps_the_mask() {
        let dataset = coordinate("x", Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])));
        let predicate = above("x", DataType::Float64, ScalarValue::from(1.5));
        let masks = masks_in(&dataset, &predicate, "x", DataType::Float64).await;
        assert_eq!(only_mask(&masks, "x"), [F, T, T]);
    }

    #[tokio::test]
    async fn a_column_the_table_types_do_not_hold_gets_no_mask() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])));
        let predicate = above("x", DataType::Float64, ScalarValue::from(1.5));
        let masks = masks_in(&dataset, &predicate, "y", DataType::Float64).await;
        assert!(masks.is_empty());
        assert_eq!(dataset.reads("x"), 0);
    }

    // The tests of the predicate walk.

    fn column(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    fn lit_i64(value: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int64(Some(value))))
    }

    fn compare(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    fn ranges_of(expr: Arc<dyn PhysicalExpr>) -> HashMap<String, ValueRange> {
        value_ranges(&expr)
    }

    /// (value as i64, inclusive) of a bound, for short asserts.
    fn bound(bound: &Option<(ScalarValue, bool)>) -> Option<(i64, bool)> {
        bound.as_ref().map(|(value, inclusive)| match value {
            ScalarValue::Int64(Some(v)) => (*v, *inclusive),
            other => panic!("unexpected scalar {other:?}"),
        })
    }

    #[test]
    fn test_column_op_literal_becomes_a_lower_bound() {
        let ranges = ranges_of(compare(column("time"), Operator::Gt, lit_i64(5)));
        let range = ranges.get("time").expect("time range");
        assert_eq!(bound(&range.min), Some((5, false)));
        assert_eq!(bound(&range.max), None);
    }

    #[test]
    fn test_gteq_is_an_inclusive_lower_bound() {
        let ranges = ranges_of(compare(column("time"), Operator::GtEq, lit_i64(5)));
        assert_eq!(bound(&ranges["time"].min), Some((5, true)));
    }

    #[test]
    fn test_literal_on_the_left_flips_the_operator() {
        // `5 < time` is `time > 5`, not `time < 5`.
        let ranges = ranges_of(compare(lit_i64(5), Operator::Lt, column("time")));
        let range = &ranges["time"];
        assert_eq!(bound(&range.min), Some((5, false)));
        assert_eq!(bound(&range.max), None);
    }

    #[test]
    fn test_equality_pins_both_bounds_inclusively() {
        let ranges = ranges_of(compare(column("time"), Operator::Eq, lit_i64(7)));
        let range = &ranges["time"];
        assert_eq!(bound(&range.min), Some((7, true)));
        assert_eq!(bound(&range.max), Some((7, true)));
    }

    #[test]
    fn test_and_tree_tightens_both_ends_per_column() {
        // (time >= 10 AND time < 100) AND depth <= 50
        let expr = compare(
            compare(
                compare(column("time"), Operator::GtEq, lit_i64(10)),
                Operator::And,
                compare(column("time"), Operator::Lt, lit_i64(100)),
            ),
            Operator::And,
            compare(column("depth"), Operator::LtEq, lit_i64(50)),
        );
        let ranges = ranges_of(expr);

        assert_eq!(bound(&ranges["time"].min), Some((10, true)));
        assert_eq!(bound(&ranges["time"].max), Some((100, false)));
        assert_eq!(bound(&ranges["depth"].max), Some((50, true)));
        assert_eq!(bound(&ranges["depth"].min), None);
    }

    #[test]
    fn test_repeated_bounds_keep_the_tighter_one() {
        // time > 5 AND time > 20 gives the lower bound 20.
        let expr = compare(
            compare(column("time"), Operator::Gt, lit_i64(5)),
            Operator::And,
            compare(column("time"), Operator::Gt, lit_i64(20)),
        );
        assert_eq!(bound(&ranges_of(expr)["time"].min), Some((20, false)));

        // The order of the conjuncts does not change the bound.
        let expr = compare(
            compare(column("time"), Operator::Gt, lit_i64(20)),
            Operator::And,
            compare(column("time"), Operator::Gt, lit_i64(5)),
        );
        assert_eq!(bound(&ranges_of(expr)["time"].min), Some((20, false)));
    }

    #[test]
    fn test_equal_bound_values_keep_the_exclusive_variant() {
        // time >= 5 AND time > 5: the exclusive bound wins.
        let expr = compare(
            compare(column("time"), Operator::GtEq, lit_i64(5)),
            Operator::And,
            compare(column("time"), Operator::Gt, lit_i64(5)),
        );
        assert_eq!(bound(&ranges_of(expr)["time"].min), Some((5, false)));
    }

    #[test]
    fn test_a_column_cast_is_recorded_and_a_literal_cast_is_evaluated() {
        let casted_col: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(column("time"), DataType::Int64, None));
        let casted_lit: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(lit_i64(5), DataType::Int64, None));

        // The column cast is recorded, and the literal cast is evaluated.
        let ranges = ranges_of(compare(casted_col, Operator::Gt, casted_lit));
        assert_eq!(bound(&ranges["time"].min), Some((5, false)));
        assert_eq!(ranges["time"].column_casts, [DataType::Int64]);
        assert!(!ranges["time"].mixed_casts);
    }

    #[test]
    fn test_nested_column_casts_are_recorded_from_the_column_out() {
        let inner: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(column("time"), DataType::Int32, None));
        let outer: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(inner, DataType::Int64, None));
        let ranges = ranges_of(compare(outer, Operator::Gt, lit_i64(5)));
        assert_eq!(
            ranges["time"].column_casts,
            [DataType::Int32, DataType::Int64]
        );
    }

    #[test]
    fn test_a_literal_cast_that_fails_gives_no_bound() {
        let text: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Utf8(Some("abc".into()))));
        let casted_lit: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(text, DataType::Int64, None));
        let ranges = ranges_of(compare(column("time"), Operator::Gt, casted_lit));
        assert!(ranges.is_empty(), "unexpected ranges: {ranges:?}");
    }

    #[test]
    fn test_null_literals_are_not_pushed_down() {
        let null_lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(None)));
        let ranges = ranges_of(compare(column("time"), Operator::Gt, null_lit));
        assert!(ranges.is_empty(), "unexpected ranges: {ranges:?}");
    }

    #[test]
    fn test_string_literals_are_not_pushed_down() {
        let string_lit: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Utf8(Some("abc".into()))));
        let ranges = ranges_of(compare(column("name"), Operator::Gt, string_lit));
        assert!(ranges.is_empty(), "unexpected ranges: {ranges:?}");
    }

    #[test]
    fn test_unsupported_operator_registers_an_unbounded_range() {
        // `!=` gives no bound: the column has a range, but the range stays open.
        let ranges = ranges_of(compare(column("time"), Operator::NotEq, lit_i64(5)));
        let range = &ranges["time"];
        assert_eq!(bound(&range.min), None);
        assert_eq!(bound(&range.max), None);
    }

    /// An `OR` gives no bound.
    ///
    /// A row can meet `time > 10 OR time > 100` and fail `time > 100`.
    /// A prune on either side drops rows that meet the predicate.
    #[test]
    fn test_or_yields_no_bound() {
        let expr = compare(
            compare(column("time"), Operator::Gt, lit_i64(10)),
            Operator::Or,
            compare(column("time"), Operator::Gt, lit_i64(100)),
        );
        assert!(
            ranges_of(expr).is_empty(),
            "a disjunction implies no bound on either side"
        );
    }

    /// A `NOT` gives no bound.
    ///
    /// `NOT (time > 5)` is `time <= 5`, the opposite of its child.
    #[test]
    fn test_negation_yields_no_bound() {
        let inner = compare(column("time"), Operator::Gt, lit_i64(5));
        let negated: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(inner));
        assert!(
            ranges_of(negated).is_empty(),
            "a negation implies no bound on its child"
        );
    }

    /// An `AND` that holds an `OR` keeps the bounds of its other conjuncts.
    #[test]
    fn test_a_conjunction_keeps_the_bounds_beside_a_disjunction() {
        // time >= 10 AND (depth > 1 OR depth > 900)
        let expr = compare(
            compare(column("time"), Operator::GtEq, lit_i64(10)),
            Operator::And,
            compare(
                compare(column("depth"), Operator::Gt, lit_i64(1)),
                Operator::Or,
                compare(column("depth"), Operator::Gt, lit_i64(900)),
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
    fn test_timestamp_literals_are_pushdown_candidates() {
        let ts: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
            Some(1_600_000_000_000_000_000),
            None,
        )));
        let ranges = ranges_of(compare(column("time"), Operator::GtEq, ts));
        assert!(ranges.contains_key("time"));
        assert!(ranges["time"].min.is_some());
    }
}
