use std::sync::Arc;

use datafusion::scalar::ScalarValue;

use crate::{NdArray, NdArrayD, datatypes::NdArrayDataType, datatypes::TimestampNanosecond};

pub fn is_pushdown_candidate<'a>(array: &'a Arc<dyn NdArrayD>) -> bool {
    let dims = array.dimensions();
    let shape = array.shape();

    if dims.len() != 1 || shape[0] == 0 {
        return false;
    }

    if !is_pushdown_type(&array.datatype()) {
        return false;
    }

    true
}

pub fn is_pushdown_candidate_ragged(
    array: &Arc<dyn NdArrayD>,
    instance_dimension: &String,
) -> bool {
    let dims = array.dimensions();
    let shape = array.shape();

    if dims.len() != 1 || shape[0] == 0 {
        return false;
    }

    if !is_pushdown_type(&array.datatype()) {
        return false;
    }

    if &dims[0] == instance_dimension {
        return true;
    }

    false
}

pub fn is_pushdown_type(dt: &NdArrayDataType) -> bool {
    matches!(
        dt,
        NdArrayDataType::I8
            | NdArrayDataType::I16
            | NdArrayDataType::I32
            | NdArrayDataType::I64
            | NdArrayDataType::U8
            | NdArrayDataType::U16
            | NdArrayDataType::U32
            | NdArrayDataType::U64
            | NdArrayDataType::F32
            | NdArrayDataType::F64
            | NdArrayDataType::Timestamp
    )
}

/// A value range for a single column, using DataFusion `ScalarValue` natively.
/// Bounds are tightened as multiple predicates are encountered.
#[derive(Debug, Clone)]
pub struct ValueRange {
    pub min: Option<(ScalarValue, bool)>, // (value, inclusive)
    pub max: Option<(ScalarValue, bool)>,
}

impl ValueRange {
    pub fn empty() -> Self {
        Self {
            min: None,
            max: None,
        }
    }

    /// Tighten this range with a lower bound.
    pub fn with_lower(&mut self, value: ScalarValue, inclusive: bool) {
        match &self.min {
            None => self.min = Some((value, inclusive)),
            Some((existing, existing_inc)) => {
                if let Some(ord) = existing.partial_cmp(&value) {
                    use std::cmp::Ordering::*;
                    match ord {
                        Less => self.min = Some((value, inclusive)),
                        Equal => self.min = Some((value, *existing_inc && inclusive)),
                        Greater => {} // keep tighter existing
                    }
                }
            }
        }
    }

    /// Tighten this range with an upper bound.
    pub fn with_upper(&mut self, value: ScalarValue, inclusive: bool) {
        match &self.max {
            None => self.max = Some((value, inclusive)),
            Some((existing, existing_inc)) => {
                if let Some(ord) = existing.partial_cmp(&value) {
                    use std::cmp::Ordering::*;
                    match ord {
                        Greater => self.max = Some((value, inclusive)),
                        Equal => self.max = Some((value, *existing_inc && inclusive)),
                        Less => {} // keep tighter existing
                    }
                }
            }
        }
    }
}

pub async fn mask_pushdown(
    array: Arc<dyn NdArrayD>,
    value_range: &ValueRange,
) -> anyhow::Result<Vec<bool>> {
    resolve_array_mask(&array, value_range).await.ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to resolve pushdown mask for array. Expected a 1D numeric/timestamp array with compatible range bounds. datatype={:?}, dims={:?}, range={:?}",
            array.datatype(),
            array.dimensions(),
            value_range
        )
    })
}

/// Bound specification for `compute_mask`.
pub struct Bound<'a, T> {
    pub value: &'a T,
    pub inclusive: bool,
}

/// Compute a boolean mask over `values`, marking `true` where the value
/// satisfies the given min/max bounds.
///
/// For float types, NaN values are always `false`.
pub fn compute_mask<T: PartialOrd>(
    values: &[T],
    min: Option<Bound<'_, T>>,
    max: Option<Bound<'_, T>>,
) -> Vec<bool> {
    values
        .iter()
        .map(|val| {
            let above_min = match &min {
                None => true,
                Some(b) => match val.partial_cmp(b.value) {
                    Some(std::cmp::Ordering::Greater) => true,
                    Some(std::cmp::Ordering::Equal) => b.inclusive,
                    Some(std::cmp::Ordering::Less) => false,
                    None => false,
                },
            };
            let below_max = match &max {
                None => true,
                Some(b) => match val.partial_cmp(b.value) {
                    Some(std::cmp::Ordering::Less) => true,
                    Some(std::cmp::Ordering::Equal) => b.inclusive,
                    Some(std::cmp::Ordering::Greater) => false,
                    None => false,
                },
            };
            above_min && below_max
        })
        .collect()
}

// ─── resolve_array_mask ────────────────────────────────────────────────────

/// Resolve a `ValueRange` against a coordinate array, producing a boolean mask.
///
/// Returns `None` if the array type is unsupported or the array is not 1D.
/// The returned `Vec<bool>` has one entry per element in the array's first axis.
pub async fn resolve_array_mask(
    coord_array: &Arc<dyn NdArrayD>,
    range: &ValueRange,
) -> Option<Vec<bool>> {
    let dims = coord_array.dimensions();
    if dims.len() != 1 {
        return None;
    }

    macro_rules! resolve_typed {
        ($ty:ty, $scalar_fn:ident) => {{
            let typed = coord_array.as_any().downcast_ref::<NdArray<$ty>>()?;
            let values = typed.clone_into_raw_vec().await;
            let min_val = range.min.as_ref().and_then(|(sv, _)| $scalar_fn(sv));
            let max_val = range.max.as_ref().and_then(|(sv, _)| $scalar_fn(sv));
            let min_inc = range.min.as_ref().map_or(true, |(_, inc)| *inc);
            let max_inc = range.max.as_ref().map_or(true, |(_, inc)| *inc);
            let min_bound = min_val.as_ref().map(|v| Bound {
                value: v,
                inclusive: min_inc,
            });
            let max_bound = max_val.as_ref().map(|v| Bound {
                value: v,
                inclusive: max_inc,
            });
            Some(compute_mask(&values, min_bound, max_bound))
        }};
    }

    match coord_array.datatype() {
        NdArrayDataType::I8 => resolve_typed!(i8, scalar_to_i8),
        NdArrayDataType::I16 => resolve_typed!(i16, scalar_to_i16),
        NdArrayDataType::I32 => resolve_typed!(i32, scalar_to_i32),
        NdArrayDataType::I64 => resolve_typed!(i64, scalar_to_i64),
        NdArrayDataType::U8 => resolve_typed!(u8, scalar_to_u8),
        NdArrayDataType::U16 => resolve_typed!(u16, scalar_to_u16),
        NdArrayDataType::U32 => resolve_typed!(u32, scalar_to_u32),
        NdArrayDataType::U64 => resolve_typed!(u64, scalar_to_u64),
        NdArrayDataType::F32 => resolve_typed!(f32, scalar_to_f32),
        NdArrayDataType::F64 => resolve_typed!(f64, scalar_to_f64),
        NdArrayDataType::Timestamp => {
            let typed = coord_array
                .as_any()
                .downcast_ref::<NdArray<TimestampNanosecond>>()?;
            let values = typed.clone_into_raw_vec().await;
            let raw_values: Vec<i64> = values.into_iter().map(|t| t.0).collect();
            let min_val = range
                .min
                .as_ref()
                .and_then(|(sv, _)| scalar_to_timestamp_nanos(sv));
            let max_val = range
                .max
                .as_ref()
                .and_then(|(sv, _)| scalar_to_timestamp_nanos(sv));
            let min_inc = range.min.as_ref().map_or(true, |(_, inc)| *inc);
            let max_inc = range.max.as_ref().map_or(true, |(_, inc)| *inc);
            let min_bound = min_val.as_ref().map(|v| Bound {
                value: v,
                inclusive: min_inc,
            });
            let max_bound = max_val.as_ref().map(|v| Bound {
                value: v,
                inclusive: max_inc,
            });
            Some(compute_mask(&raw_values, min_bound, max_bound))
        }
        _ => None,
    }
}

// ─── compute_chunk_mask ────────────────────────────────────────────────────

/// Compute a flat boolean mask for a chunk, given per-dimension masks.
///
/// For each axis of the chunk, the corresponding dimension mask is sliced
/// to `[chunk_start[axis]..chunk_start[axis]+chunk_shape[axis])`. The flat
/// mask is the row-major cartesian product of all axis masks: a row is `true`
/// only if every axis contributing to that row is `true`.
///
/// If no masks match the chunk's dimensions, returns an all-true vector.
pub fn compute_chunk_mask(
    masks: &[(String, Vec<bool>)],
    chunk_dims: &[String],
    chunk_start: &[usize],
    chunk_shape: &[usize],
) -> Vec<bool> {
    let total_elements: usize = chunk_shape.iter().product();
    if total_elements == 0 {
        return Vec::new();
    }

    // Collect per-axis mask slices relevant to this chunk.
    let axis_masks: Vec<Option<&[bool]>> = chunk_dims
        .iter()
        .enumerate()
        .map(|(axis, dim)| {
            masks.iter().find(|(name, _)| name == dim).map(|(_, mask)| {
                let start = chunk_start[axis];
                let end = start + chunk_shape[axis];
                &mask[start..end]
            })
        })
        .collect();

    // If no axes have masks, everything passes.
    if axis_masks.iter().all(|m| m.is_none()) {
        return vec![true; total_elements];
    }

    // Row-major cartesian product.
    let mut result = vec![true; total_elements];
    let ndim = chunk_shape.len();

    for flat_idx in 0..total_elements {
        // Decompose flat index into per-axis indices.
        let mut remaining = flat_idx;
        let mut passes = true;
        for axis in 0..ndim {
            let stride: usize = chunk_shape[axis + 1..].iter().product();
            let idx = remaining / stride;
            remaining %= stride;

            if let Some(mask_slice) = axis_masks[axis] {
                if !mask_slice[idx] {
                    passes = false;
                    break;
                }
            }
        }
        result[flat_idx] = passes;
    }

    result
}

/// Returns whether a mask is all-true.
pub fn mask_is_all_true(mask: &[bool]) -> bool {
    mask.iter().all(|&v| v)
}

/// Returns whether a mask is all-false.
pub fn mask_is_all_false(mask: &[bool]) -> bool {
    mask.iter().all(|&v| !v)
}

// ─── is_pushdown_type ──────────────────────────────────────────────────────

// ─── Scalar conversion helpers ─────────────────────────────────────────────

fn scalar_to_i8(sv: &ScalarValue) -> Option<i8> {
    match sv {
        ScalarValue::Int8(Some(v)) => Some(*v),
        ScalarValue::Int16(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int64(Some(v)) => (*v).try_into().ok(),
        _ => None,
    }
}

fn scalar_to_i16(sv: &ScalarValue) -> Option<i16> {
    match sv {
        ScalarValue::Int8(Some(v)) => Some(*v as i16),
        ScalarValue::Int16(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int64(Some(v)) => (*v).try_into().ok(),
        _ => None,
    }
}

fn scalar_to_i32(sv: &ScalarValue) -> Option<i32> {
    match sv {
        ScalarValue::Int8(Some(v)) => Some(*v as i32),
        ScalarValue::Int16(Some(v)) => Some(*v as i32),
        ScalarValue::Int32(Some(v)) => Some(*v),
        ScalarValue::Int64(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Float32(Some(v)) => Some(*v as i32),
        ScalarValue::Float64(Some(v)) => Some(*v as i32),
        _ => None,
    }
}

fn scalar_to_i64(sv: &ScalarValue) -> Option<i64> {
    match sv {
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(*v as i64),
        ScalarValue::Float64(Some(v)) => Some(*v as i64),
        _ => None,
    }
}

fn scalar_to_u8(sv: &ScalarValue) -> Option<u8> {
    match sv {
        ScalarValue::UInt8(Some(v)) => Some(*v),
        ScalarValue::UInt16(Some(v)) => (*v).try_into().ok(),
        ScalarValue::UInt32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::UInt64(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int64(Some(v)) => (*v).try_into().ok(),
        _ => None,
    }
}

fn scalar_to_u16(sv: &ScalarValue) -> Option<u16> {
    match sv {
        ScalarValue::UInt8(Some(v)) => Some(*v as u16),
        ScalarValue::UInt16(Some(v)) => Some(*v),
        ScalarValue::UInt32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::UInt64(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int64(Some(v)) => (*v).try_into().ok(),
        _ => None,
    }
}

fn scalar_to_u32(sv: &ScalarValue) -> Option<u32> {
    match sv {
        ScalarValue::UInt8(Some(v)) => Some(*v as u32),
        ScalarValue::UInt16(Some(v)) => Some(*v as u32),
        ScalarValue::UInt32(Some(v)) => Some(*v),
        ScalarValue::UInt64(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int64(Some(v)) => (*v).try_into().ok(),
        _ => None,
    }
}

fn scalar_to_u64(sv: &ScalarValue) -> Option<u64> {
    match sv {
        ScalarValue::UInt8(Some(v)) => Some(*v as u64),
        ScalarValue::UInt16(Some(v)) => Some(*v as u64),
        ScalarValue::UInt32(Some(v)) => Some(*v as u64),
        ScalarValue::UInt64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::Int64(Some(v)) => (*v).try_into().ok(),
        _ => None,
    }
}

fn scalar_to_f32(sv: &ScalarValue) -> Option<f32> {
    match sv {
        ScalarValue::Float32(Some(v)) => Some(*v),
        ScalarValue::Float64(Some(v)) => Some(*v as f32),
        ScalarValue::Int32(Some(v)) => Some(*v as f32),
        ScalarValue::Int64(Some(v)) => Some(*v as f32),
        _ => None,
    }
}

fn scalar_to_f64(sv: &ScalarValue) -> Option<f64> {
    match sv {
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt16(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        _ => None,
    }
}

/// Convert any timestamp ScalarValue to nanoseconds (i64).
pub fn scalar_to_timestamp_nanos(sv: &ScalarValue) -> Option<i64> {
    match sv {
        ScalarValue::TimestampNanosecond(Some(ns), _) => Some(*ns),
        ScalarValue::TimestampMicrosecond(Some(us), _) => Some(*us * 1_000),
        ScalarValue::TimestampMillisecond(Some(ms), _) => Some(*ms * 1_000_000),
        ScalarValue::TimestampSecond(Some(s), _) => Some(*s * 1_000_000_000),
        ScalarValue::Int64(Some(v)) => Some(*v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_mask_sorted() {
        let values = vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0];
        let mask = compute_mask(
            &values,
            Some(Bound {
                value: &20.0,
                inclusive: true,
            }),
            Some(Bound {
                value: &40.0,
                inclusive: true,
            }),
        );
        assert_eq!(mask, vec![false, false, true, true, true, false]);
    }

    #[test]
    fn test_compute_mask_unsorted() {
        let values = vec![50.0, 10.0, 80.0, 30.0, 70.0, 20.0, 60.0, 40.0];
        let mask = compute_mask(
            &values,
            Some(Bound {
                value: &30.0,
                inclusive: true,
            }),
            Some(Bound {
                value: &60.0,
                inclusive: true,
            }),
        );
        assert_eq!(
            mask,
            vec![true, false, false, true, false, false, true, true]
        );
    }

    #[test]
    fn test_compute_mask_exclusive() {
        let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let mask = compute_mask(
            &values,
            Some(Bound {
                value: &20.0,
                inclusive: false,
            }),
            Some(Bound {
                value: &40.0,
                inclusive: false,
            }),
        );
        assert_eq!(mask, vec![false, false, true, false, false]);
    }

    #[test]
    fn test_compute_mask_nan_excluded() {
        let values = vec![f64::NAN, 10.0, 20.0, f64::NAN, 30.0];
        let mask = compute_mask(
            &values,
            Some(Bound {
                value: &10.0,
                inclusive: true,
            }),
            Some(Bound {
                value: &30.0,
                inclusive: true,
            }),
        );
        assert_eq!(mask, vec![false, true, true, false, true]);
    }

    #[test]
    fn test_compute_mask_no_bounds() {
        let values = vec![1.0, 2.0, 3.0];
        let mask = compute_mask::<f64>(&values, None, None);
        assert_eq!(mask, vec![true, true, true]);
    }

    #[test]
    fn test_compute_mask_integers() {
        let values: Vec<i32> = vec![5, 10, 15, 20, 25];
        let mask = compute_mask(
            &values,
            Some(Bound {
                value: &10,
                inclusive: true,
            }),
            Some(Bound {
                value: &20,
                inclusive: false,
            }),
        );
        assert_eq!(mask, vec![false, true, true, false, false]);
    }

    #[test]
    fn test_scalar_to_timestamp_nanos() {
        assert_eq!(
            scalar_to_timestamp_nanos(&ScalarValue::TimestampNanosecond(Some(123), None)),
            Some(123)
        );
        assert_eq!(
            scalar_to_timestamp_nanos(&ScalarValue::TimestampMicrosecond(Some(5), None)),
            Some(5000)
        );
        assert_eq!(
            scalar_to_timestamp_nanos(&ScalarValue::TimestampMillisecond(Some(3), None)),
            Some(3_000_000)
        );
        assert_eq!(
            scalar_to_timestamp_nanos(&ScalarValue::TimestampSecond(Some(1), None)),
            Some(1_000_000_000)
        );
    }

    #[test]
    fn test_compute_chunk_mask_1d() {
        let masks = vec![("x".to_string(), vec![true, false, true, true, false])];
        let result = compute_chunk_mask(&masks, &["x".to_string()], &[1], &[3]);
        // slice [1..4] of mask = [false, true, true]
        assert_eq!(result, vec![false, true, true]);
    }

    #[test]
    fn test_compute_chunk_mask_2d() {
        // x mask: [true, false, true], y mask: [false, true]
        let masks = vec![
            ("x".to_string(), vec![true, false, true]),
            ("y".to_string(), vec![false, true]),
        ];
        let dims = vec!["x".to_string(), "y".to_string()];
        let result = compute_chunk_mask(&masks, &dims, &[0, 0], &[3, 2]);
        // Cartesian product (row-major): x[0]*y[0], x[0]*y[1], x[1]*y[0], x[1]*y[1], x[2]*y[0], x[2]*y[1]
        // = T*F, T*T, F*F, F*T, T*F, T*T
        assert_eq!(result, vec![false, true, false, false, false, true]);
    }

    #[test]
    fn test_compute_chunk_mask_no_matching_dims() {
        let masks = vec![("z".to_string(), vec![true, false])];
        let result = compute_chunk_mask(&masks, &["x".to_string()], &[0], &[5]);
        assert_eq!(result, vec![true; 5]);
    }

    #[test]
    fn test_compute_chunk_mask_all_false() {
        let masks = vec![("x".to_string(), vec![false, false, false])];
        let result = compute_chunk_mask(&masks, &["x".to_string()], &[0], &[3]);
        assert_eq!(result, vec![false, false, false]);
    }

    #[test]
    fn test_mask_helpers() {
        assert!(mask_is_all_true(&[true, true, true]));
        assert!(!mask_is_all_true(&[true, false, true]));
        assert!(mask_is_all_false(&[false, false, false]));
        assert!(!mask_is_all_false(&[false, true, false]));
    }
}
