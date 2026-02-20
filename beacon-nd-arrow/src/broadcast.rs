//! Broadcasting and materialization for `NdArrowArray`.
//!
//! This module implements **named broadcasting** (xarray-style, name-aligned).
//!
//! Axes are aligned by name:
//! - The input dim names must be a subset of the target dim names.
//! - The relative order of the input axes must be preserved (no implicit transpose).
//! - Any missing dim in the input is treated as size `1` and will broadcast.

use std::collections::{HashMap, HashSet};

use crate::{
    NdArrowArray,
    dimensions::{Dimension, Dimensions},
    error::{BroadcastError, NdArrayError},
    view::ArrayBroadcastView,
};

pub fn broadcast_arrays(arrays: &[NdArrowArray]) -> Result<Vec<ArrayBroadcastView>, NdArrayError> {
    let target_dims = find_broadcast_dimensions(
        &arrays
            .iter()
            .map(|a| a.dimensions().clone())
            .collect::<Vec<_>>(),
    )?;
    arrays
        .iter()
        .map(|a| broadcast_nd_array(a, &target_dims))
        .collect()
}

/// Broadcast an ND array to `target` and materialize it.
///
/// Broadcasting is implemented by computing output-to-input indices and using `arrow::compute::take`.
pub fn broadcast_nd_array(
    array: &NdArrowArray,
    target: &Dimensions,
) -> Result<ArrayBroadcastView, NdArrayError> {
    target.validate()?;
    let (out_shape, out_strides) = compute_broadcast_shape_and_strides(array.dimensions(), target)?;

    Ok(ArrayBroadcastView::new(
        array.clone(),
        out_shape,
        out_strides,
    ))
}

/// Compute the broadcasted output dimensions for a set of inputs.
pub fn find_broadcast_dimensions(dimensions: &[Dimensions]) -> Result<Dimensions, BroadcastError> {
    if dimensions.is_empty() {
        return Ok(Dimensions::Scalar);
    }

    // Named broadcasting (xarray-style): union of dim names.
    // Order rule: preserve the first array's order, then append new dims as encountered.
    let mut order: Vec<String> = vec![];
    let mut sizes_by_name: HashMap<String, usize> = HashMap::new();

    for dims in dimensions {
        let dims_vec = dims.as_multi_dimensional().cloned().unwrap_or_default();
        validate_unique_dim_names(&dims_vec)
            .map_err(|_| BroadcastError::NoBroadcastableShape(dimensions.to_vec()))?;

        for d in &dims_vec {
            if !sizes_by_name.contains_key(d.name()) {
                order.push(d.name().to_string());
                sizes_by_name.insert(d.name().to_string(), 1);
            }
        }
    }

    for name in &order {
        let mut out_size = 1usize;
        for dims in dimensions {
            let dims_vec = dims.as_multi_dimensional().cloned().unwrap_or_default();
            let s = dims_vec
                .iter()
                .find(|d| d.name() == name)
                .map(|d| d.size())
                .unwrap_or(1);

            if s == out_size || s == 1 {
                // ok
            } else if out_size == 1 {
                out_size = s;
            } else {
                return Err(BroadcastError::NoBroadcastableShape(dimensions.to_vec()));
            }
        }
        sizes_by_name.insert(name.clone(), out_size);
    }

    let out_dims = order
        .into_iter()
        .map(|name| {
            let size = *sizes_by_name.get(&name).unwrap_or(&1);
            Dimension::new_unchecked(name, size)
        })
        .collect::<Vec<_>>();
    Ok(Dimensions::new(out_dims))
}

/// Validate that a set of dimension names does not contain duplicates.
///
/// Duplicate names are ambiguous for named broadcasting.
fn validate_unique_dim_names(dims: &[Dimension]) -> Result<(), BroadcastError> {
    let mut seen = HashSet::new();
    for d in dims {
        if !seen.insert(d.name()) {
            return Err(BroadcastError::NoBroadcastableShape(vec![Dimensions::new(
                dims.to_vec(),
            )]));
        }
    }
    Ok(())
}

fn compute_broadcast_shape_and_strides(
    input: &Dimensions,
    target: &Dimensions,
) -> Result<(Vec<usize>, Vec<isize>), BroadcastError> {
    let in_dims = input.as_multi_dimensional().cloned().unwrap_or_default();
    let out_dims = target.as_multi_dimensional().cloned().unwrap_or_default();

    validate_unique_dim_names(&in_dims)?;
    validate_unique_dim_names(&out_dims)?;

    let out_names: Vec<&str> = out_dims.iter().map(|d| d.name()).collect();
    let out_positions: HashMap<&str, usize> =
        out_names.iter().enumerate().map(|(i, n)| (*n, i)).collect();

    // Input dims must be a subset of target dims.
    for d in &in_dims {
        if !out_positions.contains_key(d.name()) {
            return Err(BroadcastError::IncompatibleShapes(
                input.clone(),
                target.clone(),
            ));
        }
    }

    // Order must be preserved (no implicit transpose): input dims must appear as a subsequence.
    let mut last_pos = None;
    for d in &in_dims {
        let pos = *out_positions.get(d.name()).unwrap();
        if let Some(prev) = last_pos {
            if pos <= prev {
                return Err(BroadcastError::IncompatibleShapes(
                    input.clone(),
                    target.clone(),
                ));
            }
        }
        last_pos = Some(pos);
    }

    let in_shape = input.shape();
    let in_strides = row_major_strides(&in_shape);

    let mut in_by_name: HashMap<&str, (usize, isize)> = HashMap::new();
    for (idx, d) in in_dims.iter().enumerate() {
        in_by_name.insert(d.name(), (d.size(), in_strides[idx]));
    }

    let mut out_strides: Vec<isize> = Vec::with_capacity(out_dims.len());
    let mut out_shape: Vec<usize> = Vec::with_capacity(out_dims.len());

    for out_dim in &out_dims {
        let out_size = out_dim.size();
        out_shape.push(out_size);

        match in_by_name.get(out_dim.name()) {
            None => {
                // Missing dim in input => broadcast.
                out_strides.push(0);
            }
            Some((in_size, in_stride)) => {
                if *in_size == out_size {
                    out_strides.push(*in_stride);
                } else if *in_size == 1 && out_size > 1 {
                    out_strides.push(0);
                } else {
                    return Err(BroadcastError::IncompatibleShapes(
                        input.clone(),
                        target.clone(),
                    ));
                }
            }
        }
    }

    Ok((out_shape, out_strides))
}

/// Compute row-major (C-order) strides for a shape.
///
/// The returned strides map a multi-index into a flat index for a contiguous row-major array.
fn row_major_strides(shape: &[usize]) -> Vec<isize> {
    if shape.is_empty() {
        return vec![];
    }
    let mut strides = vec![0isize; shape.len()];
    let mut stride = 1isize;
    for (i, dim) in shape.iter().enumerate().rev() {
        strides[i] = stride;
        stride *= *dim as isize;
    }
    strides
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;

    use crate::{
        NdArrowArray,
        dimensions::Dimension,
        dimensions::Dimensions,
        error::{BroadcastError, NdArrayError},
    };

    fn dim(name: &str, size: usize) -> Dimension {
        Dimension::try_new(name, size).unwrap()
    }

    fn take_all_values(view: &crate::view::ArrayBroadcastView) -> Vec<i32> {
        let taken = view.take(0, view.len());
        taken
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn broadcasting_materializes_expected_values() {
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![dim("t", 1), dim("x", 3)]),
        )
        .unwrap();

        let b = a
            .broadcast_to(&Dimensions::new(vec![dim("t", 2), dim("x", 3)]))
            .unwrap();

        assert_eq!(b.len(), 6);
        assert_eq!(take_all_values(&b), vec![1, 2, 3, 1, 2, 3]);
    }

    #[test]
    fn broadcasting_scalar_to_matrix_repeats_value() {
        let scalar =
            NdArrowArray::new(Arc::new(Int32Array::from(vec![42])), Dimensions::Scalar).unwrap();

        let b = scalar
            .broadcast_to(&Dimensions::new(vec![dim("y", 2), dim("x", 3)]))
            .unwrap();

        assert_eq!(b.len(), 6);
        assert_eq!(take_all_values(&b), vec![42, 42, 42, 42, 42, 42]);
    }

    #[test]
    fn broadcasting_incompatible_shapes_errors() {
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Dimensions::new(vec![dim("t", 2), dim("x", 3)]),
        )
        .unwrap();

        let err = a
            .broadcast_to(&Dimensions::new(vec![dim("t", 2), dim("x", 2)]))
            .unwrap_err();
        assert!(matches!(
            err,
            NdArrayError::BroadcastingError(BroadcastError::IncompatibleShapes(_, _))
        ));
    }

    #[test]
    fn broadcasting_named_dims_respects_names_and_order() {
        let dims_in = Dimensions::new(vec![dim("x", 3)]);
        let a = NdArrowArray::new(Arc::new(Int32Array::from(vec![1, 2, 3])), dims_in).unwrap();

        // Add a new leading dim "t".
        let target = Dimensions::new(vec![dim("t", 2), dim("x", 3)]);
        let b = a.broadcast_to(&target).unwrap();
        assert_eq!(b.len(), 6);
        assert_eq!(take_all_values(&b), vec![1, 2, 3, 1, 2, 3]);

        // Reordering existing dims is not allowed (would require transpose).
        let dims2 = Dimensions::new(vec![dim("x", 3), dim("y", 2)]);
        let a2 = NdArrowArray::new(
            Arc::new(Int32Array::from((0..6).collect::<Vec<_>>())),
            dims2,
        )
        .unwrap();
        let bad_target = Dimensions::new(vec![dim("y", 2), dim("x", 3)]);
        let err = a2.broadcast_to(&bad_target).unwrap_err();
        assert!(matches!(
            err,
            NdArrayError::BroadcastingError(BroadcastError::IncompatibleShapes(_, _))
        ));
    }

    #[test]
    fn broadcasting_scalar_to_3d_repeats_value() {
        let scalar =
            NdArrowArray::new(Arc::new(Int32Array::from(vec![7])), Dimensions::Scalar).unwrap();

        let b = scalar
            .broadcast_to(&Dimensions::new(vec![
                dim("a", 2),
                dim("b", 2),
                dim("c", 2),
            ]))
            .unwrap();

        assert_eq!(b.len(), 8);
        assert_eq!(take_all_values(&b), vec![7, 7, 7, 7, 7, 7, 7, 7]);
    }

    #[test]
    fn broadcasting_1d_to_3d_trailing_axis() {
        // x=3 -> a=2, b=4, x=3: broadcast over new leading dims.
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![dim("x", 3)]),
        )
        .unwrap();

        let b = a
            .broadcast_to(&Dimensions::new(vec![
                dim("a", 2),
                dim("b", 4),
                dim("x", 3),
            ]))
            .unwrap();

        assert_eq!(b.len(), 24);
        // Expect 8 repeats of [1,2,3] (2*4 = 8 blocks).
        let mut expected = Vec::with_capacity(2 * 4 * 3);
        for _ in 0..8 {
            expected.extend_from_slice(&[1, 2, 3]);
        }
        assert_eq!(take_all_values(&b), expected);
    }

    #[test]
    fn broadcasting_2d_to_3d_adds_leading_dim() {
        // t=1, x=3 -> station=2, t=1, x=3: adds a new leading dim and repeats over it.
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![5, 6, 7])),
            Dimensions::new(vec![dim("t", 1), dim("x", 3)]),
        )
        .unwrap();

        let b = a
            .broadcast_to(&Dimensions::new(vec![
                dim("station", 2),
                dim("t", 1),
                dim("x", 3),
            ]))
            .unwrap();

        assert_eq!(b.len(), 6);
        assert_eq!(take_all_values(&b), vec![5, 6, 7, 5, 6, 7]);
    }

    #[test]
    fn broadcasting_3d_broadcasts_middle_dim() {
        // To broadcast a "middle" dimension, the input must already have that axis as size-1.
        // a=2, b=1, c=1 -> a=2, b=3, c=1: broadcast the middle dim.
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![10, 20])),
            Dimensions::new(vec![dim("a", 2), dim("b", 1), dim("c", 1)]),
        )
        .unwrap();

        let b = a
            .broadcast_to(&Dimensions::new(vec![
                dim("a", 2),
                dim("b", 3),
                dim("c", 1),
            ]))
            .unwrap();

        assert_eq!(b.len(), 6);
        // Row-major, each of the 2 rows is repeated across the 3 columns.
        assert_eq!(take_all_values(&b), vec![10, 10, 10, 20, 20, 20]);
    }

    #[test]
    fn named_dims_missing_in_target_errors() {
        let dims_in = Dimensions::new(vec![dim("x", 3), dim("y", 2)]);
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from((0..6).collect::<Vec<_>>())),
            dims_in,
        )
        .unwrap();

        // Target missing "y".
        let bad_target = Dimensions::new(vec![dim("x", 3)]);
        let err = a.broadcast_to(&bad_target).unwrap_err();
        assert!(matches!(
            err,
            NdArrayError::BroadcastingError(BroadcastError::IncompatibleShapes(_, _))
        ));
    }

    #[test]
    fn named_dims_can_add_new_trailing_dim_and_broadcast() {
        // Input has only "x"; target adds new trailing "y".
        // This should broadcast by repeating each x value across y.
        let dims_in = Dimensions::new(vec![dim("x", 2)]);
        let a = NdArrowArray::new(Arc::new(Int32Array::from(vec![9, 8])), dims_in).unwrap();

        let target = Dimensions::new(vec![dim("x", 2), dim("y", 3)]);
        let b = a.broadcast_to(&target).unwrap();

        assert_eq!(b.len(), 6);
        assert_eq!(take_all_values(&b), vec![9, 9, 9, 8, 8, 8]);
    }

    #[test]
    fn broadcasting_named_dims_can_add_trailing_or_leading_dim_when_order_preserved() {
        // Named-dim semantics: existing dims are aligned by name and must appear as a subsequence
        // in the target. New dims may be added on either side.
        let dims_in = Dimensions::new(vec![dim("x", 2), dim("y", 3)]);
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from((0..6).collect::<Vec<_>>())),
            dims_in,
        )
        .unwrap();

        // Add trailing dim z.
        let t1 = Dimensions::new(vec![dim("x", 2), dim("y", 3), dim("z", 4)]);
        let b1 = a.broadcast_to(&t1).unwrap();
        assert_eq!(b1.len(), 24);

        // Add leading dim z.
        let t2 = Dimensions::new(vec![dim("z", 4), dim("x", 2), dim("y", 3)]);
        let b2 = a.broadcast_to(&t2).unwrap();
        assert_eq!(b2.len(), 24);

        // Reordering existing dims is still not allowed.
        let bad = Dimensions::new(vec![dim("y", 3), dim("x", 2), dim("z", 4)]);
        let err = a.broadcast_to(&bad).unwrap_err();
        assert!(matches!(
            err,
            NdArrayError::BroadcastingError(BroadcastError::IncompatibleShapes(_, _))
        ));
    }
}
