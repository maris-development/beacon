use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::compute::{concat, take};

use crate::{
    NdArrowArray,
    dimensions::{Dimension, Dimensions},
    error::NdArrayError,
    slice::row_major_strides,
};

/// Concatenate ND arrays along the specified axis.
///
/// All arrays must have matching dimension names and sizes, except for the
/// concatenation axis, which is summed.
pub fn concat_nd(arrays: &[NdArrowArray], axis: usize) -> Result<NdArrowArray, NdArrayError> {
    if arrays.is_empty() {
        return Err(NdArrayError::InvalidNdBatch(
            "cannot concatenate empty array list".to_string(),
        ));
    }

    let first_dims = arrays[0]
        .dimensions()
        .as_multi_dimensional()
        .cloned()
        .unwrap_or_default();
    if first_dims.is_empty() {
        return Err(NdArrayError::InvalidDimensions(
            "cannot concatenate scalar arrays".to_string(),
        ));
    }
    if axis >= first_dims.len() {
        return Err(NdArrayError::InvalidSlice(format!(
            "axis {} out of bounds for {} dims",
            axis,
            first_dims.len()
        )));
    }

    let data_type = arrays[0].values().data_type().clone();
    let mut axis_total = 0usize;
    let mut concat_arrays: Vec<ArrayRef> = Vec::with_capacity(arrays.len());
    let mut array_shapes: Vec<Vec<usize>> = Vec::with_capacity(arrays.len());
    let mut array_strides: Vec<Vec<usize>> = Vec::with_capacity(arrays.len());
    let mut base_offsets: Vec<usize> = Vec::with_capacity(arrays.len());
    let mut axis_offsets: Vec<usize> = Vec::with_capacity(arrays.len() + 1);

    axis_offsets.push(0);
    let mut running_base = 0usize;

    for array in arrays {
        let dims = array
            .dimensions()
            .as_multi_dimensional()
            .cloned()
            .unwrap_or_default();
        if dims.len() != first_dims.len() {
            return Err(NdArrayError::InvalidDimensions(
                "mismatched dimension rank".to_string(),
            ));
        }
        for (i, (a, b)) in dims.iter().zip(first_dims.iter()).enumerate() {
            if a.name() != b.name() {
                return Err(NdArrayError::InvalidDimensions(
                    "dimension names do not match".to_string(),
                ));
            }
            if i != axis && a.size() != b.size() {
                return Err(NdArrayError::InvalidDimensions(
                    "dimension sizes do not match".to_string(),
                ));
            }
        }
        if array.values().data_type() != &data_type {
            return Err(NdArrayError::InvalidNdBatch(
                "array data types do not match".to_string(),
            ));
        }

        let axis_size = dims[axis].size();
        axis_total = axis_total.saturating_add(axis_size);
        axis_offsets.push(axis_total);

        let shape = array.dimensions().shape();
        let strides = row_major_strides(&shape);
        array_shapes.push(shape);
        array_strides.push(strides);
        base_offsets.push(running_base);
        running_base = running_base
            .checked_add(array.values().len())
            .ok_or_else(|| NdArrayError::InvalidNdBatch("values length overflow".to_string()))?;

        concat_arrays.push(array.values().clone());
    }

    let concat_refs: Vec<&dyn Array> = concat_arrays.iter().map(|a| a.as_ref()).collect();
    let concatenated = concat(&concat_refs)?;

    let mut out_dims = first_dims.clone();
    out_dims[axis] = Dimension::new_unchecked(out_dims[axis].name().to_string(), axis_total);
    let out_dimensions = Dimensions::new(out_dims);
    let out_shape = out_dimensions.shape();
    let out_strides = row_major_strides(&out_shape);
    let out_len = if out_shape.is_empty() {
        1
    } else {
        out_shape.iter().product()
    };

    let mut indices: Vec<u64> = Vec::with_capacity(out_len);
    for out_flat in 0..out_len {
        let mut remaining = out_flat;
        let mut coords = vec![0usize; out_shape.len()];
        for (i, dim) in out_shape.iter().enumerate() {
            let stride = out_strides[i];
            coords[i] = if stride == 0 { 0 } else { remaining / stride };
            if *dim != 0 {
                remaining %= stride.max(1);
            }
        }

        let axis_coord = coords[axis];
        let mut array_idx = 0usize;
        while axis_offsets[array_idx + 1] <= axis_coord {
            array_idx += 1;
        }
        let local_axis = axis_coord - axis_offsets[array_idx];

        let mut local_flat = 0usize;
        for (i, stride) in array_strides[array_idx].iter().enumerate() {
            let coord = if i == axis { local_axis } else { coords[i] };
            local_flat = local_flat
                .checked_add(coord.saturating_mul(*stride))
                .ok_or_else(|| NdArrayError::InvalidNdBatch("local index overflow".to_string()))?;
        }
        if local_flat >= array_shapes[array_idx].iter().product::<usize>() {
            return Err(NdArrayError::InvalidNdBatch(
                "local index out of bounds".to_string(),
            ));
        }
        indices.push((base_offsets[array_idx] + local_flat) as u64);
    }

    let index_array = UInt64Array::from(indices);
    let taken = take(&concatenated, &index_array, None)?;
    NdArrowArray::new(Arc::new(taken), out_dimensions)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;

    use crate::{
        NdArrowArray,
        concat::concat_nd,
        dimensions::{Dimension, Dimensions},
    };

    fn make_array(values: Vec<i32>, shape: Vec<usize>, names: Vec<&str>) -> NdArrowArray {
        let dims = names
            .into_iter()
            .zip(shape.into_iter())
            .map(|(name, size)| Dimension::try_new(name, size).unwrap())
            .collect::<Vec<_>>();
        NdArrowArray::new(Arc::new(Int32Array::from(values)), Dimensions::new(dims)).unwrap()
    }

    #[test]
    fn concat_axis0() {
        let a = make_array(vec![1, 2], vec![1, 2], vec!["y", "x"]);
        let b = make_array(vec![3, 4], vec![1, 2], vec!["y", "x"]);
        let combined = concat_nd(&[a, b], 0).unwrap();
        let values = combined
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[1, 2, 3, 4]);
        assert_eq!(combined.dimensions().shape(), vec![2, 2]);
    }

    #[test]
    fn concat_axis1() {
        let a = make_array(vec![1, 2], vec![2, 1], vec!["y", "x"]);
        let b = make_array(vec![3, 4], vec![2, 1], vec!["y", "x"]);
        let combined = concat_nd(&[a, b], 1).unwrap();
        let values = combined
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[1, 3, 2, 4]);
        assert_eq!(combined.dimensions().shape(), vec![2, 2]);
    }
}
