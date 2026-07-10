//! An Arrow array with N-dimensional encoding.

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};

use super::broadcast::BroadcastMap;
use super::dimensions::{Dimension, Dimensions};

/// A hyperslab of an N-dimensional array: per-axis start offset and extent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArraySubset {
    pub start: Vec<usize>,
    pub shape: Vec<usize>,
}

impl ArraySubset {
    pub fn new(start: Vec<usize>, shape: Vec<usize>) -> Self {
        Self { start, shape }
    }
}

/// A flat Arrow array plus the named C-order dimensions describing its
/// logical N-dimensional shape.
///
/// Invariant: `dims.num_elements() == values.len()`. Validity travels in the
/// Arrow array's null buffer; there is no separate mask or fill value —
/// readers are expected to map fill values to nulls when they build the
/// Arrow array.
#[derive(Debug, Clone)]
pub struct NdArrowArray {
    values: ArrayRef,
    dims: Dimensions,
}

impl NdArrowArray {
    pub fn try_new(values: ArrayRef, dims: Dimensions) -> Result<Self> {
        if dims.num_elements() != values.len() {
            return plan_err!(
                "nd array values length {} does not match dimensions {dims} ({} elements)",
                values.len(),
                dims.num_elements()
            );
        }
        Ok(Self { values, dims })
    }

    /// A rank-0 array wrapping a single-element Arrow array.
    pub fn try_new_scalar(values: ArrayRef) -> Result<Self> {
        Self::try_new(values, Dimensions::scalar())
    }

    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    pub fn dims(&self) -> &Dimensions {
        &self.dims
    }

    pub fn shape(&self) -> Vec<usize> {
        self.dims.shape()
    }

    pub fn data_type(&self) -> &DataType {
        self.values.data_type()
    }

    pub fn num_elements(&self) -> usize {
        self.values.len()
    }

    /// Extract a hyperslab. Zero-copy (`Array::slice`) when the subset is
    /// contiguous in C-order; a single `take` gather otherwise.
    pub fn subset(&self, subset: &ArraySubset) -> Result<NdArrowArray> {
        let rank = self.dims.rank();
        if subset.start.len() != rank || subset.shape.len() != rank {
            return plan_err!(
                "subset rank {}/{} does not match array rank {rank}",
                subset.start.len(),
                subset.shape.len()
            );
        }
        for axis in 0..rank {
            let size = self.dims.get(axis).size();
            if subset.start[axis] + subset.shape[axis] > size {
                return plan_err!(
                    "subset [{}..{}) exceeds axis '{}' of size {size}",
                    subset.start[axis],
                    subset.start[axis] + subset.shape[axis],
                    self.dims.get(axis).name()
                );
            }
        }

        let new_dims = Dimensions::try_new(
            self.dims
                .iter()
                .zip(subset.shape.iter())
                .map(|(dim, &size)| Dimension::new(dim.name_arc(), size))
                .collect(),
        )?;

        if new_dims.num_elements() == self.num_elements() {
            return Ok(Self {
                values: self.values.clone(),
                dims: new_dims,
            });
        }

        if let Some((offset, len)) = self.contiguous_range(subset) {
            return Ok(Self {
                values: self.values.slice(offset, len),
                dims: new_dims,
            });
        }

        // Orthogonal gather: kept coordinates per axis are contiguous ranges.
        let identity = BroadcastMap::try_new(&self.dims, &self.dims)?;
        let keep_owned: Vec<Vec<u32>> = (0..rank)
            .map(|axis| {
                (subset.start[axis]..subset.start[axis] + subset.shape[axis])
                    .map(|c| c as u32)
                    .collect()
            })
            .collect();
        let keep: Vec<Option<&[u32]>> = keep_owned.iter().map(|k| Some(k.as_slice())).collect();
        let indices = identity.gather_indices(Some(&keep));
        let values = arrow::compute::take(self.values.as_ref(), &indices, None)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Self::try_new(values, new_dims)
    }

    /// If the hyperslab is one contiguous run of the flat buffer, return its
    /// (offset, length). Writing `p` for the last axis whose extent is not
    /// full, the run is contiguous iff every axis before `p` has extent 1
    /// (each contributes a single coordinate); axis `p` itself may take any
    /// contiguous extent and everything after it is full by construction.
    fn contiguous_range(&self, subset: &ArraySubset) -> Option<(usize, usize)> {
        let rank = self.dims.rank();
        let strides = self.dims.c_strides();
        let p = (0..rank)
            .rev()
            .find(|&axis| subset.shape[axis] != self.dims.get(axis).size())
            .unwrap_or(0);
        if (0..p).any(|axis| subset.shape[axis] != 1) {
            return None;
        }

        let offset: usize = subset
            .start
            .iter()
            .zip(strides.iter())
            .map(|(&s, &stride)| s * stride)
            .sum();
        let len: usize = subset.shape.iter().product();
        Some((offset, len))
    }

    /// Broadcast map from this array's dimensions onto `target`.
    pub fn broadcast_map(&self, target: &Dimensions) -> Result<BroadcastMap> {
        BroadcastMap::try_new(&self.dims, target)
    }

    /// Materialize this array broadcast onto `target`, optionally restricted
    /// to kept coordinates per target axis. Filter and broadcast compose into
    /// a single `take`; the unfiltered expansion is never built.
    pub fn materialize(
        &self,
        target: &Dimensions,
        keep: Option<&[Option<&[u32]>]>,
    ) -> Result<ArrayRef> {
        let map = self.broadcast_map(target)?;
        if map.is_identity() && keep.map_or(true, |k| k.iter().all(|axis| axis.is_none())) {
            return Ok(self.values.clone());
        }
        let indices = map.gather_indices(keep);
        arrow::compute::take(self.values.as_ref(), &indices, None)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::Int32Type;

    use super::*;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    fn nd(values: Vec<i32>, spec: &[(&str, usize)]) -> NdArrowArray {
        NdArrowArray::try_new(Arc::new(Int32Array::from(values)), dims(spec)).unwrap()
    }

    fn ints(array: &ArrayRef) -> Vec<i32> {
        array.as_primitive::<Int32Type>().values().to_vec()
    }

    #[test]
    fn len_mismatch_rejected() {
        let result = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            dims(&[("x", 2)]),
        );
        assert!(result.is_err());
    }

    #[test]
    fn contiguous_subset_is_slice() {
        // (time=4, lon=3): rows 1..3 with all lon → contiguous.
        let a = nd((0..12).collect(), &[("time", 4), ("lon", 3)]);
        let s = a
            .subset(&ArraySubset::new(vec![1, 0], vec![2, 3]))
            .unwrap();
        assert_eq!(s.shape(), vec![2, 3]);
        assert_eq!(ints(s.values()), vec![3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn strided_subset_gathers() {
        // (time=2, lon=3): lon 1..3 for all time → non-contiguous.
        let a = nd((0..6).collect(), &[("time", 2), ("lon", 3)]);
        let s = a
            .subset(&ArraySubset::new(vec![0, 1], vec![2, 2]))
            .unwrap();
        assert_eq!(s.shape(), vec![2, 2]);
        assert_eq!(ints(s.values()), vec![1, 2, 4, 5]);
    }

    #[test]
    fn full_subset_is_zero_copy() {
        let a = nd((0..6).collect(), &[("time", 2), ("lon", 3)]);
        let s = a
            .subset(&ArraySubset::new(vec![0, 0], vec![2, 3]))
            .unwrap();
        assert_eq!(ints(s.values()), ints(a.values()));
    }

    #[test]
    fn out_of_bounds_subset_rejected() {
        let a = nd((0..6).collect(), &[("time", 2), ("lon", 3)]);
        assert!(a.subset(&ArraySubset::new(vec![1, 0], vec![2, 3])).is_err());
    }

    #[test]
    fn materialize_broadcast() {
        let lat = nd(vec![10, 20, 30], &[("lat", 3)]);
        let target = dims(&[("time", 2), ("lat", 3)]);
        let out = lat.materialize(&target, None).unwrap();
        assert_eq!(ints(&out), vec![10, 20, 30, 10, 20, 30]);
    }

    #[test]
    fn materialize_with_selection_composes() {
        let lat = nd(vec![10, 20, 30], &[("lat", 3)]);
        let target = dims(&[("time", 2), ("lat", 3)]);
        let keep: Vec<Option<&[u32]>> = vec![Some(&[1]), Some(&[0, 2])];
        let out = lat.materialize(&target, Some(&keep)).unwrap();
        assert_eq!(ints(&out), vec![10, 30]);
    }

    #[test]
    fn materialize_identity_zero_copy() {
        let a = nd((0..6).collect(), &[("time", 2), ("lon", 3)]);
        let target = dims(&[("time", 2), ("lon", 3)]);
        let out = a.materialize(&target, None).unwrap();
        assert_eq!(ints(&out), (0..6).collect::<Vec<_>>());
    }
}
