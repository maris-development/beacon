//! An Arrow array with N-dimensional encoding.

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};

use super::broadcast::BroadcastMap;
use super::dimensions::Dimensions;

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

    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    pub fn dims(&self) -> &Dimensions {
        &self.dims
    }

    pub fn data_type(&self) -> &DataType {
        self.values.data_type()
    }

    /// Broadcast map from this array's dimensions onto `target`.
    pub fn broadcast_map(&self, target: &Dimensions) -> Result<BroadcastMap> {
        BroadcastMap::try_new(&self.dims, target)
    }

    /// Materialize this array broadcast onto `target` as a single `take` (or a
    /// zero-copy clone when the broadcast is the identity).
    pub fn materialize(&self, target: &Dimensions) -> Result<ArrayRef> {
        let map = self.broadcast_map(target)?;
        if map.is_identity() {
            return Ok(self.values.clone());
        }
        let indices = map.gather_indices();
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
    use crate::nd::dimensions::Dimension;

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
    fn materialize_broadcast() {
        let lat = nd(vec![10, 20, 30], &[("lat", 3)]);
        let target = dims(&[("time", 2), ("lat", 3)]);
        let out = lat.materialize(&target).unwrap();
        assert_eq!(ints(&out), vec![10, 20, 30, 10, 20, 30]);
    }

    #[test]
    fn materialize_identity_zero_copy() {
        let a = nd((0..6).collect(), &[("time", 2), ("lon", 3)]);
        let target = dims(&[("time", 2), ("lon", 3)]);
        let out = a.materialize(&target).unwrap();
        assert_eq!(ints(&out), (0..6).collect::<Vec<_>>());
    }
}
