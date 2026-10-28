//! An Arrow array with N-dimensional encoding.

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::plan_err;

use crate::dimensions::Dimensions;

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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;

    use super::*;
    use crate::dimensions::Dimension;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn len_mismatch_rejected() {
        let result =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![1, 2, 3])), dims(&[("x", 2)]));
        assert!(result.is_err());
    }
}
