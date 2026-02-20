//! ND (N-dimensional) Arrow arrays for Beacon.
//!
//! This crate provides a small, production-oriented abstraction for representing
//! N-dimensional arrays backed by Arrow arrays.
//!
//! Key features:
//! - Stores ND arrays row-wise in Arrow IPC as a nested column:
//!   `Struct{ values: List<T>, dim_names: List<Dictionary<Int32, Utf8>>, dim_sizes: List<UInt32> }`.
//! - Provides broadcasting support (xarray-style, name-aligned) via
//!   virtual broadcast views with efficient windowed `take`.
//!
//! ## Quick start
//!
//! Create a single ND value:
//!
//! ```
//! use std::sync::Arc;
//! use arrow::array::Int32Array;
//! use beacon_nd_arrow::{NdArrowArray, dimensions::{Dimension, Dimensions}};
//!
//! let nd = NdArrowArray::new(
//!     Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
//!     Dimensions::new(vec![
//!         Dimension::try_new("y", 2)?,
//!         Dimension::try_new("x", 3)?,
//!     ]),
//! )?;
//! assert_eq!(nd.dimensions().shape(), vec![2, 3]);
//! # Ok::<(), beacon_nd_arrow::error::NdArrayError>(())
//! ```
//!
//! Compute common broadcast dimensions for multiple arrays:
//!
//! ```
//! use beacon_nd_arrow::{dimensions::{Dimension, Dimensions}, find_broadcast_dimensions};
//!
//! let dims = vec![
//!   Dimensions::new(vec![
//!     Dimension::try_new("t", 1)?,
//!     Dimension::try_new("x", 3)?,
//!   ]),
//!   Dimensions::new(vec![
//!     Dimension::try_new("t", 2)?,
//!     Dimension::try_new("x", 1)?,
//!   ]),
//! ];
//! let out = find_broadcast_dimensions(&dims)?;
//! assert_eq!(out.shape(), vec![2, 3]);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod array;
pub mod batch;
pub mod broadcast;
pub mod dimensions;
pub mod error;
pub mod extension;
pub mod view;

#[cfg(feature = "ndarray")]
pub mod ndarray_convert;

use crate::{dimensions::Dimensions, error::NdArrayError, view::ArrayBroadcastView};
use std::{ops::Deref, sync::Arc};

use arrow::array::ArrayRef;
use arrow::array::Scalar;
use arrow_schema::DataType;

pub use broadcast::broadcast_nd_array;
pub use broadcast::find_broadcast_dimensions;

/// A single ND array value (e.g. one row).
///
/// To store multiple ND values (rows) in a RecordBatch / Arrow IPC, use
/// `column::NdArrowArrayColumn`.
#[derive(Debug, Clone)]
pub struct NdArrowArray {
    values: ArrayRef,
    dimensions: Dimensions,
}

impl NdArrowArray {
    /// Create a new ND array value from a *flat* storage Arrow array and dimensions.
    ///
    /// This represents a single ND array (e.g. a single row value). To store multiple
    /// ND arrays in an Arrow column (e.g. multiple rows), use `column::NdArrowArrayColumn`.
    pub fn new(values: ArrayRef, dimensions: Dimensions) -> Result<Self, NdArrayError> {
        dimensions.validate()?;

        // Validate the dimensions against the array shape
        if values.len() != dimensions.total_flat_size() {
            return Err(NdArrayError::MisalignedArrayDimensions(
                values.len(),
                dimensions,
            ));
        }

        Ok(Self { values, dimensions })
    }

    pub fn new_scalar(value: Scalar<ArrayRef>) -> Self {
        let array = value.into_inner();
        Self {
            values: array,
            dimensions: Dimensions::Scalar,
        }
    }

    /// Construct a scalar NULL ND array.
    pub fn new_null_scalar(data_type: Option<DataType>) -> Result<Self, NdArrayError> {
        let storage = match data_type {
            Some(dt) => arrow::array::new_null_array(&dt, 1),
            None => Arc::new(arrow::array::NullArray::new(1)),
        };
        Self::new(storage, Dimensions::Scalar)
    }

    /// Returns the flat storage values.
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Returns the dimensions.
    pub fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }

    /// Returns the underlying storage array (the physical Arrow representation).
    pub fn storage_array(&self) -> ArrayRef {
        self.values.clone()
    }

    /// Broadcast this array to `target` as a virtual [`ArrayBroadcastView`].
    ///
    /// The returned view does not materialize the full broadcast result. Use
    /// [`ArrayBroadcastView::take`] to read only the required window.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use beacon_nd_arrow::{NdArrowArray, dimensions::{Dimension, Dimensions}};
    ///
    /// let a = NdArrowArray::new(
    ///     Arc::new(Int32Array::from(vec![1, 2, 3])),
    ///     Dimensions::new(vec![
    ///         Dimension::try_new("y", 1)?,
    ///         Dimension::try_new("x", 3)?,
    ///     ]),
    /// )?;
    ///
    /// let b = a.broadcast_to(&Dimensions::new(vec![
    ///     Dimension::try_new("y", 2)?,
    ///     Dimension::try_new("x", 3)?,
    /// ]))?;
    /// let taken = b.take(0, b.len());
    /// let b_arr = taken.as_any().downcast_ref::<Int32Array>().unwrap();
    /// assert_eq!(b_arr.values(), &[1, 2, 3, 1, 2, 3]);
    /// # Ok::<(), beacon_nd_arrow::error::NdArrayError>(())
    /// ```
    pub fn broadcast_to(&self, target: &Dimensions) -> Result<ArrayBroadcastView, NdArrayError> {
        broadcast::broadcast_nd_array(self, target)
    }
}

impl Deref for NdArrowArray {
    type Target = ArrayRef;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;

    use crate::{
        NdArrowArray,
        dimensions::{Dimension, Dimensions},
        find_broadcast_dimensions,
    };

    fn dim(name: &str, size: usize) -> Dimension {
        Dimension::try_new(name, size).unwrap()
    }

    #[test]
    fn broadcast_to_returns_virtual_view_and_take_reads_window() {
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![dim("x", 3)]),
        )
        .unwrap();

        let view = a
            .broadcast_to(&Dimensions::new(vec![dim("t", 2), dim("x", 3)]))
            .unwrap();

        assert_eq!(view.len(), 6);
        let taken = view.take(2, 3);
        let values = taken.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[3, 1, 2]);
    }

    #[test]
    fn find_broadcast_dimensions_merges_named_shapes() {
        let d1 = Dimensions::new(vec![dim("t", 1), dim("x", 3)]);
        let d2 = Dimensions::new(vec![dim("t", 2), dim("x", 1)]);

        let out = find_broadcast_dimensions(&[d1, d2]).unwrap();
        assert_eq!(out.shape(), vec![2, 3]);
    }
}
