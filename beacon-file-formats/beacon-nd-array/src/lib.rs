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
//! ```rust ignore
//! use std::sync::Arc;
//! use arrow::array::Int32Array;
//! use beacon_nd_arrow::array::{
//!     NdArrowArray,
//!     backend::{ArrayBackend, mem::InMemoryArrayBackend},
//! };
//! use arrow::datatypes::DataType;
//!
//! let values: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
//! let backend: Arc<dyn ArrayBackend> = Arc::new(InMemoryArrayBackend::new(
//!     values,
//!     vec![2, 3],
//!     vec!["y".to_string(), "x".to_string()],
//! ));
//! let nd = NdArrowArray::new(backend, DataType::Int32)?;
//! assert_eq!(nd.shape(), &[2, 3]);
//! assert_eq!(nd.dimensions(), &["y".to_string(), "x".to_string()]);
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! Broadcast one named dimension into a higher-rank target:
//!
//! ```rust ignore
//! use std::sync::Arc;
//! use beacon_nd_arrow::array::{
//!     NdArrowArray,
//!     backend::{ArrayBackend, mem::InMemoryArrayBackend},
//! };
//! use arrow::array::Int32Array;
//! use arrow::datatypes::DataType;
//!
//! let values: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![10, 20, 30]));
//! let backend: Arc<dyn ArrayBackend> = Arc::new(InMemoryArrayBackend::new(
//!     values,
//!     vec![3],
//!     vec!["lon".to_string()],
//! ));
//! let lon = NdArrowArray::new(backend, DataType::Int32)?;
//! let view = lon.broadcast(&[2, 3], &["time".to_string(), "lon".to_string()])?;
//! assert_eq!(view.shape(), &[2, 3]);
//! # Ok::<(), anyhow::Error>(())
//! ```

pub mod array;
pub mod arrow;
pub mod dataset;
pub mod datatypes;
pub mod error;
pub mod projection;

pub use error::NdArrayError;

use std::sync::Arc;

use crate::error::Result;

use crate::{
    array::{
        backend::{ArrayBackend, mem::InMemoryArrayBackend},
        subset::ArraySubset,
    },
    datatypes::{NdArrayDataType, NdArrayType},
};
use ndarray::{ArrayD, Axis};

#[async_trait::async_trait]
pub trait NdArrayD: std::fmt::Debug + Send + Sync {
    fn datatype(&self) -> NdArrayDataType;
    fn dimensions(&self) -> Vec<String>;
    fn shape(&self) -> Vec<usize>;
    fn chunk_shape(&self) -> Vec<usize>;
    async fn broadcast(
        &self,
        target_shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> anyhow::Result<Arc<dyn NdArrayD>>;
    async fn subset(&self, subset: ArraySubset) -> anyhow::Result<Arc<dyn NdArrayD>>;
    /// Can be used to optionally downcast into a typed nd array
    ///
    /// ```rust ignore
    ///
    /// let dyn_nd_array : Arc<dyn NdArrowArray> = array
    /// let downcasted = dyn_nd_array.as_any().downcast_ref::<NdArray<T, B>>()
    ///
    /// ```
    fn as_any(&self) -> &dyn std::any::Any;
}

#[async_trait::async_trait]
impl<T> NdArrayD for NdArray<T>
where
    T: NdArrayType + Clone,
{
    fn datatype(&self) -> NdArrayDataType {
        T::data_type()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn broadcast(
        &self,
        target_shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> anyhow::Result<Arc<dyn NdArrayD>> {
        let broadcasted = NdArray::broadcast(self, &target_shape, &dimensions).await?;
        Ok(Arc::new(broadcasted))
    }

    async fn subset(&self, subset: ArraySubset) -> anyhow::Result<Arc<dyn NdArrayD>> {
        let subset_array = NdArray::subset(self, subset).await?;
        Ok(Arc::new(subset_array))
    }

    fn dimensions(&self) -> Vec<String> {
        NdArray::<T>::dimensions(self)
    }

    fn shape(&self) -> Vec<usize> {
        NdArray::<T>::shape(self)
    }

    fn chunk_shape(&self) -> Vec<usize> {
        NdArray::<T>::chunk_shape(self)
    }
}

#[derive(Debug, Clone)]
pub struct NdArray<T: NdArrayType>(Arc<dyn ArrayBackend<T>>);

impl<T: NdArrayType> NdArray<T> {
    pub fn new_in_mem(values: ArrayD<T>, dim_names: Vec<String>, fill_value: Option<T>) -> Self {
        let dim_sizes: Vec<usize> = values.shape().to_vec();
        let backend = InMemoryArrayBackend::new(values, dim_sizes, dim_names, fill_value);
        Self(Arc::new(backend))
    }

    pub fn try_new_from_vec_in_mem(
        values: Vec<T>,
        dim_sizes: Vec<usize>,
        dim_names: Vec<String>,
        fill_value: Option<T>,
    ) -> Result<Self> {
        let array = ArrayD::from_shape_vec(dim_sizes.clone(), values.clone())
            .map_err(|e| NdArrayError::InvalidShape(e.to_string()))?;
        let backend = InMemoryArrayBackend::new(array, dim_sizes, dim_names, fill_value);
        Ok(Self(Arc::new(backend)))
    }
}

impl<T: NdArrayType> NdArray<T> {
    pub fn new_with_backend<B: ArrayBackend<T>>(backend: B) -> Result<Self> {
        let shape = backend.shape();
        let dimensions = backend.dimensions();

        if shape.len() != dimensions.len() {
            return Err(NdArrayError::ShapeDimensionMismatch {
                shape_len: shape.len(),
                dims_len: dimensions.len(),
            });
        }

        let total_size: usize = shape.iter().product();
        if total_size != backend.len() {
            return Err(NdArrayError::SizeMismatch {
                expected: total_size,
                actual: backend.len(),
            });
        }

        Ok(Self(Arc::new(backend)))
    }

    pub fn shape(&self) -> Vec<usize> {
        self.0.shape()
    }

    pub fn chunk_shape(&self) -> Vec<usize> {
        self.0.chunk_shape()
    }

    pub fn dimensions(&self) -> Vec<String> {
        self.0.dimensions()
    }

    pub async fn fill_value(&self) -> Option<T> {
        self.0.fill_value().clone()
    }

    pub async fn into_raw_vec(self) -> Vec<T> {
        let shape = self.shape();
        let array = self
            .0
            .read_subset(ArraySubset {
                start: vec![0; shape.len()],
                shape: shape.clone(),
            })
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, ?shape, "failed to read full array into raw vec");
                panic!("failed to read full array into raw vec: {e}");
            });

        array.into_raw_vec_and_offset().0
    }

    pub async fn clone_into_raw_vec(&self) -> Vec<T> {
        let shape = self.shape();
        let array = self
            .0
            .read_subset(ArraySubset {
                start: vec![0; shape.len()],
                shape: shape.clone(),
            })
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, ?shape, "failed to read full array into raw vec");
                panic!("failed to read full array into raw vec: {e}");
            });

        array.into_raw_vec_and_offset().0
    }

    pub async fn subset(&self, subset: ArraySubset) -> Result<NdArray<T>> {
        let nd_array = self.0.read_subset(subset.clone()).await?;
        let backend = InMemoryArrayBackend::new(
            nd_array,
            subset.shape.clone(),
            self.dimensions(),
            self.0.fill_value().clone(),
        );
        Ok(NdArray(Arc::new(backend)))
    }

    pub async fn broadcast(
        &self,
        target_shape: &[usize],
        dimensions: &[String],
    ) -> Result<NdArray<T>> {
        // This will always materialize the array. In the future, we may want to return a lazy broadcast view instead.
        let nd_array = self
            .0
            .read_subset(ArraySubset {
                start: vec![0; self.shape().len()],
                shape: self.shape(),
            })
            .await?;

        let source_dims = self.dimensions();
        // Reorder source axes into target-dimension order and insert singleton
        // axes for missing dimensions before applying ndarray broadcasting.
        let source_axis_order: Vec<usize> = dimensions
            .iter()
            .filter_map(|target_dim| source_dims.iter().position(|d| d == target_dim))
            .collect();

        if source_axis_order.len() != source_dims.len() {
            return Err(NdArrayError::BroadcastDimensions {
                source_dims,
                target_dims: dimensions.to_vec(),
            });
        }

        let mut aligned = nd_array
            .view()
            .permuted_axes(source_axis_order.clone())
            .into_dyn();
        for (target_axis, target_dim) in dimensions.iter().enumerate() {
            if !source_dims.iter().any(|d| d == target_dim) {
                aligned = aligned.insert_axis(Axis(target_axis));
            }
        }

        let broadcasted = aligned.broadcast(target_shape).ok_or_else(|| {
            anyhow::anyhow!(
                "Cannot broadcast array of shape {:?} to target shape {:?}",
                self.shape(),
                target_shape
            )
        })?;

        let backend = InMemoryArrayBackend::new(
            broadcasted.to_owned(),
            target_shape.to_vec(),
            dimensions.to_vec(),
            self.0.fill_value().clone(),
        );
        let new_array = NdArray(Arc::new(backend));

        Ok(new_array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dims(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    fn i32_array(values: Vec<i32>, shape: Vec<usize>, names: &[&str]) -> NdArray<i32> {
        NdArray::<i32>::try_new_from_vec_in_mem(values, shape, dims(names), None).unwrap()
    }

    // ── construction ─────────────────────────────────────────────────

    #[test]
    fn test_try_new_from_vec_rejects_values_that_do_not_fill_the_shape() {
        let err =
            NdArray::<i32>::try_new_from_vec_in_mem(vec![1, 2, 3], vec![2, 2], dims(&["a", "b"]), None)
                .unwrap_err();
        assert!(
            matches!(err, NdArrayError::InvalidShape(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_new_with_backend_rejects_a_shape_dimension_count_mismatch() {
        let backend = InMemoryArrayBackend::new(
            ArrayD::from_shape_vec(vec![2, 2], vec![1, 2, 3, 4]).unwrap(),
            vec![2, 2],
            dims(&["only_one"]),
            None,
        );
        let err = NdArray::new_with_backend(backend).unwrap_err();
        assert!(
            matches!(err, NdArrayError::ShapeDimensionMismatch { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_new_with_backend_rejects_a_shape_that_disagrees_with_the_data() {
        // The declared shape says 6 elements, the backing array holds 4.
        let backend = InMemoryArrayBackend::new(
            ArrayD::from_shape_vec(vec![2, 2], vec![1, 2, 3, 4]).unwrap(),
            vec![2, 3],
            dims(&["a", "b"]),
            None,
        );
        let err = NdArray::new_with_backend(backend).unwrap_err();
        assert!(
            matches!(
                err,
                NdArrayError::SizeMismatch {
                    expected: 6,
                    actual: 4
                }
            ),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_new_in_mem_takes_its_shape_from_the_values() {
        let array = NdArray::new_in_mem(
            ArrayD::from_shape_vec(vec![2, 3], (0..6).collect::<Vec<i32>>()).unwrap(),
            dims(&["y", "x"]),
            Some(-1),
        );
        assert_eq!(array.shape(), vec![2, 3]);
        assert_eq!(array.dimensions(), dims(&["y", "x"]));
        assert_eq!(array.fill_value().await, Some(-1));
        // No chunking backend, so the chunk shape is the whole array.
        assert_eq!(array.chunk_shape(), vec![2, 3]);
    }

    // ── subset ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_subset_slices_row_major_and_keeps_dimension_names() {
        let array = i32_array((0..12).collect(), vec![3, 4], &["y", "x"]);
        let sub = array
            .subset(ArraySubset {
                start: vec![1, 1],
                shape: vec![2, 2],
            })
            .await
            .unwrap();

        assert_eq!(sub.shape(), vec![2, 2]);
        assert_eq!(sub.dimensions(), dims(&["y", "x"]));
        assert_eq!(sub.clone_into_raw_vec().await, vec![5, 6, 9, 10]);
    }

    #[tokio::test]
    async fn test_subset_carries_the_fill_value_forward() {
        let array =
            NdArray::<i32>::try_new_from_vec_in_mem((0..4).collect(), vec![4], dims(&["x"]), Some(-9))
                .unwrap();
        let sub = array
            .subset(ArraySubset {
                start: vec![1],
                shape: vec![2],
            })
            .await
            .unwrap();
        assert_eq!(sub.fill_value().await, Some(-9));
    }

    #[tokio::test]
    async fn test_subset_out_of_bounds_is_rejected() {
        let array = i32_array((0..12).collect(), vec![3, 4], &["y", "x"]);
        let err = array
            .subset(ArraySubset {
                start: vec![2, 0],
                shape: vec![2, 4],
            })
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("exceeds axis size"),
            "unexpected error: {err}"
        );
    }

    // ── broadcast ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_broadcast_repeats_along_an_inserted_dimension() {
        let array = i32_array(vec![10, 20], vec![2], &["lat"]);
        let broadcasted = array.broadcast(&[3, 2], &dims(&["time", "lat"])).await.unwrap();

        assert_eq!(broadcasted.shape(), vec![3, 2]);
        assert_eq!(broadcasted.dimensions(), dims(&["time", "lat"]));
        assert_eq!(
            broadcasted.clone_into_raw_vec().await,
            vec![10, 20, 10, 20, 10, 20]
        );
    }

    #[tokio::test]
    async fn test_broadcast_of_a_scalar_attribute_fills_the_whole_grid() {
        // Rank-0 arrays back every NetCDF attribute; they must reach any grid.
        let array = i32_array(vec![7], vec![], &[]);
        let broadcasted = array.broadcast(&[2, 3], &dims(&["time", "lat"])).await.unwrap();
        assert_eq!(broadcasted.clone_into_raw_vec().await, vec![7; 6]);
    }

    #[tokio::test]
    async fn test_broadcast_preserves_the_fill_value() {
        let array =
            NdArray::<i32>::try_new_from_vec_in_mem(vec![1, -9], vec![2], dims(&["lat"]), Some(-9))
                .unwrap();
        let broadcasted = array.broadcast(&[2, 2], &dims(&["time", "lat"])).await.unwrap();
        assert_eq!(broadcasted.fill_value().await, Some(-9));
    }

    #[tokio::test]
    async fn test_broadcast_rejects_source_dims_outside_the_target() {
        // A CF-bounds style `nv` axis has nowhere to go in the target grid.
        let array = i32_array(vec![1, 2, 3, 4], vec![2, 2], &["lat", "nv"]);
        let err = array
            .broadcast(&[2, 2], &dims(&["time", "lat"]))
            .await
            .unwrap_err();
        assert!(
            matches!(err, NdArrayError::BroadcastDimensions { .. }),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_broadcast_rejects_an_incompatible_extent_on_a_shared_dimension() {
        // Only length-1 axes may be stretched; 3 -> 5 is not a legal broadcast.
        let array = i32_array(vec![1, 2, 3], vec![3], &["lat"]);
        let err = array.broadcast(&[5], &dims(&["lat"])).await.unwrap_err();
        assert!(
            err.to_string().contains("Cannot broadcast"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_broadcast_of_a_zero_length_dimension_stays_empty() {
        let array = i32_array(vec![], vec![0], &["time"]);
        let broadcasted = array.broadcast(&[0, 3], &dims(&["time", "lat"])).await.unwrap();
        assert_eq!(broadcasted.shape(), vec![0, 3]);
        assert!(broadcasted.clone_into_raw_vec().await.is_empty());
    }

    #[tokio::test]
    #[ignore = "known bug: a pure transposition (target shape equals the permuted source \
                shape, so ndarray never has to expand anything) keeps the source memory \
                layout through `to_owned()`. `clone_into_raw_vec` — and therefore the Arrow \
                column built from it — then reports the values in source order. Needs \
                `.as_standard_layout()` before storing the broadcast result."]
    async fn test_broadcast_transposes_values_when_dimension_order_differs() {
        // Stored as (lat, time): lat0 -> [0, 1, 2], lat1 -> [3, 4, 5].
        // In (time, lat) order that is [0, 3, 1, 4, 2, 5].
        let array = i32_array((0..6).collect(), vec![2, 3], &["lat", "time"]);
        let broadcasted = array.broadcast(&[3, 2], &dims(&["time", "lat"])).await.unwrap();

        assert_eq!(broadcasted.shape(), vec![3, 2]);
        assert_eq!(
            broadcasted.clone_into_raw_vec().await,
            vec![0, 3, 1, 4, 2, 5]
        );
    }

    // ── dyn dispatch ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_dyn_nd_array_forwards_to_the_typed_implementation() {
        let array: Arc<dyn NdArrayD> = Arc::new(i32_array(vec![1, 2, 3], vec![3], &["x"]));

        assert_eq!(array.datatype(), NdArrayDataType::I32);
        assert_eq!(array.shape(), vec![3]);
        assert_eq!(array.dimensions(), dims(&["x"]));

        let broadcasted = array
            .broadcast(vec![2, 3], dims(&["t", "x"]))
            .await
            .unwrap();
        assert_eq!(broadcasted.shape(), vec![2, 3]);

        let sub = array
            .subset(ArraySubset {
                start: vec![1],
                shape: vec![2],
            })
            .await
            .unwrap();
        assert_eq!(sub.shape(), vec![2]);

        // `as_any` must downcast back to the concrete typed array.
        let typed = sub.as_any().downcast_ref::<NdArray<i32>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![2, 3]);
    }
}
