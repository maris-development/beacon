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
pub mod projection;

use std::sync::Arc;

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
    ) -> anyhow::Result<Self> {
        let array = ArrayD::from_shape_vec(dim_sizes.clone(), values.clone()).map_err(|e| {
            anyhow::anyhow!(
                "Failed to create ArrayD from provided values and dimensions: {}",
                e
            )
        })?;
        let backend = InMemoryArrayBackend::new(array, dim_sizes, dim_names, fill_value);
        Ok(Self(Arc::new(backend)))
    }
}

impl<T: NdArrayType> NdArray<T> {
    pub fn new_with_backend<B: ArrayBackend<T>>(backend: B) -> anyhow::Result<Self> {
        let shape = backend.shape();
        let dimensions = backend.dimensions();

        if shape.len() != dimensions.len() {
            return Err(anyhow::anyhow!(
                "Shape length {} does not match dimensions length {}",
                shape.len(),
                dimensions.len()
            ));
        }

        let total_size: usize = shape.iter().product();
        if total_size != backend.len() {
            return Err(anyhow::anyhow!(
                "Total size implied by shape {} does not match backend length {}",
                total_size,
                backend.len()
            ));
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
        let array = self
            .0
            .read_subset(ArraySubset {
                start: vec![0; self.shape().len()],
                shape: self.shape(),
            })
            .await
            .unwrap();

        array.into_raw_vec_and_offset().0
    }

    pub async fn clone_into_raw_vec(&self) -> Vec<T> {
        let array = self
            .0
            .read_subset(ArraySubset {
                start: vec![0; self.shape().len()],
                shape: self.shape(),
            })
            .await
            .unwrap();

        array.into_raw_vec_and_offset().0
    }

    pub async fn subset(&self, subset: ArraySubset) -> anyhow::Result<NdArray<T>> {
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
    ) -> anyhow::Result<NdArray<T>> {
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
            return Err(anyhow::anyhow!(
                "Source dimensions {:?} are not a subset of target dimensions {:?}",
                source_dims,
                dimensions
            ));
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
