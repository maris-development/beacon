use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::ArrayRef;

use crate::array::compat_typings::ArrowTypeConversion;
use crate::array::{
    backend::{ArrayBackend, mem::InMemoryArrayBackend},
    subset::ArraySubset,
};

pub mod backend;
pub mod compat_typings;
pub mod subset;

#[async_trait::async_trait]
pub trait NdArrowArray {
    async fn subset(&self, subset: ArraySubset) -> anyhow::Result<Arc<dyn NdArrowArray>>;
    async fn as_arrow_array_ref(&self) -> anyhow::Result<ArrayRef>;
    async fn broadcast(
        &self,
        target_shape: &[usize],
        dimensions: &[String],
    ) -> anyhow::Result<Arc<dyn NdArrowArray>>;
    fn shape(&self) -> Vec<usize>;
    fn dimensions(&self) -> Vec<String>;
    fn data_type(&self) -> arrow::datatypes::DataType;
}

/// N-dimensional Arrow-backed array with named dimensions and virtual stride support.
///
/// The underlying storage is a 1D backend, while `shape` + `strides` define the logical
/// N-dimensional view. This enables efficient virtual transformations like broadcasting
/// without immediately materializing data.
///
/// Key capabilities:
/// - Named-dimension broadcasting (xarray-style alignment constraints)
/// - Stride-aware slicing into new materialized arrays
/// - Lazy chunked reads that respect the current logical view
/// - Full materialization to `ArrayRef` when needed
#[derive(Debug, Clone)]
pub struct NdArrowArrayDispatch<
    T: ArrowTypeConversion,
    B: ArrayBackend<T> = InMemoryArrayBackend<T>,
> {
    marker: std::marker::PhantomData<T>,
    pub backend: Arc<B>,
}

impl<T: ArrowTypeConversion> NdArrowArrayDispatch<T> {
    pub fn new_in_mem(
        array: Vec<T>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> anyhow::Result<Self> {
        let nd_array = ndarray::ArrayD::from_shape_vec(shape.clone(), array)?;
        let backend = InMemoryArrayBackend::new(nd_array, shape.clone(), dimensions.clone());
        Self::new(backend)
    }
}

impl<T: ArrowTypeConversion, B: ArrayBackend<T>> NdArrowArrayDispatch<T, B> {
    /// Create a new contiguous row-major ND array view over a backend.
    ///
    /// # Validation
    /// - `shape.len()` must equal `dimensions.len()`
    /// - `product(shape)` must match backend length
    ///
    /// # Errors
    /// Returns an error if validation fails.
    pub fn new(backend: B) -> anyhow::Result<Self> {
        let shape = backend.shape();
        let dimensions = backend.dimensions();
        if shape.len() != dimensions.len() {
            return Err(anyhow::anyhow!(
                "Shape length {} does not match dimensions length {}",
                shape.len(),
                dimensions.len()
            ));
        }

        // Validate that the total size implied by the shape matches the backend length
        let total_size: usize = shape.iter().product();
        if total_size != backend.len() {
            return Err(anyhow::anyhow!(
                "Total size implied by shape {} does not match backend length {}",
                total_size,
                backend.len()
            ));
        }

        Self::from_parts(Arc::new(backend), shape, dimensions)
    }

    fn from_parts(
        backend: Arc<B>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> anyhow::Result<Self> {
        if shape.len() != dimensions.len() {
            return Err(anyhow::anyhow!(
                "Shape length {} does not match dimensions length {}",
                shape.len(),
                dimensions.len()
            ));
        }

        Ok(Self {
            backend,
            marker: std::marker::PhantomData,
        })
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.shape().iter().product()
    }

    pub fn shape(&self) -> Vec<usize> {
        self.backend.shape()
    }

    pub fn chunk_shape(&self) -> Vec<usize> {
        self.backend.chunk_shape()
    }

    pub fn dimensions(&self) -> Vec<String> {
        self.backend.dimensions()
    }

    fn validate_unique_dimension_names(dimensions: &[String]) -> anyhow::Result<()> {
        let mut seen = HashSet::new();
        for dim in dimensions {
            if !seen.insert(dim.as_str()) {
                return Err(anyhow::anyhow!("Duplicate dimension name '{}'", dim));
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: ArrowTypeConversion, B: ArrayBackend<T>> NdArrowArray for NdArrowArrayDispatch<T, B> {
    async fn subset(&self, subset: ArraySubset) -> anyhow::Result<Arc<dyn NdArrowArray>> {
        let subset_array = self.backend.read_subset(subset).await?;
        let subset_dispatch = NdArrowArrayDispatch::new_in_mem(
            subset_array.into_raw_vec(),
            self.shape(),
            self.dimensions(),
        )?;
        Ok(Arc::new(subset_dispatch))
    }
    async fn as_arrow_array_ref(&self) -> anyhow::Result<ArrayRef> {
        let subset_array = self
            .backend
            .read_subset(ArraySubset {
                start: vec![0; self.shape().len()],
                shape: self.shape(),
            })
            .await?;

        let slice = subset_array.to_owned().into_raw_vec();

        match self.backend.fill_value() {
            Some(fill) => T::arrow_from_array_view_with_fill(&slice, &fill),
            None => T::arrow_from_array_view(&slice),
        }
    }

    async fn broadcast(
        &self,
        target_shape: &[usize],
        dimensions: &[String],
    ) -> anyhow::Result<Arc<dyn NdArrowArray>> {
        if target_shape.len() != dimensions.len() {
            return Err(anyhow::anyhow!(
                "Target shape length {} does not match dimensions length {}",
                target_shape.len(),
                dimensions.len()
            ));
        }

        let nd_array = self
            .backend
            .read_subset(ArraySubset {
                start: vec![0; self.shape().len()],
                shape: self.shape(),
            })
            .await?;

        let broadcasted = nd_array.broadcast(target_shape).ok_or_else(|| {
            anyhow::anyhow!(
                "Cannot broadcast array of shape {:?} to target shape {:?}",
                self.shape(),
                target_shape
            )
        })?;

        let broadcasted_vec = broadcasted.to_owned().into_raw_vec();
        let broadcasted_dispatch = NdArrowArrayDispatch::new_in_mem(
            broadcasted_vec,
            target_shape.to_vec(),
            dimensions.to_vec(),
        )?;
        Ok(Arc::new(broadcasted_dispatch))
    }
    fn shape(&self) -> Vec<usize> {
        self.backend.shape()
    }
    fn dimensions(&self) -> Vec<String> {
        self.backend.dimensions()
    }

    fn data_type(&self) -> arrow::datatypes::DataType {
        T::data_type()
    }
}
