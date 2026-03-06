pub mod mem;
#[cfg(feature = "ndarray")]
// Re-export the `nd_array` module when the "ndarray" feature is enabled.
pub mod ndarray;

use std::{fmt::Debug, sync::Arc};

use arrow::array::ArrayRef;

use crate::array::subset::ArraySubset;

#[async_trait::async_trait]
pub trait ArrayBackend: Send + Sync + 'static + Debug {
    fn len(&self) -> usize;

    /// Returns `true` when the array contains no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn shape(&self) -> Vec<usize>;
    /// Returns the shape of the array for chunking purposes. By default, this is the same as `shape()`, but it can be overridden to provide a different shape for chunking.
    fn chunk_shape(&self) -> Vec<usize> {
        self.shape()
    }
    fn dimensions(&self) -> Vec<String>;
    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef>;
    async fn subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayRef> {
        anyhow::bail!("Subset operation not implemented for this backend");
    }
}

#[async_trait::async_trait]
impl<T: ArrayBackend + ?Sized> ArrayBackend for Arc<T> {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn shape(&self) -> Vec<usize> {
        (**self).shape()
    }

    fn dimensions(&self) -> Vec<String> {
        (**self).dimensions()
    }

    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef> {
        (**self).slice(start, length).await
    }
}
