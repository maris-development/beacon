pub mod mem;
#[cfg(feature = "ndarray")]
// Re-export the `nd_array` module when the "ndarray" feature is enabled.
pub mod ndarray;

use std::{fmt::Debug, sync::Arc};

use arrow::array::ArrayRef;

#[async_trait::async_trait]
pub trait ArrayBackend: Send + Sync + 'static + Debug {
    fn len(&self) -> usize;

    /// Returns `true` when the array contains no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn shape(&self) -> Vec<usize>;
    fn dimensions(&self) -> Vec<String>;
    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef>;
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
