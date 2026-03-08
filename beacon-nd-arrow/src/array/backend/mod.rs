pub mod mem;

use std::{fmt::Debug, sync::Arc};

use crate::array::{compat_typings::ArrowTypeConversion, subset::ArraySubset};

#[async_trait::async_trait]
pub trait ArrayBackend<T: ArrowTypeConversion>: Send + Sync + 'static + Debug {
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

    fn validate_subset(&self, subset: &ArraySubset) -> anyhow::Result<()> {
        let shape = self.shape();
        if subset.start.len() != shape.len() {
            return Err(anyhow::anyhow!(
                "Subset start rank {} does not match array rank {}",
                subset.start.len(),
                shape.len()
            ));
        }

        if subset.shape.len() != shape.len() {
            return Err(anyhow::anyhow!(
                "Subset shape rank {} does not match array rank {}",
                subset.shape.len(),
                shape.len()
            ));
        }

        for axis in 0..shape.len() {
            let start = subset.start[axis];
            let len = subset.shape[axis];
            let size = shape[axis];

            if start > size {
                return Err(anyhow::anyhow!(
                    "Subset start index {} exceeds axis size {} for axis {}",
                    start,
                    size,
                    axis
                ));
            }

            if start + len > size {
                return Err(anyhow::anyhow!(
                    "Subset end index {} exceeds axis size {} for axis {}",
                    start + len,
                    size,
                    axis
                ));
            }
        }

        Ok(())
    }

    fn fill_value(&self) -> Option<T> {
        None
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>>;
}

#[async_trait::async_trait]
impl<T: ArrowTypeConversion, A: ArrayBackend<T> + ?Sized> ArrayBackend<T> for Arc<A> {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn shape(&self) -> Vec<usize> {
        (**self).shape()
    }

    fn dimensions(&self) -> Vec<String> {
        (**self).dimensions()
    }

    fn fill_value(&self) -> Option<T> {
        (**self).fill_value()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        (**self).read_subset(subset).await
    }
}
