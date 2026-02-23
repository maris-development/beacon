pub mod mem;

use std::{fmt::Debug, sync::Arc};

use arrow::array::ArrayRef;

#[allow(clippy::len_without_is_empty)]
#[async_trait::async_trait]
pub trait ArrayBackend: Send + Sync + 'static + Debug {
    fn len(&self) -> usize;
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
