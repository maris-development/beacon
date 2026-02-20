pub mod mem;

use arrow::array::ArrayRef;

#[allow(clippy::len_without_is_empty)]
#[async_trait::async_trait]
pub trait ArrayBackend: Send + Sync + 'static {
    fn len(&self) -> usize;
    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef>;
}
