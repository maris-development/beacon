use std::{fmt::Debug, sync::Arc};

use arrow::array::ArrayRef;
use futures::stream::BoxStream;

// use crate::array::ArrayPart;

mod in_memory;
mod spillable;

// pub use in_memory::InMemoryChunkStore;
// pub use spillable::SpillableChunkStore;

#[async_trait::async_trait]
pub trait ChunkStore: Send + Sync + Debug {
    /// Returns a stream of chunked array parts.
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayRef>>;
    /// Fetches a chunk by its logical chunk index.
    async fn fetch_chunk(&self, start: usize, len: usize) -> anyhow::Result<Option<ArrayRef>>;
}

#[async_trait::async_trait]
impl<T> ChunkStore for Arc<T>
where
    T: ChunkStore + ?Sized,
{
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayRef>> {
        self.as_ref().chunks()
    }

    async fn fetch_chunk(&self, start: usize, len: usize) -> anyhow::Result<Option<ArrayRef>> {
        self.as_ref().fetch_chunk(start, len).await
    }
}
