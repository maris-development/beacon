use std::{fmt::Debug, sync::Arc};

use futures::stream::BoxStream;

use crate::array::ArrayPart;

mod in_memory;
mod spillable;

pub use in_memory::InMemoryChunkStore;
pub use spillable::SpillableChunkStore;

#[async_trait::async_trait]
pub trait ChunkStore: Send + Sync + Debug {
    /// Returns a stream of chunked array parts.
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayPart>>;
    /// Fetches a chunk by its logical chunk index.
    async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>>;
}

#[async_trait::async_trait]
impl<T> ChunkStore for Arc<T>
where
    T: ChunkStore + ?Sized,
{
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayPart>> {
        self.as_ref().chunks()
    }

    async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>> {
        self.as_ref().fetch_chunk(chunk_index).await
    }
}
