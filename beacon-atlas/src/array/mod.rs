use std::sync::Arc;

use futures::StreamExt;

use crate::array::{nd::NdArray, store::ChunkStore};

pub mod buffer;
pub mod data_type;
pub mod io_cache;
pub mod layout;
pub mod nd;
pub mod pruning;
pub mod reader;
pub mod store;
pub mod writer;

/// A stream of chunked ND arrays with a shared element type and chunk shape.
#[derive(Debug, Clone)]
pub struct Array<S: ChunkStore + Send + Sync> {
    pub array_datatype: arrow::datatypes::DataType,
    pub array_shape: Vec<usize>,
    pub dimensions: Vec<String>,
    pub chunk_provider: S,
}

impl<S: ChunkStore + Send + Sync> Array<S> {
    pub async fn fetch(&self) -> anyhow::Result<Arc<dyn NdArray>> {
        //Fetch all chunks, concat using arrow
        let all_chunks_res = self.chunk_provider.chunks().collect::<Vec<_>>().await;
        let all_chunks = all_chunks_res
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;

        // Concat the arrays
        let array = arrow::compute::concat(
            &all_chunks
                .iter()
                .map(|part| part.as_ref())
                .collect::<Vec<_>>(),
        )?;

        nd::try_from_arrow_ref(array, &self.array_shape, &self.dimensions).ok_or(anyhow::anyhow!(
            "Unable to convert stored array to nd-array using shared arrow buffer."
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::Array;
    use crate::array::data_type::I32Type;
    use crate::array::nd::AsNdArray;
    use crate::array::store::ChunkStore;
    use arrow::array::{ArrayRef, Int32Array};
    use futures::stream::{self, BoxStream, StreamExt};
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct TestChunkStore {
        chunks: Vec<ArrayRef>,
    }

    impl TestChunkStore {
        fn new(chunks: Vec<ArrayRef>) -> Self {
            Self { chunks }
        }
    }

    #[async_trait::async_trait]
    impl ChunkStore for TestChunkStore {
        fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayRef>> {
            let chunks = self.chunks.clone();
            stream::iter(chunks.into_iter().map(Ok)).boxed()
        }

        async fn fetch_chunk(
            &self,
            _start: usize,
            _len: usize,
        ) -> anyhow::Result<Option<ArrayRef>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn fetch_concats_chunks_into_ndarray() -> anyhow::Result<()> {
        let chunks = vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef,
        ];
        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            array_shape: vec![5],
            dimensions: vec!["x".to_string()],
            chunk_provider: TestChunkStore::new(chunks),
        };

        let nd = array.fetch().await?;
        assert_eq!(nd.shape(), &[5]);
        assert_eq!(nd.dimensions(), vec!["x"]);

        let values = nd
            .as_primitive_nd::<I32Type>()
            .expect("expected i32 ndarray");
        let collected: Vec<i32> = values.iter().cloned().collect();
        assert_eq!(collected, vec![1, 2, 3, 4, 5]);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_returns_error_when_no_chunks_available() {
        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            array_shape: vec![0],
            dimensions: vec!["x".to_string()],
            chunk_provider: TestChunkStore::new(Vec::new()),
        };

        assert!(array.fetch().await.is_err());
    }
}
