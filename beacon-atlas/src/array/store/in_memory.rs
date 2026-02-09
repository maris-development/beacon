use std::collections::HashMap;

use futures::{StreamExt, stream::BoxStream};

use crate::array::{ArrayPart, ChunkStore};

/// Stores chunked arrays entirely in memory.
#[derive(Debug, Clone, Default)]
pub struct InMemoryChunkStore {
    parts: Vec<ArrayPart>,
    index: HashMap<Vec<usize>, ArrayPart>,
}

impl InMemoryChunkStore {
    /// Create a new in-memory chunk store from the provided parts.
    pub fn new(parts: Vec<ArrayPart>) -> Self {
        let index = parts
            .iter()
            .cloned()
            .map(|part| (part.chunk_index.clone(), part))
            .collect();
        Self { parts, index }
    }
}

#[async_trait::async_trait]
impl ChunkStore for InMemoryChunkStore {
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayPart>> {
        let parts = self.parts.clone();
        futures::stream::iter(parts.into_iter().map(Ok)).boxed()
    }

    async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>> {
        Ok(self.index.get(&chunk_index).cloned())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use beacon_nd_arrow::NdArrowArray;
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use futures::TryStreamExt;

    use crate::array::ArrayPart;
    use crate::array::store::ChunkStore;
    use crate::array::store::in_memory::InMemoryChunkStore;

    fn make_array(values: Vec<i32>, shape: Vec<usize>, names: Vec<&str>) -> NdArrowArray {
        let dims = names
            .into_iter()
            .zip(shape.into_iter())
            .map(|(name, size)| Dimension::try_new(name, size).unwrap())
            .collect::<Vec<_>>();
        NdArrowArray::new(Arc::new(Int32Array::from(values)), Dimensions::new(dims)).unwrap()
    }

    fn array_values(array: &NdArrowArray) -> Vec<i32> {
        let values = array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        (0..values.len()).map(|i| values.value(i)).collect()
    }

    #[tokio::test]
    async fn in_memory_store_streams_and_fetches() -> anyhow::Result<()> {
        let parts = vec![
            ArrayPart {
                array: make_array(vec![1, 2], vec![2], vec!["x"]),
                chunk_index: vec![0],
                start: vec![0],
                shape: vec![2],
            },
            ArrayPart {
                array: make_array(vec![3, 4], vec![2], vec!["x"]),
                chunk_index: vec![1],
                start: vec![2],
                shape: vec![2],
            },
        ];

        let store = InMemoryChunkStore::new(parts.clone());

        let loaded = store.chunks().try_collect::<Vec<_>>().await?;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].chunk_index, vec![0]);
        assert_eq!(array_values(&loaded[0].array), vec![1, 2]);
        assert_eq!(loaded[1].chunk_index, vec![1]);
        assert_eq!(array_values(&loaded[1].array), vec![3, 4]);

        let part = store.fetch_chunk(vec![1]).await?.expect("chunk exists");
        assert_eq!(array_values(&part.array), vec![3, 4]);
        let missing = store.fetch_chunk(vec![2]).await?;
        assert!(missing.is_none());

        Ok(())
    }
}
