// use std::collections::HashMap;

// use futures::{StreamExt, stream::BoxStream};

use anyhow::Context;
use arrow::array::{Array, ArrayRef};
use futures::{StreamExt, stream::BoxStream};

use crate::array::store::ChunkStore;

/// Stores chunked arrays entirely in memory.
#[derive(Debug, Clone, Default)]
pub struct InMemoryChunkStore {
    arrays: Vec<ArrayRef>,
}

impl InMemoryChunkStore {
    /// Create a new in-memory chunk store from the provided parts.
    pub fn new(arrays: Vec<ArrayRef>) -> Self {
        Self { arrays }
    }
}

#[async_trait::async_trait]
impl ChunkStore for InMemoryChunkStore {
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayRef>> {
        let arrays = self.arrays.clone();
        futures::stream::iter(arrays.into_iter().map(Ok)).boxed()
    }

    async fn fetch_chunk(&self, start: usize, len: usize) -> anyhow::Result<Option<ArrayRef>> {
        if len == 0 {
            return Ok(None);
        }

        // Find the start (start element index) and then collect all arrays that fit within the length from start onwards
        let end = start.checked_add(len).context("slice end overflowed")?;
        let mut collected_arrays: Vec<ArrayRef> = Vec::new();

        let mut cursor = 0usize;
        for array in &self.arrays {
            let array_len = array.len();
            let array_start = cursor;
            let array_end = cursor.saturating_add(array_len);

            if array_end <= start {
                cursor = array_end;
                continue;
            }
            if array_start >= end {
                break;
            }

            let overlap_start = start.max(array_start);
            let overlap_end = end.min(array_end);
            if overlap_end > overlap_start {
                let slice_start = overlap_start - array_start;
                let slice_len = overlap_end - overlap_start;
                collected_arrays.push(array.slice(slice_start, slice_len));
            }

            cursor = array_end;
        }

        if collected_arrays.is_empty() {
            return Ok(None);
        }

        if collected_arrays.len() == 1 {
            return Ok(collected_arrays.pop());
        }

        let concat_inputs: Vec<&dyn arrow::array::Array> = collected_arrays
            .iter()
            .map(|array| array.as_ref())
            .collect();
        let concatenated = arrow::compute::concat(&concat_inputs)
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to concatenate fetched arrays")?;
        Ok(Some(concatenated))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayRef, Int32Array};
    use futures::TryStreamExt;
    use std::sync::Arc;

    use super::InMemoryChunkStore;
    use crate::array::store::ChunkStore;

    fn make_array(values: Vec<i32>) -> ArrayRef {
        Arc::new(Int32Array::from(values)) as ArrayRef
    }

    fn array_values(array: &ArrayRef) -> Vec<i32> {
        let values = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        (0..values.len()).map(|i| values.value(i)).collect()
    }

    #[tokio::test]
    async fn in_memory_store_streams_arrays() -> anyhow::Result<()> {
        let arrays = vec![make_array(vec![1, 2]), make_array(vec![3, 4])];
        let store = InMemoryChunkStore::new(arrays);

        let loaded = store.chunks().try_collect::<Vec<_>>().await?;
        assert_eq!(loaded.len(), 2);
        assert_eq!(array_values(&loaded[0]), vec![1, 2]);
        assert_eq!(array_values(&loaded[1]), vec![3, 4]);

        Ok(())
    }

    #[tokio::test]
    async fn in_memory_store_fetches_and_concats() -> anyhow::Result<()> {
        let arrays = vec![
            make_array(vec![1, 2, 3]),
            make_array(vec![4, 5]),
            make_array(vec![6, 7, 8, 9]),
        ];
        let store = InMemoryChunkStore::new(arrays);

        let slice = store.fetch_chunk(1, 5).await?.expect("slice exists");
        assert_eq!(array_values(&slice), vec![2, 3, 4, 5, 6]);

        let single = store.fetch_chunk(3, 2).await?.expect("slice exists");
        assert_eq!(array_values(&single), vec![4, 5]);

        let tail = store.fetch_chunk(7, 2).await?.expect("slice exists");
        assert_eq!(array_values(&tail), vec![8, 9]);

        let missing = store.fetch_chunk(20, 1).await?;
        assert!(missing.is_none());

        Ok(())
    }
}
