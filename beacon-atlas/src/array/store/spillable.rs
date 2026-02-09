use std::sync::Arc;

use anyhow::Context;
use futures::{StreamExt, stream::BoxStream};
use object_store::ObjectStore;

use crate::{
    array::{
        ArrayPart, ChunkStore,
        io_cache::{CacheKey, IoCache},
        layout::ArrayLayout,
    },
    arrow_object_store::ArrowObjectStoreReader,
};
use beacon_nd_arrow::column::NdArrowArrayColumn;

/// Lazily resolves chunked ND arrays from object storage using layout metadata.
///
/// This implementation reads Arrow IPC batches on demand and decodes a single
/// ND array row per chunk index.
#[derive(Debug)]
pub struct SpillableChunkStore<S: ObjectStore + Send + Sync> {
    /// Layout metadata for resolving chunk -> batch/row index.
    pub layout: Arc<ArrayLayout>,
    /// Reader that can fetch Arrow IPC batches from object storage.
    pub reader: Arc<ArrowObjectStoreReader<S>>,
    /// Shared IO cache for batch reads.
    pub io_cache: Arc<IoCache>,
}

fn chunk_start_shape(
    layout: &ArrayLayout,
    chunk_index: &[usize],
) -> anyhow::Result<(Vec<usize>, Vec<usize>)> {
    if layout.chunk_shape.len() != layout.array_shape.len()
        || layout.chunk_shape.len() != chunk_index.len()
    {
        return Err(anyhow::anyhow!("chunk index dimensionality does not match layout"));
    }

    let mut start = Vec::with_capacity(chunk_index.len());
    let mut shape = Vec::with_capacity(chunk_index.len());
    for (i, idx) in chunk_index.iter().enumerate() {
        let chunk_dim = layout.chunk_shape[i] as usize;
        let array_dim = layout.array_shape[i] as usize;
        let offset = idx.saturating_mul(chunk_dim);
        let remaining = array_dim.saturating_sub(offset);
        start.push(offset);
        shape.push(remaining.min(chunk_dim));
    }

    Ok((start, shape))
}

#[async_trait::async_trait]
impl<S: ObjectStore + Send + Sync> ChunkStore for SpillableChunkStore<S> {
    /// Streams all chunk parts by walking the layout and decoding rows lazily.
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayPart>> {
        // Clone shared state into the stream so we can move it into the async closure.
        let layout = self.layout.clone();
        let reader = self.reader.clone();
        let io_cache = self.io_cache.clone();

        // Iterate over the chunk index list, read the referenced batch, and decode the row.
        futures::stream::iter(layout.chunk_indexes.clone())
            .enumerate()
            .then(move |(index, chunk_indices)| {
                let layout = layout.clone();
                let reader = reader.clone();
                let io_cache = io_cache.clone();
                async move {
                    // Convert chunk indices to usize for ArrayPart.
                    let chunk_index = chunk_indices
                        .iter()
                        .map(|d| *d as usize)
                        .collect::<Vec<_>>();

                    // Map chunk position to the (batch, row) location in the IPC file.
                    let array_index = layout
                        .array_indexes
                        .get(index)
                        .ok_or_else(|| anyhow::anyhow!("missing array index for chunk {index}"))?;
                    let batch_index = array_index[0] as usize;
                    let array_in_batch_index = array_index[1] as usize;

                    // Read the batch (cached) and decode the ND array column.
                    let cache_key: CacheKey = (reader.path().clone(), batch_index);
                    let batch = io_cache
                        .get_or_insert_with(cache_key, move |_| async move {
                            reader
                                .read_batch(batch_index)
                                .await?
                                .ok_or_else(|| anyhow::anyhow!("missing batch {batch_index}"))
                        })
                        .await?;
                    let column = NdArrowArrayColumn::try_from_array(batch.column(0).clone())
                        .map_err(|err| anyhow::anyhow!(err))
                        .context("failed to decode ND array column")?;

                    if array_in_batch_index >= column.len() {
                        return Err(anyhow::anyhow!(
                            "array index {array_in_batch_index} out of bounds"
                        ));
                    }

                    // Extract the ND array row and return it with the chunk index.
                    let array = column
                        .row(array_in_batch_index)
                        .context("failed to read ND array row")?
                        .clone();
                    let (start, shape) = chunk_start_shape(&layout, &chunk_index)?;
                    Ok(ArrayPart {
                        array,
                        chunk_index,
                        start,
                        shape,
                    })
                }
            })
            .boxed()
    }

    /// Fetches a single chunk by index, returning `None` when not present.
    async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>> {
        // Copy the layout/reader so we can use them in this async function safely.
        let layout = self.layout.clone();
        let reader = self.reader.clone();
        let io_cache = self.io_cache.clone();

        // Find the chunk index in the layout metadata.
        let index = layout.chunk_indexes.iter().position(|idxs| {
            idxs.iter()
                .zip(chunk_index.iter())
                .all(|(a, b)| *a as usize == *b)
        });

        if let Some(index) = index {
            // Resolve the IPC batch and row for this chunk.
            let array_index = layout.array_indexes[index];
            let batch_index = array_index[0] as usize;
            let array_in_batch_index = array_index[1] as usize;

            // Read the batch, decode the ND array column, and return the row.
            let cache_key: CacheKey = (reader.path().clone(), batch_index);
            let batch = io_cache
                .get_or_insert_with(cache_key, move |_| async move {
                    reader
                        .read_batch(batch_index)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("missing batch {batch_index}"))
                })
                .await?;
            let column = NdArrowArrayColumn::try_from_array(batch.column(0).clone())
                .map_err(|err| anyhow::anyhow!(err))
                .context("failed to decode ND array column")?;

            if array_in_batch_index < column.len() {
                let array = column
                    .row(array_in_batch_index)
                    .context("failed to read ND array row")?
                    .clone();
                let (start, shape) = chunk_start_shape(&layout, &chunk_index)?;
                return Ok(Some(ArrayPart {
                    array,
                    chunk_index,
                    start,
                    shape,
                }));
            }
        }

        // Chunk index not present in the layout.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::DataType;
    use beacon_nd_arrow::NdArrowArray;
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use futures::{StreamExt, TryStreamExt};
    use object_store::{memory::InMemory, path::Path};

    use crate::array::writer::ArrayWriter;
    use crate::array::{Array, ArrayPart, ChunkStore, io_cache::IoCache, layout::ArrayLayouts};
    use crate::arrow_object_store::ArrowObjectStoreReader;
    use crate::config;

    use super::SpillableChunkStore;

    #[derive(Debug, Clone)]
    struct TestChunkStore {
        parts: Vec<ArrayPart>,
        index: HashMap<Vec<usize>, ArrayPart>,
    }

    impl TestChunkStore {
        fn new(parts: Vec<ArrayPart>) -> Self {
            let index = parts
                .iter()
                .cloned()
                .map(|part| (part.chunk_index.clone(), part))
                .collect();
            Self { parts, index }
        }
    }

    #[async_trait::async_trait]
    impl ChunkStore for TestChunkStore {
        fn chunks(&self) -> futures::stream::BoxStream<'static, anyhow::Result<ArrayPart>> {
            let parts = self.parts.clone();
            futures::stream::iter(parts.into_iter().map(Ok)).boxed()
        }

        async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>> {
            Ok(self.index.get(&chunk_index).cloned())
        }
    }

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
    async fn lazy_chunk_store_reads_chunks_and_fetches() -> anyhow::Result<()> {
        // Build a small two-chunk array and write it to the in-memory store.
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(config::io_cache_bytes()));
        let path = Path::from("lazy-array");

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

        let array = Array {
            array_datatype: DataType::Int32,
            chunk_shape: vec![2],
            array_shape: vec![4],
            chunk_provider: TestChunkStore::new(parts.clone()),
        };

        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32);
        writer.append_array(0, array).await?;
        writer.finalize().await?;

        let layouts = ArrayLayouts::from_object(store.clone(), path.child("layout.arrow")).await?;
        let layout = layouts.find_dataset_array_layout(0).expect("layout exists");
        let reader =
            Arc::new(ArrowObjectStoreReader::new(store.clone(), path.child("array.arrow")).await?);

        let lazy = SpillableChunkStore {
            layout: Arc::new(layout),
            reader,
            io_cache,
        };

        // Stream all chunks and validate the decoded data.
        let chunks = lazy.chunks();
        let loaded = chunks.try_collect::<Vec<_>>().await?;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].chunk_index, vec![0]);
        assert_eq!(array_values(&loaded[0].array), vec![1, 2]);
        assert_eq!(loaded[1].chunk_index, vec![1]);
        assert_eq!(array_values(&loaded[1].array), vec![3, 4]);

        // Fetch a single chunk and verify missing chunk handling.
        let part = lazy.fetch_chunk(vec![1]).await?.expect("chunk exists");
        assert_eq!(array_values(&part.array), vec![3, 4]);
        let missing = lazy.fetch_chunk(vec![2]).await?;
        assert!(missing.is_none());

        Ok(())
    }
}
