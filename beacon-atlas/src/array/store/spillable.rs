use std::sync::Arc;

use anyhow::Context;
use arrow::array::{Array, ArrayRef};
use futures::{StreamExt, stream::BoxStream};
use object_store::ObjectStore;

use crate::{
    array::{
        io_cache::{CacheKey, IoCache},
        layout::ArrayLayout,
        store::ChunkStore,
    },
    arrow_object_store::ArrowObjectStoreReader,
};

#[derive(Debug)]
pub struct SpillableChunkStore<S: ObjectStore + Send + Sync> {
    /// The logical length of each batch in the IPC file, used for chunk -> batch mapping.
    pub array_batch_len: usize,
    /// Layout metadata for resolving chunk -> batch/row index.
    pub layout: Arc<ArrayLayout>,
    /// Reader that can fetch Arrow IPC batches from object storage.
    pub reader: Arc<ArrowObjectStoreReader<S>>,
    /// Shared IO cache for batch reads.
    pub io_cache: Arc<IoCache>,
}

#[async_trait::async_trait]
impl<S: ObjectStore + Send + Sync> ChunkStore for SpillableChunkStore<S> {
    /// Streams all chunk parts by walking the layout and decoding rows lazily.
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayRef>> {
        let reader = self.reader.clone();
        let io_cache = self.io_cache.clone();
        let layout_start = usize::try_from(self.layout.array_start).unwrap_or(usize::MAX);
        let layout_len = usize::try_from(self.layout.array_len).unwrap_or(0);
        let layout_end = layout_start.saturating_add(layout_len);

        if layout_len == 0 || self.array_batch_len == 0 {
            return futures::stream::empty().boxed();
        }

        let array_batch_len = self.array_batch_len;
        let start_batch = layout_start / array_batch_len;
        let end_batch = layout_end.saturating_sub(1) / array_batch_len;

        futures::stream::iter(start_batch..=end_batch)
            .then(move |batch_index| {
                let reader = reader.clone();
                let io_cache = io_cache.clone();
                async move {
                    let cache_key: CacheKey = (reader.path().clone(), batch_index);
                    let batch = match io_cache
                        .get_or_insert_with(cache_key, move |_| async move {
                            reader
                                .read_batch(batch_index)
                                .await?
                                .ok_or_else(|| anyhow::anyhow!("missing batch {batch_index}"))
                        })
                        .await
                    {
                        Ok(batch) => batch,
                        Err(err) => return Err(err),
                    };
                    let array = batch.column(0).clone();
                    let array_len = array.len();

                    let batch_start = batch_index * array_batch_len;
                    let batch_end = batch_start.saturating_add(array_len);
                    let overlap_start = layout_start.max(batch_start);
                    let overlap_end = layout_end.min(batch_end);

                    if overlap_end <= overlap_start {
                        return Ok(None);
                    }

                    let slice_start = overlap_start - batch_start;
                    let slice_len = overlap_end - overlap_start;
                    Ok(Some(array.slice(slice_start, slice_len)))
                }
            })
            .filter_map(|result| async move {
                match result {
                    Ok(Some(array)) => Some(Ok(array)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .boxed()
    }

    async fn fetch_chunk(&self, start: usize, len: usize) -> anyhow::Result<Option<ArrayRef>> {
        if len == 0 {
            return Ok(None);
        }

        let layout_start =
            usize::try_from(self.layout.array_start).context("layout start overflowed")?;
        let layout_len =
            usize::try_from(self.layout.array_len).context("layout length overflowed")?;
        if start >= layout_len {
            return Ok(None);
        }

        let relative_len = len.min(layout_len - start);
        if relative_len == 0 {
            return Ok(None);
        }

        let absolute_start = layout_start
            .checked_add(start)
            .context("absolute start overflowed")?;
        let end = absolute_start
            .checked_add(relative_len)
            .context("slice end overflowed")?;
        let mut collected_arrays: Vec<ArrayRef> = Vec::new();

        if self.array_batch_len == 0 {
            return Ok(None);
        }

        let start_batch = absolute_start / self.array_batch_len;
        let end_batch = end.saturating_sub(1) / self.array_batch_len;

        for batch_index in start_batch..=end_batch {
            let reader = self.reader.clone();
            let io_cache = self.io_cache.clone();
            let cache_key: CacheKey = (reader.path().clone(), batch_index);
            let batch = match io_cache
                .get_or_insert_with(cache_key, move |_| async move {
                    reader
                        .read_batch(batch_index)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("missing batch {batch_index}"))
                })
                .await
            {
                Ok(batch) => batch,
                Err(err) => {
                    return Err(err).context("failed to load batch from cache");
                }
            };
            let array = batch.column(0).clone();
            let array_len = array.len();

            let batch_start = batch_index * self.array_batch_len;
            let batch_end = batch_start.saturating_add(array_len);

            if batch_end <= absolute_start {
                continue;
            }
            if batch_start >= end {
                break;
            }

            let overlap_start = absolute_start.max(batch_start);
            let overlap_end = end.min(batch_end);
            if overlap_end > overlap_start {
                let slice_start = overlap_start - batch_start;
                let slice_len = overlap_end - overlap_start;
                collected_arrays.push(array.slice(slice_start, slice_len));
            }
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
    use super::SpillableChunkStore;
    use crate::{
        array::io_cache::IoCache, array::layout::ArrayLayout, array::store::ChunkStore,
        arrow_object_store::ArrowObjectStoreReader,
    };
    use arrow::{
        array::{Array, ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema},
        ipc::writer::FileWriter,
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use futures::TryStreamExt;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::{io::Cursor, sync::Arc};

    fn array_values(array: &ArrayRef) -> Vec<i32> {
        let values = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        (0..values.len()).map(|i| values.value(i)).collect()
    }

    async fn write_ipc_batches(
        store: &InMemory,
        path: &Path,
        batches: Vec<ArrayRef>,
    ) -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "values",
            DataType::Int32,
            false,
        )]));
        let mut cursor = Cursor::new(Vec::new());
        let mut writer = FileWriter::try_new(&mut cursor, &schema)?;

        for array in batches {
            let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
            writer.write(&batch)?;
        }
        writer.finish()?;

        store
            .put(path, Bytes::from(cursor.into_inner()).into())
            .await?;
        Ok(())
    }

    async fn make_store()
    -> anyhow::Result<(Arc<ArrowObjectStoreReader<InMemory>>, Arc<ArrayLayout>)> {
        let store = InMemory::new();
        let path = Path::from("array.arrow");
        let batches: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
            Arc::new(Int32Array::from(vec![7])) as ArrayRef,
        ];
        write_ipc_batches(&store, &path, batches).await?;

        let reader = Arc::new(ArrowObjectStoreReader::new(store, path).await?);
        let layout = Arc::new(ArrayLayout {
            dataset_index: 0,
            array_start: 1,
            array_len: 5,
            array_shape: vec![5],
            dimensions: vec!["x".to_string()],
        });
        Ok((reader, layout))
    }

    #[tokio::test]
    async fn spillable_store_streams_batches() -> anyhow::Result<()> {
        let (reader, layout) = make_store().await?;
        let store = SpillableChunkStore {
            array_batch_len: 3,
            layout,
            reader,
            io_cache: Arc::new(IoCache::new(1024 * 1024)),
        };

        let arrays = store.chunks().try_collect::<Vec<_>>().await?;
        assert_eq!(arrays.len(), 2);
        assert_eq!(array_values(&arrays[0]), vec![2, 3]);
        assert_eq!(array_values(&arrays[1]), vec![4, 5, 6]);

        Ok(())
    }

    #[tokio::test]
    async fn spillable_store_fetches_and_concats() -> anyhow::Result<()> {
        let (reader, layout) = make_store().await?;
        let store = SpillableChunkStore {
            array_batch_len: 3,
            layout,
            reader,
            io_cache: Arc::new(IoCache::new(1024 * 1024)),
        };

        let slice = store.fetch_chunk(0, 5).await?.expect("slice exists");
        assert_eq!(array_values(&slice), vec![2, 3, 4, 5, 6]);

        let single = store.fetch_chunk(1, 2).await?.expect("slice exists");
        assert_eq!(array_values(&single), vec![3, 4]);

        let tail = store.fetch_chunk(4, 4).await?.expect("slice exists");
        assert_eq!(array_values(&tail), vec![6]);

        let missing = store.fetch_chunk(10, 1).await?;
        assert!(missing.is_none());

        Ok(())
    }
}
