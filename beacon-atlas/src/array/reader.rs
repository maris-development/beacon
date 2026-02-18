use std::sync::Arc;

use object_store::ObjectStore;

use crate::{
    array::{
        Array, ChunkStore, io_cache::IoCache, layout::ArrayLayouts, store::SpillableChunkStore,
    },
    arrow_object_store::ArrowObjectStoreReader,
};

/// Reads chunked ND Arrow arrays and their layout metadata from object storage.
pub struct ArrayReader<S: ObjectStore + Clone> {
    array_reader: Arc<ArrowObjectStoreReader<S>>,
    layouts: ArrayLayouts,
    array_datatype: arrow::datatypes::DataType,
    io_cache: Arc<IoCache>,
}

impl<S: ObjectStore + Clone> ArrayReader<S> {
    pub async fn new_with_cache(
        store: S,
        layout_path: object_store::path::Path,
        array_path: object_store::path::Path,
        io_cache: Arc<IoCache>,
    ) -> anyhow::Result<Self> {
        let array_reader = Arc::new(ArrowObjectStoreReader::new(store.clone(), array_path).await?);

        let array_field = array_reader.schema();
        let array_datatype = array_field.field(0).data_type().clone();

        Ok(Self {
            array_reader,
            layouts: ArrayLayouts::from_object(store.clone(), layout_path).await?,
            array_datatype,
            io_cache,
        })
    }

    /// Returns the loaded array layouts.
    pub fn layouts(&self) -> &ArrayLayouts {
        &self.layouts
    }

    /// Returns the array datatype stored in this reader.
    pub fn array_datatype(&self) -> &arrow::datatypes::DataType {
        &self.array_datatype
    }

    /// Returns the shared IO cache.
    pub fn io_cache(&self) -> &Arc<IoCache> {
        &self.io_cache
    }

    /// Returns a chunked array for a dataset index, if present.
    pub fn read_dataset_array(&self, dataset_index: u32) -> Option<Array<Arc<dyn ChunkStore>>> {
        let layout = if let Some(layout) = self.layouts.find_dataset_array_layout(dataset_index) {
            layout.clone()
        } else {
            return None;
        };
        let layout = Arc::new(layout);

        let provider = Arc::new(SpillableChunkStore {
            layout: layout.clone(),
            reader: self.array_reader.clone(),
            io_cache: self.io_cache.clone(),
            array_batch_len: self.array_reader.num_batches(),
        });

        Some(Array {
            array_datatype: self.array_datatype.clone(),
            dimensions: layout.dimensions.clone(),
            array_shape: layout.array_shape.iter().map(|d| *d as usize).collect(),
            chunk_provider: provider,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ArrayReader;
    use crate::array::Array;
    use crate::array::data_type::I32Type;
    use crate::array::io_cache::IoCache;
    use crate::array::nd::AsNdArray;
    use crate::array::store::InMemoryChunkStore;
    use crate::array::writer::ArrayWriter;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::DataType;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;

    fn chunk(values: Vec<i32>) -> ArrayRef {
        Arc::new(Int32Array::from(values)) as ArrayRef
    }

    fn build_array(
        chunks: Vec<Vec<i32>>,
        shape: Vec<usize>,
        dims: Vec<&str>,
    ) -> Array<InMemoryChunkStore> {
        let arrays = chunks.into_iter().map(chunk).collect::<Vec<_>>();
        Array {
            array_datatype: DataType::Int32,
            array_shape: shape,
            dimensions: dims.into_iter().map(|d| d.to_string()).collect(),
            chunk_provider: InMemoryChunkStore::new(arrays),
        }
    }

    #[tokio::test]
    async fn read_dataset_array_returns_data_and_metadata() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let object_store = store.clone();
        let root = Path::from("reader/basic");
        let array_path = root.child("array.arrow");
        let layout_path = root.child("layout.arrow");

        let mut writer = ArrayWriter::new(
            object_store,
            array_path.clone(),
            layout_path.clone(),
            DataType::Int32,
            2,
        );
        let array = build_array(vec![vec![1, 2], vec![3, 4]], vec![4], vec!["x"]);
        writer.append_array(7, array).await?;
        writer.finalize().await?;

        let cache = Arc::new(IoCache::new(1024 * 1024));
        let reader =
            ArrayReader::new_with_cache(store.clone(), layout_path, array_path, cache.clone())
                .await?;

        assert!(Arc::ptr_eq(reader.io_cache(), &cache));
        let expected_dtype = DataType::Int32;
        assert_eq!(reader.array_datatype(), &expected_dtype);

        let loaded = reader
            .read_dataset_array(7)
            .expect("expected dataset array");
        assert_eq!(loaded.array_shape, vec![4]);
        assert_eq!(loaded.dimensions, vec!["x".to_string()]);

        let nd = loaded.fetch().await?;
        let values = nd
            .as_primitive_nd::<I32Type>()
            .expect("expected i32 ndarray");
        let collected: Vec<i32> = values.iter().cloned().collect();
        assert_eq!(collected, vec![1, 2, 3, 4]);

        Ok(())
    }

    #[tokio::test]
    async fn read_dataset_array_returns_none_when_missing() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let object_store = store.clone();
        let root = Path::from("reader/missing");
        let array_path = root.child("array.arrow");
        let layout_path = root.child("layout.arrow");

        let mut writer = ArrayWriter::new(
            object_store,
            array_path.clone(),
            layout_path.clone(),
            DataType::Int32,
            4,
        );
        let array = build_array(vec![vec![9, 10]], vec![2], vec!["x"]);
        writer.append_array(1, array).await?;
        writer.finalize().await?;

        let reader = ArrayReader::new_with_cache(
            store,
            layout_path,
            array_path,
            Arc::new(IoCache::new(1024 * 1024)),
        )
        .await?;

        assert!(reader.read_dataset_array(99).is_none());
        Ok(())
    }
}
