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
        let array_datatype = beacon_nd_arrow::extension::nd_column_data_type(
            array_field.field(0).data_type().clone(),
        );

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
