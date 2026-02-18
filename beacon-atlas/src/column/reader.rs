use std::sync::Arc;

use object_store::ObjectStore;

use crate::array::{Array, io_cache::IoCache, reader::ArrayReader, store::ChunkStore};

pub struct ColumnReader<S: ObjectStore + Clone> {
    object_store: S,
    column_directory: object_store::path::Path,

    // Array Reader
    reader: Arc<ArrayReader<S>>,
}

impl<S: ObjectStore + Clone> ColumnReader<S> {
    pub async fn new(
        object_store: S,
        column_directory: object_store::path::Path,
        io_cache: Arc<IoCache>,
    ) -> anyhow::Result<Self> {
        let array_path = column_directory.child("array.arrow");
        let layout_path = column_directory.child("layout.arrow");

        let reader = Arc::new(
            ArrayReader::new_with_cache(object_store.clone(), layout_path, array_path, io_cache)
                .await?,
        );

        Ok(Self {
            object_store,
            column_directory,
            reader,
        })
    }

    pub fn read_column_array(&self, dataset_index: u32) -> Option<Array<Arc<dyn ChunkStore>>> {
        self.reader.read_dataset_array(dataset_index)
    }
}
