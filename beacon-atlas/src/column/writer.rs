use std::sync::Arc;

use object_store::ObjectStore;

use crate::array::{Array, store::ChunkStore, writer::ArrayWriter};

pub struct ColumnWriter<S: ObjectStore + Clone> {
    object_store: S,
    column_directory: object_store::path::Path,

    array_writer: ArrayWriter<S>,
}

impl<S: ObjectStore + Clone> ColumnWriter<S> {
    pub fn new(
        object_store: S,
        column_directory: object_store::path::Path,
        data_type: arrow::datatypes::DataType,
        chunk_size: usize,
    ) -> Self {
        Self {
            array_writer: ArrayWriter::new(
                object_store.clone(),
                column_directory.child("array.arrow"),
                column_directory.child("layout.arrow"),
                data_type,
                chunk_size,
            ),
            object_store,
            column_directory,
        }
    }

    pub async fn write_column_array(
        &mut self,
        dataset_index: u32,
        array: Array<Arc<dyn ChunkStore>>,
    ) -> anyhow::Result<()> {
        self.array_writer.append_array(dataset_index, array).await
    }

    pub fn data_type(&self) -> &arrow::datatypes::DataType {
        self.array_writer.data_type()
    }

    pub async fn finalize(self) -> anyhow::Result<()> {
        self.array_writer.finalize().await
    }
}
