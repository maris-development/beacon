use std::sync::Arc;

use object_store::ObjectStore;

use crate::{
    array::writer::ArrayWriter,
    consts::{ARRAY_FILE_NAME, LAYOUT_FILE_NAME, STATISTICS_FILE_NAME},
};

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
    ) -> anyhow::Result<Self> {
        let array_path = column_directory.child(ARRAY_FILE_NAME);
        let layout_path = column_directory.child(LAYOUT_FILE_NAME);
        let statistics_path = column_directory.child(STATISTICS_FILE_NAME);
        let array_writer = ArrayWriter::new(
            object_store.clone(),
            array_path,
            layout_path,
            statistics_path,
            data_type,
            chunk_size,
        )?;

        Ok(Self {
            object_store,
            column_directory,
            array_writer,
        })
    }

    pub fn column_directory(&self) -> &object_store::path::Path {
        &self.column_directory
    }

    pub async fn write_column_array(
        &mut self,
        dataset_index: u32,
        array: Arc<dyn beacon_nd_arrow::array::NdArrowArray>,
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
