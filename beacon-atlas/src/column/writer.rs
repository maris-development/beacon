use std::sync::Arc;

use beacon_nd_arrow::{NdArray, NdArrayD};
use object_store::ObjectStore;

use crate::{
    array::writer::ArrayFileWriter,
    consts::ARRAY_FILE_NAME,
    schema::_type::{AtlasDataType, AtlasType},
};

#[async_trait::async_trait]
pub trait ColumnWriterD {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_mutable_any(&mut self) -> &mut dyn std::any::Any;
    fn column_directory(&self) -> &object_store::path::Path;
    async fn write_column_array(
        &mut self,
        entry_index: u32,
        array: Arc<dyn NdArrayD>,
    ) -> anyhow::Result<()>;
    fn data_type(&self) -> AtlasDataType;
    async fn finish(self: Box<Self>) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl<T: AtlasType, S: ObjectStore + Clone> ColumnWriterD for ColumnWriter<T, S> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_mutable_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn column_directory(&self) -> &object_store::path::Path {
        &self.column_directory
    }
    async fn write_column_array(
        &mut self,
        entry_index: u32,
        array: Arc<dyn NdArrayD>,
    ) -> anyhow::Result<()> {
        if self.data_type() != array.datatype().into() {
            anyhow::bail!(
                "data type mismatch: expected {:?}, got {:?}",
                self.data_type(),
                array.datatype()
            );
        }

        // SAFETY: We have checked that the data type matches, so this downcast should always succeed.
        let array = array
            .as_any()
            .downcast_ref::<NdArray<T>>()
            .expect("data type checked above, so this should never fail");
        self.write_column_array_typed(entry_index, array.clone())
            .await
    }
    fn data_type(&self) -> AtlasDataType {
        self.array_writer.datatype()
    }
    async fn finish(self: Box<Self>) -> anyhow::Result<()> {
        self.array_writer.finish().await
    }
}

pub struct ColumnWriter<T: AtlasType, S: ObjectStore + Clone> {
    column_directory: object_store::path::Path,
    array_writer: ArrayFileWriter<T, S>,
}

impl<T: AtlasType, S: ObjectStore + Clone> ColumnWriter<T, S> {
    pub fn new(
        object_store: S,
        column_directory: object_store::path::Path,
        chunk_size: usize,
    ) -> anyhow::Result<Self> {
        let array_writer = ArrayFileWriter::<T, S>::new(
            object_store.clone(),
            column_directory.child(ARRAY_FILE_NAME),
            chunk_size,
        )?;

        Ok(Self {
            column_directory,
            array_writer,
        })
    }

    pub fn column_directory(&self) -> &object_store::path::Path {
        &self.column_directory
    }

    pub async fn write_column_array_typed(
        &mut self,
        entry_index: u32,
        array: NdArray<T>,
    ) -> anyhow::Result<()> {
        self.array_writer.append(entry_index, array).await
    }

    pub fn data_type(&self) -> AtlasDataType {
        self.array_writer.datatype()
    }

    pub async fn finish(self) -> anyhow::Result<()> {
        self.array_writer.finish().await
    }
}
