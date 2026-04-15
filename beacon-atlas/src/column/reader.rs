use std::sync::Arc;

use beacon_nd_arrow::NdArrayD;
use object_store::ObjectStore;

use crate::{
    array::reader::ArrayFileReader, cache::VecCache, consts::ARRAY_FILE_NAME,
    schema::_type::AtlasDataType,
};

#[derive(Debug, Clone)]
pub struct ColumnReader<S: ObjectStore + Clone> {
    // Array Reader
    reader: Arc<ArrayFileReader<S>>,
}

impl<S: ObjectStore + Clone> ColumnReader<S> {
    pub async fn new(
        object_store: S,
        column_directory: object_store::path::Path,
        io_cache: Arc<VecCache>,
    ) -> anyhow::Result<Self> {
        let array_path = column_directory.child(ARRAY_FILE_NAME);

        let reader =
            Arc::new(ArrayFileReader::open(object_store, array_path, Some(io_cache)).await?);

        Ok(Self { reader })
    }

    pub fn column_array_dataset_indexes(&self) -> Vec<u32> {
        self.reader.footer().lookup_table.keys().cloned().collect()
    }

    pub fn read_column_array(
        &self,
        dataset_index: u32,
    ) -> Option<anyhow::Result<Arc<dyn NdArrayD>>> {
        todo!()
    }

    pub fn column_data_type(&self) -> AtlasDataType {
        self.reader.footer().datatype.clone()
    }
}
