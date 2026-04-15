pub mod reader;
pub mod writer;

use std::sync::Arc;

use beacon_nd_arrow::{NdArray, NdArrayD, datatypes::NdArrayDataType};
pub use reader::ColumnReader;
pub use writer::ColumnWriter;

use crate::schema::_type::AtlasType;

#[derive(Clone)]
pub struct Column {
    name: String,
    array: Arc<dyn NdArrayD>,
}

impl Column {
    pub fn new(name: String, array: Arc<dyn NdArrayD>) -> Self {
        Self { name, array }
    }

    pub fn new_from_vec<T: AtlasType>(
        name: String,
        data: Vec<T>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        fill_value: Option<T>,
    ) -> anyhow::Result<Self> {
        let nd_array = NdArray::try_new_from_vec_in_mem(data, shape, dimensions, fill_value)?;

        Ok(Self {
            name,
            array: Arc::new(nd_array),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn datatype(&self) -> NdArrayDataType {
        self.array.datatype()
    }

    pub fn array(&self) -> Arc<dyn NdArrayD> {
        self.array.clone()
    }
}
