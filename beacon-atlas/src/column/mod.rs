pub mod reader;
pub mod writer;

use std::sync::Arc;

use beacon_nd_arrow::{
    NdArrowArrayDispatch,
    array::{NdArrowArray, compat_typings::ArrowTypeConversion},
};
pub use reader::ColumnReader;
pub use writer::ColumnWriter;

#[derive(Clone)]
pub struct Column {
    name: String,
    array: Arc<dyn NdArrowArray>,
}

impl Column {
    pub fn new(name: String, array: Arc<dyn NdArrowArray>) -> Self {
        Self { name, array }
    }

    pub fn new_from_vec<T: ArrowTypeConversion>(
        name: String,
        data: Vec<T>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        fill_value: Option<T>,
    ) -> anyhow::Result<Self> {
        let array = NdArrowArrayDispatch::new_in_mem(data, shape, dimensions, fill_value)?;
        Ok(Self {
            name,
            array: Arc::new(array),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> arrow::datatypes::DataType {
        self.array.data_type()
    }

    pub fn array(&self) -> Arc<dyn NdArrowArray> {
        self.array.clone()
    }
}
