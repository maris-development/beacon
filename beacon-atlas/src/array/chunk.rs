use std::sync::Arc;

use crate::array::{data_type::DataType, nd::NdArray};

pub struct ArrayChunk {
    pub start: Vec<usize>,
    pub shape: Vec<usize>,
    pub chunk_index: Vec<usize>,
    pub nd_array: Arc<dyn NdArray>,
}

impl ArrayChunk {
    pub fn data_type(&self) -> DataType {
        self.nd_array.data_type()
    }

    pub fn as_array(&self) -> &dyn NdArray {
        self.nd_array.as_ref()
    }

    pub fn as_array_cloned(&self) -> Arc<dyn NdArray> {
        self.nd_array.clone()
    }
}
