pub mod reader;
pub mod writer;

use std::sync::Arc;

pub use reader::ColumnReader;
pub use writer::ColumnWriter;

use crate::array::{Array, store::ChunkStore};

pub struct Column {
    array: Array<Arc<dyn ChunkStore>>,
    name: String,
}

impl Column {
    pub fn new(array: impl Into<Array<Arc<dyn ChunkStore>>>, name: String) -> Self {
        Self {
            array: array.into(),
            name,
        }
    }
}
