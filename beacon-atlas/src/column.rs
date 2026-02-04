use std::sync::Arc;

use arrow::array::{ArrayRef, Scalar};

use crate::array_chunked::{ChunkedArray, ChunkedArrayProvider};

pub enum ColumnType {
    Array,
    Attribute,
}

#[derive(Debug, Clone)]
pub enum Column {
    Array(ChunkedArray<Arc<dyn ChunkedArrayProvider>>),
    Attribute(Scalar<ArrayRef>),
}

impl Column {
    pub fn column_type(&self) -> ColumnType {
        match self {
            Column::Array(_) => ColumnType::Array,
            Column::Attribute(_) => ColumnType::Attribute,
        }
    }
}
