use std::sync::Arc;

use arrow::array::{ArrayRef, Scalar};

use crate::array::{Array, store::ChunkStore};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ColumnType {
    Array {
        data_type: arrow::datatypes::DataType,
    },
    Attribute {
        data_type: arrow::datatypes::DataType,
    },
}

#[derive(Debug, Clone)]
pub enum Column {
    Array(Array<Arc<dyn ChunkStore>>),
    Attribute(Scalar<ArrayRef>),
}

impl Column {
    pub fn column_type(&self) -> ColumnType {
        match self {
            Column::Array(_) => ColumnType::Array {
                data_type: self.as_array().array_datatype.clone(),
            },
            Column::Attribute(_) => ColumnType::Attribute {
                data_type: self.as_attribute().clone().into_inner().data_type().clone(),
            },
        }
    }

    pub fn as_array(&self) -> &Array<Arc<dyn ChunkStore>> {
        match self {
            Column::Array(array) => array,
            Column::Attribute(_) => panic!("Column is not an Array"),
        }
    }

    pub fn as_attribute(&self) -> &Scalar<ArrayRef> {
        match self {
            Column::Array(_) => panic!("Column is not an Attribute"),
            Column::Attribute(attr) => attr,
        }
    }
}
