use std::collections::HashMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkedSchema {
    pub datasets: Vec<ChunkedDatasetSchema>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkedDatasetSchema {
    pub name: String,
    pub arrow_schema: arrow::datatypes::Schema,
    pub global_attributes: HashMap<String, ChunkedAttributeValue>,
    pub variables: HashMap<String, ChunkedVariableSchema>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkedVariableSchema {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub shape: Vec<usize>,
    pub dimensions: Vec<String>,
    pub chunked_shape: Vec<usize>,
    pub attributes: HashMap<String, ChunkedAttributeValue>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ChunkedAttributeValue {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bool(bool),
    Timestamp(i64),
    // Add more types as needed
}
