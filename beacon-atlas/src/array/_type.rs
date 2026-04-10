use crate::array::file::{ArrayV1, VersionedArray};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Timestamp,
    Binary,
    String,
}

impl From<&VersionedArray> for ArrayType {
    fn from(array: &VersionedArray) -> Self {
        match array {
            VersionedArray::V1(inner) => match inner {
                ArrayV1::Bool(_) => ArrayType::Bool,
                ArrayV1::I8(_) => ArrayType::I8,
                ArrayV1::I16(_) => ArrayType::I16,
                ArrayV1::I32(_) => ArrayType::I32,
                ArrayV1::I64(_) => ArrayType::I64,
                ArrayV1::U8(_) => ArrayType::U8,
                ArrayV1::U16(_) => ArrayType::U16,
                ArrayV1::U32(_) => ArrayType::U32,
                ArrayV1::U64(_) => ArrayType::U64,
                ArrayV1::F32(_) => ArrayType::F32,
                ArrayV1::F64(_) => ArrayType::F64,
                ArrayV1::Timestamp(_) => ArrayType::Timestamp,
                ArrayV1::Binary(_) => ArrayType::Binary,
                ArrayV1::String(_) => ArrayType::String,
            },
        }
    }
}
