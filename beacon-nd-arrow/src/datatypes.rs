#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NdArrayDataType {
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

pub trait NdArrayType: Clone + std::fmt::Debug + Send + Sync + 'static {
    fn data_type() -> NdArrayDataType;
}

impl NdArrayType for bool {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::Bool
    }
}

impl NdArrayType for i8 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::I8
    }
}

impl NdArrayType for i16 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::I16
    }
}

impl NdArrayType for i32 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::I32
    }
}

impl NdArrayType for i64 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::I64
    }
}

impl NdArrayType for u8 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::U8
    }
}

impl NdArrayType for u16 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::U16
    }
}

impl NdArrayType for u32 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::U32
    }
}

impl NdArrayType for u64 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::U64
    }
}

impl NdArrayType for f32 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::F32
    }
}

impl NdArrayType for f64 {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::F64
    }
}

impl NdArrayType for String {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::String
    }
}

impl NdArrayType for Vec<u8> {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::Binary
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
    bytemuck::Pod,
    bytemuck::Zeroable,
)]
#[repr(transparent)]
#[rkyv(attr(derive(Debug, PartialEq)))]
pub struct TimestampNanosecond(pub i64);

impl NdArrayType for TimestampNanosecond {
    fn data_type() -> NdArrayDataType {
        NdArrayDataType::Timestamp
    }
}
