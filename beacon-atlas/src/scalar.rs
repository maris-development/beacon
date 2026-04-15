use beacon_nd_arrow::datatypes::TimestampNanosecond;

use crate::schema::_type::AtlasDataType;

#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(attr(derive(Debug, PartialEq)))]
pub enum Scalar {
    Bool(bool),
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
    Timestamp(TimestampNanosecond),
    Binary(Vec<u8>),
    String(String),
}

impl Scalar {
    pub fn data_type(&self) -> AtlasDataType {
        match self {
            Scalar::Bool(_) => AtlasDataType::Bool,
            Scalar::I8(_) => AtlasDataType::I8,
            Scalar::I16(_) => AtlasDataType::I16,
            Scalar::I32(_) => AtlasDataType::I32,
            Scalar::I64(_) => AtlasDataType::I64,
            Scalar::U8(_) => AtlasDataType::U8,
            Scalar::U16(_) => AtlasDataType::U16,
            Scalar::U32(_) => AtlasDataType::U32,
            Scalar::U64(_) => AtlasDataType::U64,
            Scalar::F32(_) => AtlasDataType::F32,
            Scalar::F64(_) => AtlasDataType::F64,
            Scalar::Timestamp(_) => AtlasDataType::Timestamp,
            Scalar::Binary(_) => AtlasDataType::Binary,
            Scalar::String(_) => AtlasDataType::String,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        if let Scalar::Bool(b) = self {
            Some(*b)
        } else {
            None
        }
    }

    pub fn as_i8(&self) -> Option<i8> {
        if let Scalar::I8(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        if let Scalar::I16(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        if let Scalar::I32(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        if let Scalar::I64(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_u8(&self) -> Option<u8> {
        if let Scalar::U8(u) = self {
            Some(*u)
        } else {
            None
        }
    }

    pub fn as_u16(&self) -> Option<u16> {
        if let Scalar::U16(u) = self {
            Some(*u)
        } else {
            None
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        if let Scalar::U32(u) = self {
            Some(*u)
        } else {
            None
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        if let Scalar::U64(u) = self {
            Some(*u)
        } else {
            None
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        if let Scalar::F32(f) = self {
            Some(*f)
        } else {
            None
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        if let Scalar::F64(f) = self {
            Some(*f)
        } else {
            None
        }
    }

    pub fn as_binary(&self) -> Option<&[u8]> {
        if let Scalar::Binary(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        if let Scalar::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_timestamp(&self) -> Option<&TimestampNanosecond> {
        if let Scalar::Timestamp(ts) = self {
            Some(ts)
        } else {
            None
        }
    }
}

impl From<bool> for Scalar {
    fn from(value: bool) -> Self {
        Scalar::Bool(value)
    }
}

impl From<i8> for Scalar {
    fn from(value: i8) -> Self {
        Scalar::I8(value)
    }
}

impl From<i16> for Scalar {
    fn from(value: i16) -> Self {
        Scalar::I16(value)
    }
}

impl From<i32> for Scalar {
    fn from(value: i32) -> Self {
        Scalar::I32(value)
    }
}

impl From<i64> for Scalar {
    fn from(value: i64) -> Self {
        Scalar::I64(value)
    }
}

impl From<u8> for Scalar {
    fn from(value: u8) -> Self {
        Scalar::U8(value)
    }
}

impl From<u16> for Scalar {
    fn from(value: u16) -> Self {
        Scalar::U16(value)
    }
}

impl From<u32> for Scalar {
    fn from(value: u32) -> Self {
        Scalar::U32(value)
    }
}

impl From<u64> for Scalar {
    fn from(value: u64) -> Self {
        Scalar::U64(value)
    }
}

impl From<f32> for Scalar {
    fn from(value: f32) -> Self {
        Scalar::F32(value)
    }
}

impl From<f64> for Scalar {
    fn from(value: f64) -> Self {
        Scalar::F64(value)
    }
}

impl From<TimestampNanosecond> for Scalar {
    fn from(value: TimestampNanosecond) -> Self {
        Scalar::Timestamp(value)
    }
}

impl From<Vec<u8>> for Scalar {
    fn from(value: Vec<u8>) -> Self {
        Scalar::Binary(value)
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Scalar::String(value)
    }
}

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Scalar::String(value.to_string())
    }
}
