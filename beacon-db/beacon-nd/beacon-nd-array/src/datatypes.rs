#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

impl NdArrayDataType {
    /// Compute the super type (widest common type) of two data types.
    ///
    /// Returns `None` if no valid promotion exists (e.g. `Binary` + numeric).
    pub fn super_type(&self, other: &NdArrayDataType) -> Option<NdArrayDataType> {
        use NdArrayDataType::*;

        if self == other {
            return Some(self.clone());
        }

        match (self, other) {
            // String absorbs everything
            (String, _) | (_, String) => Some(String),

            // Null-like: Bool promotes to any numeric
            (Bool, I8) | (I8, Bool) => Some(I8),
            (Bool, I16) | (I16, Bool) => Some(I16),
            (Bool, I32) | (I32, Bool) => Some(I32),
            (Bool, I64) | (I64, Bool) => Some(I64),
            (Bool, U8) | (U8, Bool) => Some(U8),
            (Bool, U16) | (U16, Bool) => Some(U16),
            (Bool, U32) | (U32, Bool) => Some(U32),
            (Bool, U64) | (U64, Bool) => Some(U64),
            (Bool, F32) | (F32, Bool) => Some(F32),
            (Bool, F64) | (F64, Bool) => Some(F64),

            // Signed integer widening
            (I8, I16) | (I16, I8) => Some(I16),
            (I8, I32) | (I32, I8) => Some(I32),
            (I8, I64) | (I64, I8) => Some(I64),
            (I16, I32) | (I32, I16) => Some(I32),
            (I16, I64) | (I64, I16) => Some(I64),
            (I32, I64) | (I64, I32) => Some(I64),

            // Unsigned integer widening
            (U8, U16) | (U16, U8) => Some(U16),
            (U8, U32) | (U32, U8) => Some(U32),
            (U8, U64) | (U64, U8) => Some(U64),
            (U16, U32) | (U32, U16) => Some(U32),
            (U16, U64) | (U64, U16) => Some(U64),
            (U32, U64) | (U64, U32) => Some(U64),

            // Signed + unsigned → next signed width (or F64 if overflow)
            (I8, U8) | (U8, I8) => Some(I16),
            (I8, U16) | (U16, I8) => Some(I32),
            (I8, U32) | (U32, I8) => Some(I64),
            (I8, U64) | (U64, I8) => Some(F64),
            (I16, U8) | (U8, I16) => Some(I16),
            (I16, U16) | (U16, I16) => Some(I32),
            (I16, U32) | (U32, I16) => Some(I64),
            (I16, U64) | (U64, I16) => Some(F64),
            (I32, U8) | (U8, I32) => Some(I32),
            (I32, U16) | (U16, I32) => Some(I32),
            (I32, U32) | (U32, I32) => Some(I64),
            (I32, U64) | (U64, I32) => Some(F64),
            (I64, U8) | (U8, I64) => Some(I64),
            (I64, U16) | (U16, I64) => Some(I64),
            (I64, U32) | (U32, I64) => Some(I64),
            (I64, U64) | (U64, I64) => Some(F64),

            // Float widening
            (F32, F64) | (F64, F32) => Some(F64),

            // Int + Float promotion
            (I8, F32) | (F32, I8) => Some(F32),
            (I16, F32) | (F32, I16) => Some(F32),
            (I32, F32) | (F32, I32) => Some(F64),
            (I64, F32) | (F32, I64) => Some(F64),
            (I8, F64) | (F64, I8) => Some(F64),
            (I16, F64) | (F64, I16) => Some(F64),
            (I32, F64) | (F64, I32) => Some(F64),
            (I64, F64) | (F64, I64) => Some(F64),

            // Unsigned + Float promotion
            (U8, F32) | (F32, U8) => Some(F32),
            (U16, F32) | (F32, U16) => Some(F32),
            (U32, F32) | (F32, U32) => Some(F64),
            (U64, F32) | (F32, U64) => Some(F64),
            (U8, F64) | (F64, U8) => Some(F64),
            (U16, F64) | (F64, U16) => Some(F64),
            (U32, F64) | (F64, U32) => Some(F64),
            (U64, F64) | (F64, U64) => Some(F64),

            // Timestamp + numeric → I64 or F64
            (Timestamp, I8)
            | (I8, Timestamp)
            | (Timestamp, I16)
            | (I16, Timestamp)
            | (Timestamp, I32)
            | (I32, Timestamp)
            | (Timestamp, I64)
            | (I64, Timestamp) => Some(I64),
            (Timestamp, U8)
            | (U8, Timestamp)
            | (Timestamp, U16)
            | (U16, Timestamp)
            | (Timestamp, U32)
            | (U32, Timestamp) => Some(I64),
            (Timestamp, U64) | (U64, Timestamp) => Some(F64),
            (Timestamp, F32) | (F32, Timestamp) => Some(F64),
            (Timestamp, F64) | (F64, Timestamp) => Some(F64),
            (Timestamp, Bool) | (Bool, Timestamp) => Some(I64),

            // Binary is incompatible with everything else
            (Binary, _) | (_, Binary) => None,

            _ => None,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use NdArrayDataType::*;

    #[test]
    fn test_super_type_same_returns_self() {
        for dt in [
            Bool, I8, I16, I32, I64, U8, U16, U32, U64, F32, F64, Timestamp, Binary, String,
        ] {
            assert_eq!(dt.super_type(&dt), Some(dt.clone()));
        }
    }

    #[test]
    fn test_super_type_signed_widening() {
        assert_eq!(I8.super_type(&I16), Some(I16));
        assert_eq!(I8.super_type(&I32), Some(I32));
        assert_eq!(I8.super_type(&I64), Some(I64));
        assert_eq!(I16.super_type(&I32), Some(I32));
        assert_eq!(I16.super_type(&I64), Some(I64));
        assert_eq!(I32.super_type(&I64), Some(I64));
        // Symmetric
        assert_eq!(I64.super_type(&I8), Some(I64));
    }

    #[test]
    fn test_super_type_unsigned_widening() {
        assert_eq!(U8.super_type(&U16), Some(U16));
        assert_eq!(U8.super_type(&U32), Some(U32));
        assert_eq!(U8.super_type(&U64), Some(U64));
        assert_eq!(U16.super_type(&U32), Some(U32));
        assert_eq!(U32.super_type(&U64), Some(U64));
    }

    #[test]
    fn test_super_type_signed_unsigned_cross() {
        assert_eq!(I8.super_type(&U8), Some(I16));
        assert_eq!(I8.super_type(&U16), Some(I32));
        assert_eq!(I8.super_type(&U32), Some(I64));
        assert_eq!(I8.super_type(&U64), Some(F64));
        assert_eq!(I16.super_type(&U16), Some(I32));
        assert_eq!(I32.super_type(&U32), Some(I64));
        assert_eq!(I64.super_type(&U64), Some(F64));
        // Symmetric
        assert_eq!(U32.super_type(&I8), Some(I64));
    }

    #[test]
    fn test_super_type_float_widening() {
        assert_eq!(F32.super_type(&F64), Some(F64));
        assert_eq!(F64.super_type(&F32), Some(F64));
    }

    #[test]
    fn test_super_type_int_float_promotion() {
        assert_eq!(I8.super_type(&F32), Some(F32));
        assert_eq!(I16.super_type(&F32), Some(F32));
        assert_eq!(I32.super_type(&F32), Some(F64));
        assert_eq!(I64.super_type(&F32), Some(F64));
        assert_eq!(I8.super_type(&F64), Some(F64));
        assert_eq!(U8.super_type(&F32), Some(F32));
        assert_eq!(U32.super_type(&F32), Some(F64));
    }

    #[test]
    fn test_super_type_string_absorbs_all() {
        for dt in [
            Bool, I8, I16, I32, I64, U8, U16, U32, U64, F32, F64, Timestamp, Binary,
        ] {
            assert_eq!(dt.super_type(&String), Some(String));
            assert_eq!(String.super_type(&dt), Some(String));
        }
    }

    #[test]
    fn test_super_type_bool_promotes_to_numeric() {
        assert_eq!(Bool.super_type(&I32), Some(I32));
        assert_eq!(Bool.super_type(&F64), Some(F64));
        assert_eq!(Bool.super_type(&U8), Some(U8));
    }

    #[test]
    fn test_super_type_binary_incompatible() {
        assert_eq!(Binary.super_type(&I32), None);
        assert_eq!(Binary.super_type(&F64), None);
        assert_eq!(Binary.super_type(&Bool), None);
        assert_eq!(Binary.super_type(&Timestamp), None);
        // But Binary + String works
        assert_eq!(Binary.super_type(&String), Some(String));
    }

    #[test]
    fn test_super_type_timestamp_numeric() {
        assert_eq!(Timestamp.super_type(&I64), Some(I64));
        assert_eq!(Timestamp.super_type(&I32), Some(I64));
        assert_eq!(Timestamp.super_type(&U64), Some(F64));
        assert_eq!(Timestamp.super_type(&F64), Some(F64));
        assert_eq!(Timestamp.super_type(&F32), Some(F64));
        assert_eq!(Timestamp.super_type(&Bool), Some(I64));
    }
}
