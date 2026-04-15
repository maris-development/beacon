use beacon_nd_arrow::datatypes::TimestampNanosecond;

use crate::{scalar::Scalar, schema::_type::AtlasDataType};

/// A strongly-typed columnar vector that stores values for a single Atlas data type.
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
#[rkyv(attr(derive(Debug)))]
pub enum TypedVec {
    Bool(Vec<bool>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Timestamp(Vec<TimestampNanosecond>),
    Binary(Vec<Vec<u8>>),
    String(Vec<String>),
}

impl TypedVec {
    pub fn new(data_type: AtlasDataType) -> Self {
        match data_type {
            AtlasDataType::Bool => TypedVec::Bool(Vec::new()),
            AtlasDataType::I8 => TypedVec::I8(Vec::new()),
            AtlasDataType::I16 => TypedVec::I16(Vec::new()),
            AtlasDataType::I32 => TypedVec::I32(Vec::new()),
            AtlasDataType::I64 => TypedVec::I64(Vec::new()),
            AtlasDataType::U8 => TypedVec::U8(Vec::new()),
            AtlasDataType::U16 => TypedVec::U16(Vec::new()),
            AtlasDataType::U32 => TypedVec::U32(Vec::new()),
            AtlasDataType::U64 => TypedVec::U64(Vec::new()),
            AtlasDataType::F32 => TypedVec::F32(Vec::new()),
            AtlasDataType::F64 => TypedVec::F64(Vec::new()),
            AtlasDataType::Timestamp => TypedVec::Timestamp(Vec::new()),
            AtlasDataType::Binary => TypedVec::Binary(Vec::new()),
            AtlasDataType::String => TypedVec::String(Vec::new()),
        }
    }

    /// Returns the logical Atlas data type represented by this vector.
    pub fn data_type(&self) -> AtlasDataType {
        match self {
            TypedVec::Bool(_) => AtlasDataType::Bool,
            TypedVec::I8(_) => AtlasDataType::I8,
            TypedVec::I16(_) => AtlasDataType::I16,
            TypedVec::I32(_) => AtlasDataType::I32,
            TypedVec::I64(_) => AtlasDataType::I64,
            TypedVec::U8(_) => AtlasDataType::U8,
            TypedVec::U16(_) => AtlasDataType::U16,
            TypedVec::U32(_) => AtlasDataType::U32,
            TypedVec::U64(_) => AtlasDataType::U64,
            TypedVec::F32(_) => AtlasDataType::F32,
            TypedVec::F64(_) => AtlasDataType::F64,
            TypedVec::Timestamp(_) => AtlasDataType::Timestamp,
            TypedVec::Binary(_) => AtlasDataType::Binary,
            TypedVec::String(_) => AtlasDataType::String,
        }
    }

    /// Appends values from another typed vector of the same variant.
    ///
    /// Returns an error if `other` has a different [`AtlasDataType`].
    pub fn try_append(&mut self, other: impl Into<TypedVec>) -> anyhow::Result<()> {
        let other = other.into();
        if self.data_type() != other.data_type() {
            anyhow::bail!(
                "Cannot append TypedVec of type {:?} to TypedVec of type {:?}",
                other.data_type(),
                self.data_type()
            );
        }

        match (self, other) {
            (TypedVec::Bool(vec1), TypedVec::Bool(vec2)) => vec1.extend(vec2),
            (TypedVec::I8(vec1), TypedVec::I8(vec2)) => vec1.extend(vec2),
            (TypedVec::I16(vec1), TypedVec::I16(vec2)) => vec1.extend(vec2),
            (TypedVec::I32(vec1), TypedVec::I32(vec2)) => vec1.extend(vec2),
            (TypedVec::I64(vec1), TypedVec::I64(vec2)) => vec1.extend(vec2),
            (TypedVec::U8(vec1), TypedVec::U8(vec2)) => vec1.extend(vec2),
            (TypedVec::U16(vec1), TypedVec::U16(vec2)) => vec1.extend(vec2),
            (TypedVec::U32(vec1), TypedVec::U32(vec2)) => vec1.extend(vec2),
            (TypedVec::U64(vec1), TypedVec::U64(vec2)) => vec1.extend(vec2),
            (TypedVec::F32(vec1), TypedVec::F32(vec2)) => vec1.extend(vec2),
            (TypedVec::F64(vec1), TypedVec::F64(vec2)) => vec1.extend(vec2),
            (TypedVec::Timestamp(vec1), TypedVec::Timestamp(vec2)) => vec1.extend(vec2),
            (TypedVec::Binary(vec1), TypedVec::Binary(vec2)) => vec1.extend(vec2),
            (TypedVec::String(vec1), TypedVec::String(vec2)) => vec1.extend(vec2),
            _ => unreachable!(),
        }
        Ok(())
    }

    /// Appends a single scalar value when the scalar type matches this vector type.
    ///
    /// Returns an error if the scalar has a different [`AtlasDataType`].
    pub fn try_append_scalar(&mut self, scalar: impl Into<Scalar>) -> anyhow::Result<()> {
        let scalar = scalar.into();
        if self.data_type() != scalar.data_type() {
            anyhow::bail!(
                "Cannot append Scalar of type {:?} to TypedVec of type {:?}",
                scalar.data_type(),
                self.data_type()
            );
        }

        match (self, scalar) {
            (TypedVec::Bool(vec), Scalar::Bool(b)) => vec.push(b),
            (TypedVec::I8(vec), Scalar::I8(i)) => vec.push(i),
            (TypedVec::I16(vec), Scalar::I16(i)) => vec.push(i),
            (TypedVec::I32(vec), Scalar::I32(i)) => vec.push(i),
            (TypedVec::I64(vec), Scalar::I64(i)) => vec.push(i),
            (TypedVec::U8(vec), Scalar::U8(u)) => vec.push(u),
            (TypedVec::U16(vec), Scalar::U16(u)) => vec.push(u),
            (TypedVec::U32(vec), Scalar::U32(u)) => vec.push(u),
            (TypedVec::U64(vec), Scalar::U64(u)) => vec.push(u),
            (TypedVec::F32(vec), Scalar::F32(f)) => vec.push(f),
            (TypedVec::F64(vec), Scalar::F64(f)) => vec.push(f),
            (TypedVec::Timestamp(vec), Scalar::Timestamp(t)) => vec.push(t),
            (TypedVec::Binary(vec), Scalar::Binary(b)) => vec.push(b),
            (TypedVec::String(vec), Scalar::String(s)) => vec.push(s),
            _ => unreachable!(),
        }
        Ok(())
    }

    /// Returns the number of elements currently stored in the vector.
    pub fn len(&self) -> usize {
        match self {
            TypedVec::Bool(vec) => vec.len(),
            TypedVec::I8(vec) => vec.len(),
            TypedVec::I16(vec) => vec.len(),
            TypedVec::I32(vec) => vec.len(),
            TypedVec::I64(vec) => vec.len(),
            TypedVec::U8(vec) => vec.len(),
            TypedVec::U16(vec) => vec.len(),
            TypedVec::U32(vec) => vec.len(),
            TypedVec::U64(vec) => vec.len(),
            TypedVec::F32(vec) => vec.len(),
            TypedVec::F64(vec) => vec.len(),
            TypedVec::Timestamp(vec) => vec.len(),
            TypedVec::Binary(vec) => vec.len(),
            TypedVec::String(vec) => vec.len(),
        }
    }

    /// Returns `true` when the vector has no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a reference to the underlying `Vec<bool>` if this is a boolean vector.
    pub fn as_bool_vec(&self) -> Option<&Vec<bool>> {
        if let TypedVec::Bool(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<i8>` if this is an i8 vector.
    pub fn as_i8_vec(&self) -> Option<&Vec<i8>> {
        if let TypedVec::I8(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<i16>` if this is an i16 vector.
    pub fn as_i16_vec(&self) -> Option<&Vec<i16>> {
        if let TypedVec::I16(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<i32>` if this is an i32 vector.
    pub fn as_i32_vec(&self) -> Option<&Vec<i32>> {
        if let TypedVec::I32(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<i64>` if this is an i64 vector.
    pub fn as_i64_vec(&self) -> Option<&Vec<i64>> {
        if let TypedVec::I64(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<u8>` if this is a u8 vector.
    pub fn as_u8_vec(&self) -> Option<&Vec<u8>> {
        if let TypedVec::U8(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<u16>` if this is a u16 vector.
    pub fn as_u16_vec(&self) -> Option<&Vec<u16>> {
        if let TypedVec::U16(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<u32>` if this is a u32 vector.
    pub fn as_u32_vec(&self) -> Option<&Vec<u32>> {
        if let TypedVec::U32(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<u64>` if this is a u64 vector.
    pub fn as_u64_vec(&self) -> Option<&Vec<u64>> {
        if let TypedVec::U64(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<f32>` if this is an f32 vector.
    pub fn as_f32_vec(&self) -> Option<&Vec<f32>> {
        if let TypedVec::F32(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<f64>` if this is an f64 vector.
    pub fn as_f64_vec(&self) -> Option<&Vec<f64>> {
        if let TypedVec::F64(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<TimestampNanosecond>` if this is a timestamp vector.
    pub fn as_timestamp_vec(&self) -> Option<&Vec<TimestampNanosecond>> {
        if let TypedVec::Timestamp(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<Vec<u8>>` if this is a binary vector.
    pub fn as_binary_vec(&self) -> Option<&Vec<Vec<u8>>> {
        if let TypedVec::Binary(vec) = self {
            Some(vec)
        } else {
            None
        }
    }

    /// Returns a reference to the underlying `Vec<String>` if this is a string vector.
    pub fn as_string_vec(&self) -> Option<&Vec<String>> {
        if let TypedVec::String(vec) = self {
            Some(vec)
        } else {
            None
        }
    }
}

impl From<Vec<bool>> for TypedVec {
    fn from(vec: Vec<bool>) -> Self {
        TypedVec::Bool(vec)
    }
}

impl From<Vec<i8>> for TypedVec {
    fn from(vec: Vec<i8>) -> Self {
        TypedVec::I8(vec)
    }
}

impl From<Vec<i16>> for TypedVec {
    fn from(vec: Vec<i16>) -> Self {
        TypedVec::I16(vec)
    }
}

impl From<Vec<i32>> for TypedVec {
    fn from(vec: Vec<i32>) -> Self {
        TypedVec::I32(vec)
    }
}

impl From<Vec<i64>> for TypedVec {
    fn from(vec: Vec<i64>) -> Self {
        TypedVec::I64(vec)
    }
}

impl From<Vec<u8>> for TypedVec {
    fn from(vec: Vec<u8>) -> Self {
        TypedVec::U8(vec)
    }
}

impl From<Vec<u16>> for TypedVec {
    fn from(vec: Vec<u16>) -> Self {
        TypedVec::U16(vec)
    }
}

impl From<Vec<u32>> for TypedVec {
    fn from(vec: Vec<u32>) -> Self {
        TypedVec::U32(vec)
    }
}

impl From<Vec<u64>> for TypedVec {
    fn from(vec: Vec<u64>) -> Self {
        TypedVec::U64(vec)
    }
}

impl From<Vec<f32>> for TypedVec {
    fn from(vec: Vec<f32>) -> Self {
        TypedVec::F32(vec)
    }
}

impl From<Vec<f64>> for TypedVec {
    fn from(vec: Vec<f64>) -> Self {
        TypedVec::F64(vec)
    }
}

impl From<Vec<TimestampNanosecond>> for TypedVec {
    fn from(vec: Vec<TimestampNanosecond>) -> Self {
        TypedVec::Timestamp(vec)
    }
}

impl From<Vec<Vec<u8>>> for TypedVec {
    fn from(vec: Vec<Vec<u8>>) -> Self {
        TypedVec::Binary(vec)
    }
}

impl From<Vec<String>> for TypedVec {
    fn from(vec: Vec<String>) -> Self {
        TypedVec::String(vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_returns_expected_variant() {
        assert_eq!(TypedVec::Bool(vec![true]).data_type(), AtlasDataType::Bool);
        assert_eq!(TypedVec::I8(vec![1]).data_type(), AtlasDataType::I8);
        assert_eq!(TypedVec::I16(vec![1]).data_type(), AtlasDataType::I16);
        assert_eq!(TypedVec::I32(vec![1]).data_type(), AtlasDataType::I32);
        assert_eq!(TypedVec::I64(vec![1]).data_type(), AtlasDataType::I64);
        assert_eq!(TypedVec::U8(vec![1]).data_type(), AtlasDataType::U8);
        assert_eq!(TypedVec::U16(vec![1]).data_type(), AtlasDataType::U16);
        assert_eq!(TypedVec::U32(vec![1]).data_type(), AtlasDataType::U32);
        assert_eq!(TypedVec::U64(vec![1]).data_type(), AtlasDataType::U64);
        assert_eq!(TypedVec::F32(vec![1.0]).data_type(), AtlasDataType::F32);
        assert_eq!(TypedVec::F64(vec![1.0]).data_type(), AtlasDataType::F64);
        assert_eq!(
            TypedVec::Timestamp(vec![TimestampNanosecond(1)]).data_type(),
            AtlasDataType::Timestamp
        );
        assert_eq!(
            TypedVec::Binary(vec![vec![1]]).data_type(),
            AtlasDataType::Binary
        );
        assert_eq!(
            TypedVec::String(vec!["a".to_string()]).data_type(),
            AtlasDataType::String
        );
    }

    #[test]
    fn try_append_appends_same_type() {
        let mut values = TypedVec::I32(vec![1, 2]);

        values.try_append(vec![3, 4]).unwrap();

        assert_eq!(values.as_i32_vec(), Some(&vec![1, 2, 3, 4]));
    }

    #[test]
    fn try_append_rejects_mismatched_type() {
        let mut values = TypedVec::Bool(vec![true]);

        let err = values.try_append(vec![1_i32]).unwrap_err();

        assert!(
            err.to_string()
                .contains("Cannot append TypedVec of type I32 to TypedVec of type Bool")
        );
    }

    #[test]
    fn try_append_scalar_appends_same_type() {
        let mut values = TypedVec::String(vec!["a".to_string()]);

        values.try_append_scalar("b").unwrap();

        assert_eq!(
            values.as_string_vec(),
            Some(&vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn try_append_scalar_rejects_mismatched_type() {
        let mut values = TypedVec::U64(vec![1]);

        let err = values.try_append_scalar(1_i64).unwrap_err();

        assert!(
            err.to_string()
                .contains("Cannot append Scalar of type I64 to TypedVec of type U64")
        );
    }

    #[test]
    fn len_and_is_empty_reflect_content() {
        let empty = TypedVec::Binary(vec![]);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());

        let non_empty = TypedVec::Binary(vec![vec![1], vec![2]]);
        assert_eq!(non_empty.len(), 2);
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn accessor_returns_none_for_mismatched_variant() {
        let values = TypedVec::U8(vec![1, 2, 3]);

        assert_eq!(values.as_u8_vec(), Some(&vec![1, 2, 3]));
        assert_eq!(values.as_i8_vec(), None);
        assert_eq!(values.as_string_vec(), None);
    }
}
