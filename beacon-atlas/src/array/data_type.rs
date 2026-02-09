//! Array datatype definitions.
//!
//! This module defines [`ArrayDataType`], which describes how a typed [`crate::variable::array::Array`]
//! interprets and validates its backing byte buffer.
//!
//! ## Core responsibilities
//! - Define the logical [`crate::dtype::DataType`] for the array.
//! - Specify required byte alignment (`ALIGNMENT`).
//! - Specify fixed element width (`BYTE_WIDTH`) or implement variable-width validation.
//! - Provide `ndarray` views over validated bytes via [`ArrayDataType::as_array`] (and optionally
//!   [`ArrayDataType::as_array_mut`]).
//!
//! ## Validation
//! The default [`ArrayDataType::validate`] implementation checks:
//! - alignment of the byte buffer
//! - exact byte length for fixed-width types
//!
//! Variable-width types (e.g. [`Utf8`]) must override `validate` to enforce their encoding.
//!
//! ## ndarray views
//! Most fixed-width types can return **zero-copy** `ndarray` views by reinterpreting the byte
//! buffer as a slice of native elements.
//!
//! [`Utf8`] is variable-width and returns an `ndarray` backed by a `Vec<&str>` (string *references*
//! into the payload), so it allocates for the index vector even though it does not copy the
//! underlying string bytes.
//!
//!
//!

use arrow::array::ArrowPrimitiveType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
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
    Utf8,
}

pub trait PrimitiveArrayDataType: Send + Sync + 'static {
    /// The element type produced when viewing bytes for a particular lifetime.
    ///
    /// Most scalar types use `T` itself; [`I8`] uses `i8`.
    type Native: Copy + Send + Sync;
    /// The corresponding Arrow primitive type, used for zero-copy conversions.
    type ArrowNativeType: ArrowPrimitiveType;

    const DATA_TYPE: DataType;
}

/// Macro to implement PrimitiveArrayDataType for a primitive type
#[macro_export]
macro_rules! impl_primitive_array_data_type {
    ($type_name:ident, $native:ty, $arrow_native:ty, $data_type:expr) => {
        pub struct $type_name;
        impl PrimitiveArrayDataType for $type_name {
            type Native = $native;
            type ArrowNativeType = $arrow_native;
            const DATA_TYPE: DataType = $data_type;
        }
    };
}

impl_primitive_array_data_type!(BoolType, bool, arrow::datatypes::UInt8Type, DataType::Bool); // Use byte representation for bool to simplify buffer handling.
impl_primitive_array_data_type!(I8Type, i8, arrow::datatypes::Int8Type, DataType::I8);
impl_primitive_array_data_type!(I16Type, i16, arrow::datatypes::Int16Type, DataType::I16);
impl_primitive_array_data_type!(I32Type, i32, arrow::datatypes::Int32Type, DataType::I32);
impl_primitive_array_data_type!(I64Type, i64, arrow::datatypes::Int64Type, DataType::I64);
impl_primitive_array_data_type!(U8Type, u8, arrow::datatypes::UInt8Type, DataType::U8);
impl_primitive_array_data_type!(U16Type, u16, arrow::datatypes::UInt16Type, DataType::U16);
impl_primitive_array_data_type!(U32Type, u32, arrow::datatypes::UInt32Type, DataType::U32);
impl_primitive_array_data_type!(U64Type, u64, arrow::datatypes::UInt64Type, DataType::U64);
impl_primitive_array_data_type!(F32Type, f32, arrow::datatypes::Float32Type, DataType::F32);
impl_primitive_array_data_type!(F64Type, f64, arrow::datatypes::Float64Type, DataType::F64);
impl_primitive_array_data_type!(
    TimestampType,
    i64,
    arrow::datatypes::Time64NanosecondType,
    DataType::Timestamp
);

pub trait VLenByteArrayDataType: Clone + Send + Sync + 'static {
    type View<'a>: Send + Sync
    where
        Self: 'a;
    type OwnedView: Send + Sync + Clone;

    const DATA_TYPE: DataType;

    fn from_bytes<'a>(bytes: &'a [u8]) -> Self::View<'a>;
}

#[derive(Debug, Copy, Clone)]
pub struct BytesView<'a> {
    bytes: &'a [u8],
}

#[derive(Clone)]
pub struct BytesType;

impl VLenByteArrayDataType for BytesType {
    type View<'a> = &'a [u8];
    type OwnedView = Vec<u8>;

    fn from_bytes<'a>(bytes: &'a [u8]) -> Self::View<'a> {
        bytes
    }

    const DATA_TYPE: DataType = DataType::Binary;
}

#[derive(Debug, Copy, Clone)]
pub struct StrType;

impl VLenByteArrayDataType for StrType {
    type View<'a> = &'a str;
    type OwnedView = String;

    const DATA_TYPE: DataType = DataType::Utf8;

    #[inline(always)]
    fn from_bytes<'a>(bytes: &'a [u8]) -> Self::View<'a> {
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }
}
