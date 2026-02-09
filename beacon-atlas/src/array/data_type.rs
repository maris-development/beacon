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

use arrow::{array::ArrowPrimitiveType, datatypes::ByteArrayType};

use crate::array::buffer::{PrimitiveArrayBuffer, VlenArrayBuffer};

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
    /// Most scalar types use `T` itself; [`Utf8`] uses `&'a str`.
    type Native: Copy + Send + Sync;
    /// The corresponding Arrow primitive type, used for zero-copy conversions.
    type ArrowNativeType: ArrowPrimitiveType;

    // The logical datatype for this array.
    // const TYPE: DataType;

    // fn as_array<'a>(
    //     buffer: &'a PrimitiveArrayBuffer<Self>,
    //     shape: &'a [usize],
    // ) -> ndarray::CowArray<'a, Self::Native, ndarray::IxDyn>;

    // fn from_nd_array(
    //     array: ndarray::ArrayView<Self::Native, ndarray::IxDyn>,
    //     fill_value: Option<Self::Native>,
    // ) -> PrimitiveArrayBuffer<Self>;
}

pub trait VLenByteArrayDataType: Default + Clone + Send + Sync + 'static {
    type Native: Clone + Send + Sync;
    type ArrowNativeType: ByteArrayType;

    const TYPE: DataType;

    fn as_array<'a, F>(
        buffer: &'a VlenArrayBuffer<Self::Native, F>,
        shape: &'a [usize],
    ) -> ndarray::CowArray<'a, Self::Native, ndarray::IxDyn>
    where
        F: AsRef<Self::Native>;

    fn from_nd_array<'a, F>(
        array: ndarray::ArrayView<Self::Native, ndarray::IxDyn>,
        fill_value: Option<Self::Native>,
    ) -> VlenArrayBuffer<Self::Native, F>
    where
        F: AsRef<Self::Native>;
}
