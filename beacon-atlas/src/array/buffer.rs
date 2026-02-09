use arrow::{
    array::{ArrayRef, AsArray},
    buffer::{Buffer, OffsetBuffer, ScalarBuffer},
    datatypes::{Int8Type, UInt32Type},
};

use crate::array::data_type::PrimitiveArrayDataType;

pub struct PrimitiveArrayBuffer<T>
where
    T: PrimitiveArrayDataType,
{
    pub buffer: Buffer, // The raw byte buffer containing the array data.
    pub fill_value: Option<T::Native>, // Optional fill value for missing data (e.g., for nulls).
    _marker: std::marker::PhantomData<T>, // Marker to associate the buffer with the type T.
}

impl<T> PrimitiveArrayBuffer<T>
where
    T: PrimitiveArrayDataType,
{
    pub fn from_arrow(arrow_buffer: Buffer, fill_value: Option<T::Native>) -> Self {
        Self {
            buffer: arrow_buffer,
            fill_value,
            _marker: std::marker::PhantomData,
        }
    }
}

pub struct VlenArrayBuffer<T, F>
where
    F: AsRef<T> + 'static,
{
    pub offsets: OffsetBuffer<i32>, // Buffer containing the offsets for variable-length data.
    pub values: Buffer,             // Buffer containing the concatenated variable-length data.
    pub fill_value: Option<F>,      // Optional fill value for missing data (e.g., for nulls).
    _marker: std::marker::PhantomData<T>, // Marker to associate the buffer with the type T.
}

impl<T, F> VlenArrayBuffer<T, F>
where
    F: AsRef<T> + 'static,
{
    pub fn from_arrow(offsets: OffsetBuffer<i32>, values: Buffer, fill_value: Option<F>) -> Self {
        Self {
            offsets,
            values,
            fill_value,
            _marker: std::marker::PhantomData,
        }
    }
}
