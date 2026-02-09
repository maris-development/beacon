use arrow::{
    array::{ArrayRef, AsArray},
    buffer::{Buffer, OffsetBuffer, ScalarBuffer},
    datatypes::{Int8Type, UInt32Type},
};

use crate::array::data_type::{PrimitiveArrayDataType, VLenByteArrayDataType};

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

    pub fn as_slice(&self) -> &[T::Native] {
        let byte_slice = self.buffer.as_slice();
        let num_elements = byte_slice.len() / std::mem::size_of::<T::Native>();
        unsafe { std::slice::from_raw_parts(byte_slice.as_ptr() as *const T::Native, num_elements) }
    }
}

pub struct VlenArrayBuffer<T>
where
    T: VLenByteArrayDataType + 'static,
{
    pub offsets: OffsetBuffer<i32>, // Buffer containing the offsets for variable-length data.
    pub values: Buffer,             // Buffer containing the concatenated variable-length data.
    pub fill_value: Option<T::OwnedView>, // Optional fill value for missing data (e.g., for nulls).
    _marker: std::marker::PhantomData<T>, // Marker to associate the buffer with the type T.
}

impl<T> VlenArrayBuffer<T>
where
    T: VLenByteArrayDataType + 'static,
{
    pub fn from_arrow(
        offsets: OffsetBuffer<i32>,
        values: Buffer,
        fill_value: Option<T::OwnedView>,
    ) -> Self {
        Self {
            offsets,
            values,
            fill_value,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn as_slice<'a>(&'a self) -> Vec<T::View<'a>> {
        let mut result: Vec<T::View<'a>> = Vec::new();
        let offsets_slice = self.offsets.as_ref();
        let values_slice = self.values.as_slice();

        for i in 0..offsets_slice.len() - 1 {
            let start = offsets_slice[i] as usize;
            let end = offsets_slice[i + 1] as usize;
            let value_bytes = &values_slice[start..end];
            let value: T::View<'a> = T::from_bytes(value_bytes);
            result.push(value);
        }

        result
    }
}
