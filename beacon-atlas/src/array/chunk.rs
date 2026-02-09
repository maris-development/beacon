use ndarray::CowArray;

use crate::array::{
    buffer::{PrimitiveArrayBuffer, VlenArrayBuffer},
    data_type::{DataType, PrimitiveArrayDataType},
};

pub trait ArrayChunk {
    fn start(&self) -> &[usize];
    fn shape(&self) -> &[usize];
    fn chunk_index(&self) -> &[usize];
    fn data_type(&self) -> &DataType;
    fn as_primitive_nd_array<'a, T: PrimitiveArrayDataType>(
        &'a self,
    ) -> Option<CowArray<'a, T::Native, ndarray::IxDyn>>;
    fn as_vlen_byte_nd_array<T: PrimitiveArrayDataType, F: AsRef<T::Native>>(
        &self,
    ) -> Option<CowArray<T::Native, ndarray::IxDyn>>;
}
