use ndarray::CowArray;
use std::sync::Arc;

use crate::array::{
    buffer::{PrimitiveArrayBuffer, VlenArrayBuffer},
    data_type::{DataType, PrimitiveArrayDataType, VLenByteArrayDataType},
};

pub type NdArrayRef = Arc<dyn NdArray>;

pub trait NdArray: Send + Sync + 'static {
    fn shape(&self) -> &[usize];
    fn as_any(&self) -> &dyn std::any::Any;
    fn data_type(&self) -> DataType;
}

pub trait AsNdArray {
    fn as_primitive_nd<'a, T: PrimitiveArrayDataType>(
        &'a self,
    ) -> Option<CowArray<'a, T::Native, ndarray::IxDyn>>;

    fn as_primitive_fill_value<T: PrimitiveArrayDataType>(&self) -> Option<T::Native>;

    fn as_vlen_byte_nd<'a, T: VLenByteArrayDataType>(
        &'a self,
    ) -> Option<CowArray<'a, T::View<'a>, ndarray::IxDyn>>;

    fn as_vlen_byte_fill_value<T: VLenByteArrayDataType>(&self) -> Option<T::OwnedView>;
}

impl AsNdArray for dyn NdArray {
    fn as_primitive_nd<'a, T: PrimitiveArrayDataType>(
        &'a self,
    ) -> Option<CowArray<'a, T::Native, ndarray::IxDyn>> {
        self.as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .map(|array| array.as_nd_array())
    }

    fn as_primitive_fill_value<T: PrimitiveArrayDataType>(&self) -> Option<T::Native> {
        self.as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .and_then(|array| array.buffer.fill_value)
    }

    fn as_vlen_byte_nd<'a, T: VLenByteArrayDataType>(
        &'a self,
    ) -> Option<CowArray<'a, T::View<'a>, ndarray::IxDyn>> {
        self.as_any()
            .downcast_ref::<VlenByteArray<T>>()
            .map(|array| array.as_nd_array())
    }

    fn as_vlen_byte_fill_value<T: VLenByteArrayDataType>(&self) -> Option<T::OwnedView> {
        self.as_any()
            .downcast_ref::<VlenByteArray<T>>()
            .and_then(|array| array.buffer.fill_value.as_ref().cloned())
    }
}

pub struct PrimitiveArray<T: PrimitiveArrayDataType> {
    pub buffer: PrimitiveArrayBuffer<T>,
    pub shape: Vec<usize>,
}

impl<T: PrimitiveArrayDataType> NdArray for PrimitiveArray<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn data_type(&self) -> DataType {
        T::DATA_TYPE
    }
}

impl<T: PrimitiveArrayDataType> PrimitiveArray<T> {
    pub fn from_arrow(
        arrow_array: arrow::array::PrimitiveArray<T::ArrowNativeType>,
        shape: Vec<usize>,
        fill_value: Option<T::Native>,
    ) -> PrimitiveArray<T> {
        let arrow_buffer = arrow_array.values().inner().clone();
        let buffer = PrimitiveArrayBuffer::<T>::from_arrow(arrow_buffer, fill_value);
        PrimitiveArray { buffer, shape }
    }

    pub fn as_nd_array<'a>(&'a self) -> ndarray::CowArray<'a, T::Native, ndarray::IxDyn> {
        let slice = self.buffer.as_slice();
        let array = ndarray::ArrayView::from_shape(self.shape.as_slice(), slice).unwrap();
        ndarray::CowArray::from(array)
    }
}

pub struct VlenByteArray<T: VLenByteArrayDataType> {
    pub buffer: VlenArrayBuffer<T>,
    pub shape: Vec<usize>,
}

impl<T: VLenByteArrayDataType> NdArray for VlenByteArray<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn data_type(&self) -> DataType {
        T::DATA_TYPE
    }
}

impl<T: VLenByteArrayDataType> VlenByteArray<T> {
    pub fn from_arrow(
        array: arrow::array::GenericBinaryArray<i32>,
        shape: Vec<usize>,
        fill_value: Option<T::OwnedView>,
    ) -> Self {
        let offsets = array.offsets();
        let values = array.values().clone();
        Self {
            buffer: VlenArrayBuffer::from_arrow(offsets.clone(), values, fill_value),
            shape,
        }
    }

    pub fn as_nd_array<'a>(&'a self) -> ndarray::CowArray<'a, T::View<'a>, ndarray::IxDyn> {
        let slice: Vec<T::View<'a>> = self.buffer.as_slice();
        let array = ndarray::ArrayBase::<ndarray::OwnedRepr<T::View<'a>>, _>::from_shape_vec(
            self.shape.as_slice(),
            slice,
        )
        .unwrap();
        ndarray::CowArray::from(array)
    }
}
