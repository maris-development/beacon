use ndarray::CowArray;
use std::sync::Arc;

use crate::array::{
    buffer::{PrimitiveArrayBuffer, VlenArrayBuffer},
    data_type::{DataType, PrimitiveArrayDataType, VLenByteArrayDataType},
};

pub type NdArrayRef = Arc<dyn NdArray>;

pub trait NdArray: Send + Sync + 'static {
    fn shape(&self) -> &[usize];
    fn dimensions(&self) -> Vec<&str>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn data_type(&self) -> DataType;
    fn as_arrow_array(&self) -> arrow::array::ArrayRef;
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
    pub arrow_array: arrow::array::PrimitiveArray<T::ArrowNativeType>,
    pub buffer: PrimitiveArrayBuffer<T>,
    pub shape: Vec<usize>,
    pub dimensions: Vec<String>,
}

impl<T: PrimitiveArrayDataType> NdArray for PrimitiveArray<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn dimensions(&self) -> Vec<&str> {
        self.dimensions.iter().map(|s| s.as_str()).collect()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn data_type(&self) -> DataType {
        T::DATA_TYPE
    }
    fn as_arrow_array(&self) -> arrow::array::ArrayRef {
        Arc::new(self.arrow_array.clone())
    }
}

impl<T: PrimitiveArrayDataType> PrimitiveArray<T> {
    pub fn from_arrow(
        arrow_array: arrow::array::PrimitiveArray<T::ArrowNativeType>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        fill_value: Option<T::Native>,
    ) -> PrimitiveArray<T> {
        let arrow_buffer = arrow_array.values().inner().clone();
        let buffer = PrimitiveArrayBuffer::<T>::from_arrow(arrow_buffer, fill_value);
        PrimitiveArray {
            arrow_array,
            buffer,
            shape,
            dimensions,
        }
    }

    pub fn as_nd_array<'a>(&'a self) -> ndarray::CowArray<'a, T::Native, ndarray::IxDyn> {
        let slice = self.buffer.as_slice();
        let array = ndarray::ArrayView::from_shape(self.shape.as_slice(), slice).unwrap();
        ndarray::CowArray::from(array)
    }
}

pub struct VlenByteArray<T: VLenByteArrayDataType> {
    pub arrow_array: arrow::array::GenericBinaryArray<i32>,
    pub buffer: VlenArrayBuffer<T>,
    pub shape: Vec<usize>,
    pub dimensions: Vec<String>,
}

impl<T: VLenByteArrayDataType> NdArray for VlenByteArray<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn dimensions(&self) -> Vec<&str> {
        self.dimensions.iter().map(|s| s.as_str()).collect()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn data_type(&self) -> DataType {
        T::DATA_TYPE
    }

    fn as_arrow_array(&self) -> arrow::array::ArrayRef {
        Arc::new(self.arrow_array.clone())
    }
}

impl<T: VLenByteArrayDataType> VlenByteArray<T> {
    pub fn from_arrow(
        array: arrow::array::GenericBinaryArray<i32>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        fill_value: Option<T::OwnedView>,
    ) -> Self {
        let offsets = array.offsets();
        let values = array.values().clone();

        Self {
            buffer: VlenArrayBuffer::from_arrow(offsets.clone(), values, fill_value),
            shape,
            dimensions,
            arrow_array: array,
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
