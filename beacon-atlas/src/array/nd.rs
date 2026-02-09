use std::sync::Arc;

use ndarray::CowArray;

use crate::array::{
    buffer::{PrimitiveArrayBuffer, VlenArrayBuffer},
    data_type::{DataType, PrimitiveArrayDataType, VLenByteArrayDataType},
};

pub type NdArrayRef = Arc<dyn NdArray>;

pub fn try_from_arrow(
    array: arrow::array::ArrayRef,
    shape: Vec<usize>,
) -> anyhow::Result<NdArrayRef> {
    match array.data_type() {
        _ => anyhow::bail!("Unsupported Arrow data type: {:?}", array.data_type()),
    }
}

pub trait NdArray: Send + Sync + 'static {
    fn shape(&self) -> &[usize];
    fn as_any(&self) -> &dyn std::any::Any;
    // fn dimensions(&self) -> Vec<Dimension>;
    fn as_primitive_nd<'a, T: PrimitiveArrayDataType>(
        &'a self,
    ) -> Option<CowArray<'a, T::Native, ndarray::IxDyn>>;
    // fn primitive_fill_value<T: PrimitiveArrayDataType>(&self) -> Option<T::Native> {
    //     None
    // }
    // fn as_vlen_byte_nd<'a, T: VLenByteArrayDataType, F: AsRef<T::Native> + 'static>(
    //     &'a self,
    // ) -> Option<CowArray<'a, T::Native, ndarray::IxDyn>> {
    //     None
    // }
    // fn vlen_byte_fill_value<T: VLenByteArrayDataType, F: AsRef<T::Native> + 'static>(
    //     &self,
    // ) -> Option<T::Native> {
    //     None
    // }
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

    fn as_primitive_nd<'a, U: PrimitiveArrayDataType>(
        &'a self,
    ) -> Option<CowArray<'a, U::Native, ndarray::IxDyn>> {
        todo!()
    }
    // fn primitive_fill_value<U: PrimitiveArrayDataType>(&self) -> Option<U::Native> {
    //     self.as_any()
    //         .downcast_ref::<PrimitiveArray<U>>()
    //         .and_then(|array| array.buffer.fill_value)
    // }
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
}

// pub struct VlenByteArray<T: VLenByteArrayDataType, F: AsRef<T::Native>>
// where
//     F: 'static,
// {
//     pub buffer: VlenArrayBuffer<T::Native, F>,
//     pub shape: Vec<usize>,
// }

// impl<T: VLenByteArrayDataType, F: AsRef<T::Native> + Send + Sync> NdArray for VlenByteArray<T, F>
// where
//     F: 'static,
// {
//     fn shape(&self) -> &[usize] {
//         &self.shape
//     }

//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }

//     fn as_vlen_byte_nd<'a, U: VLenByteArrayDataType, G: AsRef<U::Native> + 'static>(
//         &'a self,
//     ) -> Option<CowArray<'a, U::Native, ndarray::IxDyn>> {
//         self.as_any()
//             .downcast_ref::<VlenByteArray<U, G>>()
//             .map(|array| U::as_array(&array.buffer, &array.shape))
//     }

//     fn vlen_byte_fill_value<U: VLenByteArrayDataType, G: AsRef<U::Native> + 'static>(
//         &self,
//     ) -> Option<U::Native> {
//         self.as_any()
//             .downcast_ref::<VlenByteArray<U, G>>()
//             .and_then(|array| array.buffer.fill_value.as_ref().map(|v| v.as_ref().clone()))
//     }
// }

// impl<T: VLenByteArrayDataType, F: AsRef<T::Native> + 'static> VlenByteArray<T, F> {
//     pub fn from_arrow(
//         array: arrow::array::GenericBinaryArray<i32>,
//         shape: Vec<usize>,
//         fill_value: Option<F>,
//     ) -> Self {
//         let offsets = array.offsets();
//         let values = array.values().clone();
//         Self {
//             buffer: VlenArrayBuffer::from_arrow(offsets.clone(), values, fill_value),
//             shape,
//         }
//     }
// }
