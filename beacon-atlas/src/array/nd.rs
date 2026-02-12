use arrow::array::ArrayRef;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use ndarray::CowArray;
use std::sync::Arc;

use crate::array::{
    buffer::{PrimitiveArrayBuffer, VlenArrayBuffer},
    data_type::{
        BoolType, BytesType, DataType, F32Type, F64Type, I8Type, I16Type, I32Type, I64Type,
        PrimitiveArrayDataType, StrType, TimestampType, U8Type, U16Type, U32Type, U64Type,
        VLenByteArrayDataType,
    },
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

pub fn try_from_arrow_ref(
    array: ArrayRef,
    shape: &[usize],
    dimensions: &[String],
) -> Option<NdArrayRef> {
    let shape = shape.to_vec();
    let dimensions = dimensions.to_vec();

    fn primitive_from_arrow<T: PrimitiveArrayDataType>(
        array: &ArrayRef,
        shape: &[usize],
        dimensions: &[String],
    ) -> Option<NdArrayRef> {
        let primitive = array
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<T::ArrowNativeType>>()?;
        Some(Arc::new(PrimitiveArray::<T>::from_arrow(
            primitive.clone(),
            shape.to_vec(),
            dimensions.to_vec(),
            None,
        )))
    }

    fn vlen_from_arrow<T: VLenByteArrayDataType>(
        array: arrow::array::BinaryArray,
        shape: &[usize],
        dimensions: &[String],
    ) -> Option<NdArrayRef> {
        Some(Arc::new(VlenByteArray::<T>::from_arrow(
            array,
            shape.to_vec(),
            dimensions.to_vec(),
            None,
        )))
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let boolean_array = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()?;
            let values: Vec<u8> = boolean_array
                .iter()
                .map(|value| value.map(u8::from).unwrap_or(0))
                .collect();
            let uint8_array = arrow::array::UInt8Array::from(values);
            Some(Arc::new(PrimitiveArray::<BoolType>::from_arrow(
                uint8_array,
                shape,
                dimensions,
                None,
            )))
        }
        ArrowDataType::Int8 => primitive_from_arrow::<I8Type>(&array, &shape, &dimensions),
        ArrowDataType::Int16 => primitive_from_arrow::<I16Type>(&array, &shape, &dimensions),
        ArrowDataType::Int32 => primitive_from_arrow::<I32Type>(&array, &shape, &dimensions),
        ArrowDataType::Int64 => primitive_from_arrow::<I64Type>(&array, &shape, &dimensions),
        ArrowDataType::UInt8 => primitive_from_arrow::<U8Type>(&array, &shape, &dimensions),
        ArrowDataType::UInt16 => primitive_from_arrow::<U16Type>(&array, &shape, &dimensions),
        ArrowDataType::UInt32 => primitive_from_arrow::<U32Type>(&array, &shape, &dimensions),
        ArrowDataType::UInt64 => primitive_from_arrow::<U64Type>(&array, &shape, &dimensions),
        ArrowDataType::Float32 => primitive_from_arrow::<F32Type>(&array, &shape, &dimensions),
        ArrowDataType::Float64 => primitive_from_arrow::<F64Type>(&array, &shape, &dimensions),
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => {
            primitive_from_arrow::<TimestampType>(&array, &shape, &dimensions)
        }
        ArrowDataType::Binary => {
            let binary_array = array.as_any().downcast_ref::<arrow::array::BinaryArray>()?;
            vlen_from_arrow::<BytesType>(binary_array.clone(), &shape, &dimensions)
        }
        ArrowDataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<arrow::array::StringArray>()?;
            let binary_array = arrow::array::BinaryArray::from(string_array.clone());
            vlen_from_arrow::<StrType>(binary_array, &shape, &dimensions)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };

    fn dims() -> Vec<String> {
        vec!["x".to_string()]
    }

    #[test]
    fn try_from_arrow_ref_supports_primitive_types() {
        let shape = vec![2usize];
        let dimensions = dims();

        let cases: Vec<(ArrayRef, DataType)> = vec![
            (
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                DataType::Bool,
            ),
            (
                Arc::new(Int8Array::from(vec![1, 2])) as ArrayRef,
                DataType::I8,
            ),
            (
                Arc::new(Int16Array::from(vec![1, 2])) as ArrayRef,
                DataType::I16,
            ),
            (
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                DataType::I32,
            ),
            (
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                DataType::I64,
            ),
            (
                Arc::new(UInt8Array::from(vec![1, 2])) as ArrayRef,
                DataType::U8,
            ),
            (
                Arc::new(UInt16Array::from(vec![1, 2])) as ArrayRef,
                DataType::U16,
            ),
            (
                Arc::new(UInt32Array::from(vec![1, 2])) as ArrayRef,
                DataType::U32,
            ),
            (
                Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef,
                DataType::U64,
            ),
            (
                Arc::new(Float32Array::from(vec![1.0, 2.5])) as ArrayRef,
                DataType::F32,
            ),
            (
                Arc::new(Float64Array::from(vec![1.0, 2.5])) as ArrayRef,
                DataType::F64,
            ),
            (
                Arc::new(TimestampNanosecondArray::from(vec![1i64, 2i64])) as ArrayRef,
                DataType::Timestamp,
            ),
            (
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                DataType::Utf8,
            ),
        ];

        for (array, expected_type) in cases {
            let nd = match try_from_arrow_ref(array.clone(), &shape, &dimensions) {
                Some(nd) => nd,
                None => panic!(
                    "expected array conversion to succeed for {:?}",
                    array.data_type()
                ),
            };
            assert_eq!(nd.data_type(), expected_type);
            assert_eq!(nd.shape(), shape.as_slice());
            assert_eq!(nd.dimensions(), vec!["x"]);
        }
    }
}
