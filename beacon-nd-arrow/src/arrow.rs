use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, Scalar, StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};

use crate::{
    NdArray, NdArrayD,
    datatypes::{NdArrayDataType, TimestampNanosecond},
};

#[async_trait::async_trait]
pub trait NdArrowArray: NdArrayD {
    async fn copy_into_raw_arrow(&self) -> anyhow::Result<arrow::array::ArrayRef>;
    async fn fill_value(&self) -> Option<arrow::array::Scalar<ArrayRef>>;
}

#[async_trait::async_trait]
impl NdArrowArray for dyn NdArrayD {
    async fn copy_into_raw_arrow(&self) -> anyhow::Result<arrow::array::ArrayRef> {
        match self.datatype() {
            NdArrayDataType::Bool => {
                todo!()
            }
            NdArrayDataType::I8 => todo!(),
            NdArrayDataType::I16 => todo!(),
            NdArrayDataType::I32 => todo!(),
            NdArrayDataType::I64 => todo!(),
            NdArrayDataType::U8 => todo!(),
            NdArrayDataType::U16 => todo!(),
            NdArrayDataType::U32 => todo!(),
            NdArrayDataType::U64 => todo!(),
            NdArrayDataType::F32 => todo!(),
            NdArrayDataType::F64 => todo!(),
            NdArrayDataType::Timestamp => todo!(),
            NdArrayDataType::Binary => todo!(),
            NdArrayDataType::String => todo!(),
        }
    }
    async fn fill_value(&self) -> Option<arrow::array::Scalar<ArrayRef>> {
        match self.datatype() {
            NdArrayDataType::Bool => {
                let nd_bool = self
                    .as_any()
                    .downcast_ref::<NdArray<bool>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(BooleanArray::from(vec![nd_bool]))))
            }
            NdArrayDataType::I8 => {
                let nd_i8 = self
                    .as_any()
                    .downcast_ref::<NdArray<i8>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(Int8Array::from(vec![nd_i8]))))
            }
            NdArrayDataType::I16 => {
                let nd_i16 = self
                    .as_any()
                    .downcast_ref::<NdArray<i16>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(Int16Array::from(vec![nd_i16]))))
            }
            NdArrayDataType::I32 => {
                let nd_i32 = self
                    .as_any()
                    .downcast_ref::<NdArray<i32>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(Int32Array::from(vec![nd_i32]))))
            }
            NdArrayDataType::I64 => {
                let nd_i64 = self
                    .as_any()
                    .downcast_ref::<NdArray<i64>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(Int64Array::from(vec![nd_i64]))))
            }
            NdArrayDataType::U8 => {
                let nd_u8 = self
                    .as_any()
                    .downcast_ref::<NdArray<u8>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(UInt8Array::from(vec![nd_u8]))))
            }
            NdArrayDataType::U16 => {
                let nd_u16 = self
                    .as_any()
                    .downcast_ref::<NdArray<u16>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(UInt16Array::from(vec![nd_u16]))))
            }
            NdArrayDataType::U32 => {
                let nd_u32 = self
                    .as_any()
                    .downcast_ref::<NdArray<u32>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(UInt32Array::from(vec![nd_u32]))))
            }
            NdArrayDataType::U64 => {
                let nd_u64 = self
                    .as_any()
                    .downcast_ref::<NdArray<u64>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(UInt64Array::from(vec![nd_u64]))))
            }
            NdArrayDataType::F32 => {
                let nd_f32 = self
                    .as_any()
                    .downcast_ref::<NdArray<f32>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(Float32Array::from(vec![nd_f32]))))
            }
            NdArrayDataType::F64 => {
                let nd_f64 = self
                    .as_any()
                    .downcast_ref::<NdArray<f64>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(Float64Array::from(vec![nd_f64]))))
            }
            NdArrayDataType::Timestamp => {
                let nd_timestamp = self
                    .as_any()
                    .downcast_ref::<NdArray<TimestampNanosecond>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(TimestampNanosecondArray::from(vec![
                    nd_timestamp.0,
                ]))))
            }
            NdArrayDataType::Binary => {
                let nd_binary = self
                    .as_any()
                    .downcast_ref::<NdArray<Vec<u8>>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(BinaryArray::from_iter_values(vec![
                    nd_binary,
                ]))))
            }
            NdArrayDataType::String => {
                let nd_string = self
                    .as_any()
                    .downcast_ref::<NdArray<String>>()
                    .map(|nd| nd.fill_value())?
                    .await?;

                Some(Scalar::new(Arc::new(StringArray::from(vec![nd_string]))))
            }
        }
    }
}
