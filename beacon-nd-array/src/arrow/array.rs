use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, Scalar, StringArray, TimestampNanosecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array, make_array,
    },
    buffer::NullBuffer,
    compute::kernels::cmp::neq,
};

use crate::{
    NdArray, NdArrayD,
    datatypes::{NdArrayDataType, TimestampNanosecond},
};

/// Converts a boolean or primitive numeric NdArray to an Arrow array,
/// masking fill values as nulls.
macro_rules! convert_ndarray {
    ($ndarray:expr, $rust_ty:ty, $arrow_ty:ty, $label:expr) => {{
        let nd = $ndarray
            .as_any()
            .downcast_ref::<NdArray<$rust_ty>>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Failed to downcast NdArray to NdArray<{}> for Arrow conversion.",
                    $label
                )
            })?;
        let values = nd.clone_into_raw_vec().await;
        let array = <$arrow_ty>::from(values);
        if let Some(fill_value) = nd.fill_value().await {
            let scalar = Scalar::new(Arc::new(<$arrow_ty>::from(vec![fill_value])) as ArrayRef);
            let fill_mask = neq(&array, &scalar)?;
            let null_mask = NullBuffer::new(fill_mask.values().clone());
            return Ok(
                Arc::new(<$arrow_ty>::new(array.values().clone(), Some(null_mask))) as ArrayRef,
            );
        }
        Ok(Arc::new(array) as ArrayRef)
    }};
}

/// Applies a fill-value null mask to an already-built Arrow array via ArrayData reconstruction.
fn apply_fill_mask_via_data(
    array: impl arrow::array::Array,
    null_mask: NullBuffer,
) -> anyhow::Result<ArrayRef> {
    let data = array
        .into_data()
        .into_builder()
        .nulls(Some(null_mask))
        .build()?;
    Ok(make_array(data))
}

pub async fn ndarray_to_arrow_array(ndarray: &dyn NdArrayD) -> anyhow::Result<ArrayRef> {
    match ndarray.datatype() {
        NdArrayDataType::Bool => convert_ndarray!(ndarray, bool, BooleanArray, "bool"),
        NdArrayDataType::I8 => convert_ndarray!(ndarray, i8, Int8Array, "i8"),
        NdArrayDataType::I16 => convert_ndarray!(ndarray, i16, Int16Array, "i16"),
        NdArrayDataType::I32 => convert_ndarray!(ndarray, i32, Int32Array, "i32"),
        NdArrayDataType::I64 => convert_ndarray!(ndarray, i64, Int64Array, "i64"),
        NdArrayDataType::U8 => convert_ndarray!(ndarray, u8, UInt8Array, "u8"),
        NdArrayDataType::U16 => convert_ndarray!(ndarray, u16, UInt16Array, "u16"),
        NdArrayDataType::U32 => convert_ndarray!(ndarray, u32, UInt32Array, "u32"),
        NdArrayDataType::U64 => convert_ndarray!(ndarray, u64, UInt64Array, "u64"),
        NdArrayDataType::F32 => convert_ndarray!(ndarray, f32, Float32Array, "f32"),
        NdArrayDataType::F64 => convert_ndarray!(ndarray, f64, Float64Array, "f64"),
        NdArrayDataType::Timestamp => {
            let nd = ndarray
                .as_any()
                .downcast_ref::<NdArray<TimestampNanosecond>>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Failed to downcast NdArray to NdArray<TimestampNanosecond> for Arrow conversion."
                    )
                })?;
            let values: Vec<i64> = nd
                .clone_into_raw_vec()
                .await
                .into_iter()
                .map(|t| t.0)
                .collect();
            let array = TimestampNanosecondArray::from(values);
            if let Some(fill_value) = nd.fill_value().await {
                let scalar =
                    Scalar::new(
                        Arc::new(TimestampNanosecondArray::from(vec![fill_value.0])) as ArrayRef
                    );
                let fill_mask = neq(&array, &scalar)?;
                let null_mask = NullBuffer::new(fill_mask.values().clone());
                return Ok(Arc::new(TimestampNanosecondArray::new(
                    array.values().clone(),
                    Some(null_mask),
                )) as ArrayRef);
            }
            Ok(Arc::new(array) as ArrayRef)
        }
        NdArrayDataType::String => {
            let nd = ndarray
                .as_any()
                .downcast_ref::<NdArray<String>>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Failed to downcast NdArray to NdArray<String> for Arrow conversion."
                    )
                })?;
            let values = nd.clone_into_raw_vec().await;
            let array = StringArray::from_iter_values(&values);
            if let Some(fill_value) = nd.fill_value().await {
                let scalar =
                    Scalar::new(Arc::new(StringArray::from(vec![fill_value.as_str()])) as ArrayRef);
                let fill_mask = neq(&array, &scalar)?;
                let null_mask = NullBuffer::new(fill_mask.values().clone());
                return apply_fill_mask_via_data(array, null_mask);
            }
            Ok(Arc::new(array) as ArrayRef)
        }
        NdArrayDataType::Binary => {
            let nd = ndarray
                .as_any()
                .downcast_ref::<NdArray<Vec<u8>>>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Failed to downcast NdArray to NdArray<Vec<u8>> for Arrow conversion."
                    )
                })?;
            let values = nd.clone_into_raw_vec().await;
            let array = BinaryArray::from_iter_values(values.iter().map(|v| v.as_slice()));
            if let Some(fill_value) = nd.fill_value().await {
                let scalar = Scalar::new(Arc::new(BinaryArray::from_iter_values(std::iter::once(
                    fill_value.as_slice(),
                ))) as ArrayRef);
                let fill_mask = neq(&array, &scalar)?;
                let null_mask = NullBuffer::new(fill_mask.values().clone());
                return apply_fill_mask_via_data(array, null_mask);
            }
            Ok(Arc::new(array) as ArrayRef)
        }
    }
}

impl From<NdArrayDataType> for arrow::datatypes::DataType {
    fn from(nd_dtype: NdArrayDataType) -> Self {
        match nd_dtype {
            NdArrayDataType::Bool => Self::Boolean,
            NdArrayDataType::I8 => Self::Int8,
            NdArrayDataType::I16 => Self::Int16,
            NdArrayDataType::I32 => Self::Int32,
            NdArrayDataType::I64 => Self::Int64,
            NdArrayDataType::U8 => Self::UInt8,
            NdArrayDataType::U16 => Self::UInt16,
            NdArrayDataType::U32 => Self::UInt32,
            NdArrayDataType::U64 => Self::UInt64,
            NdArrayDataType::F32 => Self::Float32,
            NdArrayDataType::F64 => Self::Float64,
            NdArrayDataType::Timestamp => {
                // For simplicity, we use Timestamp(Nanosecond, None) for all timestamp types.
                // In a full implementation, we might want to preserve the original time unit.
                Self::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
            }
            NdArrayDataType::Binary => Self::Binary,
            NdArrayDataType::String => Self::Utf8,
        }
    }
}

impl TryFrom<arrow::datatypes::DataType> for NdArrayDataType {
    type Error = anyhow::Error;

    fn try_from(arrow_dtype: arrow::datatypes::DataType) -> Result<Self, Self::Error> {
        match arrow_dtype {
            arrow::datatypes::DataType::Boolean => Ok(Self::Bool),
            arrow::datatypes::DataType::Int8 => Ok(Self::I8),
            arrow::datatypes::DataType::Int16 => Ok(Self::I16),
            arrow::datatypes::DataType::Int32 => Ok(Self::I32),
            arrow::datatypes::DataType::Int64 => Ok(Self::I64),
            arrow::datatypes::DataType::UInt8 => Ok(Self::U8),
            arrow::datatypes::DataType::UInt16 => Ok(Self::U16),
            arrow::datatypes::DataType::UInt32 => Ok(Self::U32),
            arrow::datatypes::DataType::UInt64 => Ok(Self::U64),
            arrow::datatypes::DataType::Float32 => Ok(Self::F32),
            arrow::datatypes::DataType::Float64 => Ok(Self::F64),
            arrow::datatypes::DataType::Timestamp(arrow_time_unit, _) => {
                if let arrow::datatypes::TimeUnit::Nanosecond = arrow_time_unit {
                    Ok(Self::Timestamp)
                } else {
                    Err(anyhow::anyhow!(
                        "Unsupported time unit in Arrow Timestamp: {:?}",
                        arrow_time_unit
                    ))
                }
            }
            arrow::datatypes::DataType::Binary => Ok(Self::Binary),
            arrow::datatypes::DataType::Utf8 => Ok(Self::String),
            other => Err(anyhow::anyhow!("Unsupported Arrow data type: {:?}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NdArray;
    use crate::datatypes::TimestampNanosecond;
    use arrow::array::Array;

    fn make_nd<T: crate::datatypes::NdArrayType>(
        values: Vec<T>,
        fill_value: Option<T>,
    ) -> NdArray<T> {
        NdArray::try_new_from_vec_in_mem(values, vec![3], vec!["x".to_string()], fill_value)
            .unwrap()
    }

    // ---- Bool ----

    #[tokio::test]
    async fn test_bool_no_fill() {
        let nd = make_nd(vec![true, false, true], None);
        let arr = ndarray_to_arrow_array(&nd).await.unwrap();
        let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(ba.len(), 3);
        assert_eq!(ba.null_count(), 0);
        assert!(ba.value(0));
        assert!(!ba.value(1));
    }

    #[tokio::test]
    async fn test_bool_with_fill() {
        let nd = make_nd(vec![true, false, false], Some(false));
        let arr = ndarray_to_arrow_array(&nd).await.unwrap();
        let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(ba.len(), 3);
        assert!(ba.is_valid(0));
        assert!(ba.is_null(1));
        assert!(ba.is_null(2));
    }

    // ---- Integer types ----

    macro_rules! test_primitive {
        ($name_no_fill:ident, $name_fill:ident, $rust_ty:ty, $arrow_ty:ty, $vals:expr, $fill:expr, $null_idx:expr) => {
            #[tokio::test]
            async fn $name_no_fill() {
                let nd = make_nd::<$rust_ty>($vals, None);
                let arr = ndarray_to_arrow_array(&nd).await.unwrap();
                let pa = arr.as_any().downcast_ref::<$arrow_ty>().unwrap();
                assert_eq!(pa.len(), 3);
                assert_eq!(pa.null_count(), 0);
            }

            #[tokio::test]
            async fn $name_fill() {
                let vals: Vec<$rust_ty> = $vals;
                let nd = make_nd(vals, Some($fill));
                let arr = ndarray_to_arrow_array(&nd).await.unwrap();
                let pa = arr.as_any().downcast_ref::<$arrow_ty>().unwrap();
                assert_eq!(pa.len(), 3);
                assert!(pa.is_null($null_idx));
                for i in 0..3 {
                    if i != $null_idx {
                        assert!(pa.is_valid(i));
                    }
                }
            }
        };
    }

    test_primitive!(
        test_i8_no_fill,
        test_i8_fill,
        i8,
        Int8Array,
        vec![1, -99, 3],
        -99i8,
        1
    );
    test_primitive!(
        test_i16_no_fill,
        test_i16_fill,
        i16,
        Int16Array,
        vec![1, -99, 3],
        -99i16,
        1
    );
    test_primitive!(
        test_i32_no_fill,
        test_i32_fill,
        i32,
        Int32Array,
        vec![1, -99, 3],
        -99i32,
        1
    );
    test_primitive!(
        test_i64_no_fill,
        test_i64_fill,
        i64,
        Int64Array,
        vec![1, -99, 3],
        -99i64,
        1
    );
    test_primitive!(
        test_u8_no_fill,
        test_u8_fill,
        u8,
        UInt8Array,
        vec![1, 255, 3],
        255u8,
        1
    );
    test_primitive!(
        test_u16_no_fill,
        test_u16_fill,
        u16,
        UInt16Array,
        vec![1, 9999, 3],
        9999u16,
        1
    );
    test_primitive!(
        test_u32_no_fill,
        test_u32_fill,
        u32,
        UInt32Array,
        vec![1, 9999, 3],
        9999u32,
        1
    );
    test_primitive!(
        test_u64_no_fill,
        test_u64_fill,
        u64,
        UInt64Array,
        vec![1, 9999, 3],
        9999u64,
        1
    );
    test_primitive!(
        test_f32_no_fill,
        test_f32_fill,
        f32,
        Float32Array,
        vec![1.0, -9999.0, 3.0],
        -9999.0f32,
        1
    );
    test_primitive!(
        test_f64_no_fill,
        test_f64_fill,
        f64,
        Float64Array,
        vec![1.0, -9999.0, 3.0],
        -9999.0f64,
        1
    );

    // ---- Timestamp ----

    #[tokio::test]
    async fn test_timestamp_no_fill() {
        let vals = vec![
            TimestampNanosecond(1000),
            TimestampNanosecond(2000),
            TimestampNanosecond(3000),
        ];
        let nd = make_nd(vals, None);
        let arr = ndarray_to_arrow_array(&nd).await.unwrap();
        let ta = arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ta.len(), 3);
        assert_eq!(ta.null_count(), 0);
        assert_eq!(ta.value(0), 1000);
        assert_eq!(ta.value(2), 3000);
    }

    #[tokio::test]
    async fn test_timestamp_with_fill() {
        let fill = TimestampNanosecond(-9999);
        let vals = vec![TimestampNanosecond(1000), fill, TimestampNanosecond(3000)];
        let nd = make_nd(vals, Some(fill));
        let arr = ndarray_to_arrow_array(&nd).await.unwrap();
        let ta = arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ta.len(), 3);
        assert!(ta.is_valid(0));
        assert!(ta.is_null(1));
        assert!(ta.is_valid(2));
    }

    // ---- String ----

    #[tokio::test]
    async fn test_string_no_fill() {
        let nd = make_nd(
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            None,
        );
        let arr = ndarray_to_arrow_array(&nd).await.unwrap();
        let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(sa.len(), 3);
        assert_eq!(sa.null_count(), 0);
        assert_eq!(sa.value(0), "a");
    }

    #[tokio::test]
    async fn test_string_with_fill() {
        let nd = make_nd(
            vec!["a".to_string(), "".to_string(), "c".to_string()],
            Some("".to_string()),
        );
        let arr = ndarray_to_arrow_array(&nd).await.unwrap();
        let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(sa.len(), 3);
        assert!(sa.is_valid(0));
        assert!(sa.is_null(1));
        assert!(sa.is_valid(2));
    }

    // ---- Binary ----

    #[tokio::test]
    async fn test_binary_no_fill() {
        let nd = make_nd(vec![vec![1u8, 2], vec![3, 4], vec![5, 6]], None);
        let arr = ndarray_to_arrow_array(&nd).await.unwrap();
        let ba = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(ba.len(), 3);
        assert_eq!(ba.null_count(), 0);
        assert_eq!(ba.value(0), &[1u8, 2]);
    }

    #[tokio::test]
    async fn test_binary_with_fill() {
        let fill = vec![0xFFu8, 0xFF];
        let nd = make_nd(vec![vec![1u8, 2], fill.clone(), vec![5, 6]], Some(fill));
        let arr = ndarray_to_arrow_array(&nd).await.unwrap();
        let ba = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(ba.len(), 3);
        assert!(ba.is_valid(0));
        assert!(ba.is_null(1));
        assert!(ba.is_valid(2));
    }
}
