use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};

use crate::{
    NdArray, NdArrayD,
    datatypes::{NdArrayDataType, TimestampNanosecond},
};

pub fn value_range_impl<T: PartialOrd + Copy>(array: &[T]) -> Option<(T, T)> {
    if array.is_empty() {
        return None;
    }

    let mut min = array[0];
    let mut max = array[0];

    for &value in array.iter().skip(1) {
        if value < min {
            min = value;
        }
        if value > max {
            max = value;
        }
    }

    Some((min, max))
}

macro_rules! value_range_typed {
    ($array:expr, $rust_ty:ty, $arrow_ty:ty) => {{
        let nd = $array.as_any().downcast_ref::<NdArray<$rust_ty>>()?;
        let values = nd.clone_into_raw_vec().await;
        let (min, max) = match nd.fill_value().await {
            Some(fill) => {
                let filtered: Vec<$rust_ty> = values.into_iter().filter(|v| *v != fill).collect();
                value_range_impl(&filtered)?
            }
            None => value_range_impl(&values)?,
        };
        let min_array: ArrayRef = Arc::new(<$arrow_ty>::from(vec![min]));
        let max_array: ArrayRef = Arc::new(<$arrow_ty>::from(vec![max]));
        Some((min_array, max_array))
    }};
}

pub async fn value_range(array: &dyn NdArrayD) -> Option<(ArrayRef, ArrayRef)> {
    match array.datatype() {
        NdArrayDataType::Bool => value_range_typed!(array, bool, BooleanArray),
        NdArrayDataType::I8 => value_range_typed!(array, i8, Int8Array),
        NdArrayDataType::I16 => value_range_typed!(array, i16, Int16Array),
        NdArrayDataType::I32 => value_range_typed!(array, i32, Int32Array),
        NdArrayDataType::I64 => value_range_typed!(array, i64, Int64Array),
        NdArrayDataType::U8 => value_range_typed!(array, u8, UInt8Array),
        NdArrayDataType::U16 => value_range_typed!(array, u16, UInt16Array),
        NdArrayDataType::U32 => value_range_typed!(array, u32, UInt32Array),
        NdArrayDataType::U64 => value_range_typed!(array, u64, UInt64Array),
        NdArrayDataType::F32 => value_range_typed!(array, f32, Float32Array),
        NdArrayDataType::F64 => value_range_typed!(array, f64, Float64Array),
        NdArrayDataType::Timestamp => {
            let nd = array
                .as_any()
                .downcast_ref::<NdArray<TimestampNanosecond>>()?;
            let values = nd.clone_into_raw_vec().await;
            let (min, max) = match nd.fill_value().await {
                Some(fill) => {
                    let filtered: Vec<TimestampNanosecond> =
                        values.into_iter().filter(|v| *v != fill).collect();
                    value_range_impl(&filtered)?
                }
                None => value_range_impl(&values)?,
            };
            let min_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![min.0]));
            let max_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![max.0]));
            Some((min_array, max_array))
        }
        NdArrayDataType::String | NdArrayDataType::Binary => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn make_nd<T: crate::datatypes::NdArrayType>(
        values: Vec<T>,
        fill_value: Option<T>,
    ) -> NdArray<T> {
        let len = values.len();
        crate::NdArray::try_new_from_vec_in_mem(
            values,
            vec![len],
            vec!["x".to_string()],
            fill_value,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_i32_basic() {
        let nd = make_nd(vec![3i32, 1, 4, 1, 5, 9, 2, 6], None);
        let (min, max) = value_range(&nd).await.unwrap();
        let min = min.as_any().downcast_ref::<Int32Array>().unwrap();
        let max = max.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(min.value(0), 1);
        assert_eq!(max.value(0), 9);
    }

    #[tokio::test]
    async fn test_f64_basic() {
        let nd = make_nd(vec![2.5f64, -1.0, 7.3, 0.0], None);
        let (min, max) = value_range(&nd).await.unwrap();
        let min = min.as_any().downcast_ref::<Float64Array>().unwrap();
        let max = max.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(min.value(0), -1.0);
        assert_eq!(max.value(0), 7.3);
    }

    #[tokio::test]
    async fn test_fill_value_excluded() {
        // Fill value -9999 sits outside the real range; must not skew min.
        let nd = make_nd(vec![10i32, -9999, 5, -9999, 20], Some(-9999));
        let (min, max) = value_range(&nd).await.unwrap();
        let min = min.as_any().downcast_ref::<Int32Array>().unwrap();
        let max = max.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(min.value(0), 5);
        assert_eq!(max.value(0), 20);
    }

    #[tokio::test]
    async fn test_all_fill_returns_none() {
        let nd = make_nd(vec![-9999i32, -9999, -9999], Some(-9999));
        assert!(value_range(&nd).await.is_none());
    }

    #[tokio::test]
    async fn test_empty_returns_none() {
        let nd = crate::NdArray::<f32>::try_new_from_vec_in_mem(
            vec![],
            vec![0],
            vec!["x".to_string()],
            None,
        )
        .unwrap();
        assert!(value_range(&nd).await.is_none());
    }

    #[tokio::test]
    async fn test_single_element() {
        let nd = make_nd(vec![42i64], None);
        let (min, max) = value_range(&nd).await.unwrap();
        let min = min.as_any().downcast_ref::<Int64Array>().unwrap();
        let max = max.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(min.value(0), 42);
        assert_eq!(max.value(0), 42);
    }

    #[tokio::test]
    async fn test_bool_range() {
        let nd = make_nd(vec![true, false, true], None);
        let (min, max) = value_range(&nd).await.unwrap();
        let min = min.as_any().downcast_ref::<BooleanArray>().unwrap();
        let max = max.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!min.value(0));
        assert!(max.value(0));
    }

    #[tokio::test]
    async fn test_f32_range() {
        let nd = make_nd(vec![1.0f32, 3.0, 2.0], None);
        let (min, max) = value_range(&nd).await.unwrap();
        let min = min.as_any().downcast_ref::<Float32Array>().unwrap();
        let max = max.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(min.value(0), 1.0);
        assert_eq!(max.value(0), 3.0);
    }

    #[tokio::test]
    async fn test_timestamp_range() {
        let nd = make_nd(
            vec![
                TimestampNanosecond(100),
                TimestampNanosecond(300),
                TimestampNanosecond(200),
            ],
            None,
        );
        let (min, max) = value_range(&nd).await.unwrap();
        let min = min
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let max = max
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(min.value(0), 100);
        assert_eq!(max.value(0), 300);
    }

    #[tokio::test]
    async fn test_timestamp_fill_excluded() {
        let fill = TimestampNanosecond(i64::MIN);
        let nd = make_nd(
            vec![TimestampNanosecond(500), fill, TimestampNanosecond(100)],
            Some(fill),
        );
        let (min, max) = value_range(&nd).await.unwrap();
        let min = min
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let max = max
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(min.value(0), 100);
        assert_eq!(max.value(0), 500);
    }

    #[tokio::test]
    async fn test_string_returns_none() {
        let nd = make_nd(vec!["a".to_string(), "b".to_string()], None);
        assert!(value_range(&nd).await.is_none());
    }
}
