use beacon_nd_arrow::array::compat_typings::{ArrowTypeConversion, TimestampNanosecond};

pub trait AtlasArrowCompat: ArrowTypeConversion + std::fmt::Debug + Send + Sync + 'static {
    fn from_arrow_array(array: &dyn arrow::array::Array) -> anyhow::Result<Vec<Self>>
    where
        Self: Sized;
}

macro_rules! impl_atlas_arrow_compat_primitive {
    ($rust_ty:ty, $array_ty:ty) => {
        impl AtlasArrowCompat for $rust_ty {
            fn from_arrow_array(array: &dyn arrow::array::Array) -> anyhow::Result<Vec<$rust_ty>> {
                let array = array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                    anyhow::anyhow!(
                        "expected {}, got {:?}",
                        stringify!($array_ty),
                        array.data_type()
                    )
                })?;

                array
                    .iter()
                    .map(|value| {
                        value.ok_or_else(|| {
                            anyhow::anyhow!("null values are not supported in AtlasArrowCompat")
                        })
                    })
                    .collect()
            }
        }
    };
}

impl_atlas_arrow_compat_primitive!(i8, arrow::array::Int8Array);
impl_atlas_arrow_compat_primitive!(i16, arrow::array::Int16Array);
impl_atlas_arrow_compat_primitive!(i32, arrow::array::Int32Array);
impl_atlas_arrow_compat_primitive!(i64, arrow::array::Int64Array);
impl_atlas_arrow_compat_primitive!(bool, arrow::array::BooleanArray);
impl_atlas_arrow_compat_primitive!(u8, arrow::array::UInt8Array);
impl_atlas_arrow_compat_primitive!(u16, arrow::array::UInt16Array);
impl_atlas_arrow_compat_primitive!(u32, arrow::array::UInt32Array);
impl_atlas_arrow_compat_primitive!(u64, arrow::array::UInt64Array);
impl_atlas_arrow_compat_primitive!(f32, arrow::array::Float32Array);
impl_atlas_arrow_compat_primitive!(f64, arrow::array::Float64Array);

impl AtlasArrowCompat for String {
    fn from_arrow_array(array: &dyn arrow::array::Array) -> anyhow::Result<Vec<String>> {
        if let Some(array) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
            return array
                .iter()
                .map(|value| {
                    value.map(str::to_string).ok_or_else(|| {
                        anyhow::anyhow!("null values are not supported in AtlasArrowCompat")
                    })
                })
                .collect();
        }

        if let Some(array) = array
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
        {
            return array
                .iter()
                .map(|value| {
                    value.map(str::to_string).ok_or_else(|| {
                        anyhow::anyhow!("null values are not supported in AtlasArrowCompat")
                    })
                })
                .collect();
        }

        Err(anyhow::anyhow!(
            "expected StringArray or LargeStringArray, got {:?}",
            array.data_type()
        ))
    }
}

impl AtlasArrowCompat for TimestampNanosecond {
    fn from_arrow_array(
        array: &dyn arrow::array::Array,
    ) -> anyhow::Result<Vec<TimestampNanosecond>> {
        let array = array
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "expected TimestampNanosecondArray, got {:?}",
                    array.data_type()
                )
            })?;

        array
            .iter()
            .map(|value| {
                value.map(TimestampNanosecond).ok_or_else(|| {
                    anyhow::anyhow!("null values are not supported in AtlasArrowCompat")
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::AtlasArrowCompat;
    use arrow::array::{
        BooleanArray, Float64Array, Int16Array, LargeStringArray, StringArray,
        TimestampNanosecondArray,
    };
    use beacon_nd_arrow::array::compat_typings::TimestampNanosecond;

    #[test]
    fn converts_primitive_arrays() {
        let array = Int16Array::from(vec![1, 2, 3]);
        let values = <i16 as AtlasArrowCompat>::from_arrow_array(&array).unwrap();

        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn converts_float_arrays() {
        let array = Float64Array::from(vec![1.5, 2.5, 3.5]);
        let values = <f64 as AtlasArrowCompat>::from_arrow_array(&array).unwrap();

        assert_eq!(values, vec![1.5, 2.5, 3.5]);
    }

    #[test]
    fn converts_boolean_arrays() {
        let array = BooleanArray::from(vec![true, false, true]);
        let values = <bool as AtlasArrowCompat>::from_arrow_array(&array).unwrap();

        assert_eq!(values, vec![true, false, true]);
    }

    #[test]
    fn converts_utf8_arrays() {
        let array = StringArray::from(vec!["alpha", "beta"]);
        let values = <String as AtlasArrowCompat>::from_arrow_array(&array).unwrap();

        assert_eq!(values, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn converts_large_utf8_arrays() {
        let array = LargeStringArray::from(vec!["gamma", "delta"]);
        let values = <String as AtlasArrowCompat>::from_arrow_array(&array).unwrap();

        assert_eq!(values, vec!["gamma".to_string(), "delta".to_string()]);
    }

    #[test]
    fn converts_timestamp_nanosecond_arrays() {
        let array = TimestampNanosecondArray::from(vec![100_i64, 200_i64, 300_i64]);
        let values = <TimestampNanosecond as AtlasArrowCompat>::from_arrow_array(&array).unwrap();

        assert_eq!(
            values,
            vec![
                TimestampNanosecond(100),
                TimestampNanosecond(200),
                TimestampNanosecond(300),
            ]
        );
    }

    #[test]
    fn rejects_nulls() {
        let array = StringArray::from(vec![Some("alpha"), None]);
        let err = <String as AtlasArrowCompat>::from_arrow_array(&array).unwrap_err();

        assert_eq!(
            err.to_string(),
            "null values are not supported in AtlasArrowCompat"
        );
    }

    #[test]
    fn rejects_wrong_array_type() {
        let array = StringArray::from(vec!["alpha"]);
        let err = <i16 as AtlasArrowCompat>::from_arrow_array(&array).unwrap_err();

        let message = err.to_string();
        assert!(message.contains("Int16Array"));
        assert!(message.contains("Utf8"));
    }
}
