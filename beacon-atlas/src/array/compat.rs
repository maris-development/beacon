use arrow::array::Array;
use beacon_nd_arrow::array::compat_typings::{ArrowTypeConversion, TimestampNanosecond};

#[derive(Debug, Clone, PartialEq)]
pub struct AtlasArrowValues<T> {
    pub values: Vec<T>,
    pub validity: Option<Vec<bool>>,
}

impl<T> AtlasArrowValues<T> {
    pub fn new(values: Vec<T>, validity: Option<Vec<bool>>) -> Self {
        Self { values, validity }
    }
}

pub trait AtlasArrowCompat: ArrowTypeConversion + std::fmt::Debug + Send + Sync + 'static {
    fn null_sentinel() -> Self
    where
        Self: Sized;

    fn from_arrow_array_with_validity(
        array: &dyn arrow::array::Array,
    ) -> anyhow::Result<AtlasArrowValues<Self>>
    where
        Self: Sized;

    fn from_arrow_array(array: &dyn arrow::array::Array) -> anyhow::Result<Vec<Self>>
    where
        Self: Sized,
    {
        let converted = Self::from_arrow_array_with_validity(array)?;
        if let Some(validity) = converted.validity {
            if validity.iter().any(|is_valid| !is_valid) {
                anyhow::bail!("null values are not supported in AtlasArrowCompat");
            }
        }
        Ok(converted.values)
    }
}

macro_rules! impl_atlas_arrow_compat_primitive {
    ($rust_ty:ty, $array_ty:ty, $sentinel:expr) => {
        impl AtlasArrowCompat for $rust_ty {
            fn null_sentinel() -> Self {
                $sentinel
            }

            fn from_arrow_array_with_validity(
                array: &dyn arrow::array::Array,
            ) -> anyhow::Result<AtlasArrowValues<$rust_ty>> {
                let array = array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                    anyhow::anyhow!(
                        "expected {}, got {:?}",
                        stringify!($array_ty),
                        array.data_type()
                    )
                })?;

                let validity = if array.null_count() == 0 {
                    None
                } else {
                    Some(array.iter().map(|value| value.is_some()).collect())
                };
                let values = array
                    .iter()
                    .map(|value| value.unwrap_or_else(Self::null_sentinel))
                    .collect();

                Ok(AtlasArrowValues::new(values, validity))
            }
        }
    };
}

impl_atlas_arrow_compat_primitive!(i8, arrow::array::Int8Array, i8::MAX);
impl_atlas_arrow_compat_primitive!(i16, arrow::array::Int16Array, i16::MAX);
impl_atlas_arrow_compat_primitive!(i32, arrow::array::Int32Array, i32::MAX);
impl_atlas_arrow_compat_primitive!(i64, arrow::array::Int64Array, i64::MAX);
impl_atlas_arrow_compat_primitive!(bool, arrow::array::BooleanArray, false);
impl_atlas_arrow_compat_primitive!(u8, arrow::array::UInt8Array, u8::MAX);
impl_atlas_arrow_compat_primitive!(u16, arrow::array::UInt16Array, u16::MAX);
impl_atlas_arrow_compat_primitive!(u32, arrow::array::UInt32Array, u32::MAX);
impl_atlas_arrow_compat_primitive!(u64, arrow::array::UInt64Array, u64::MAX);
impl_atlas_arrow_compat_primitive!(f32, arrow::array::Float32Array, f32::MAX);
impl_atlas_arrow_compat_primitive!(f64, arrow::array::Float64Array, f64::MAX);

impl AtlasArrowCompat for String {
    fn null_sentinel() -> Self {
        String::new()
    }

    fn from_arrow_array_with_validity(
        array: &dyn arrow::array::Array,
    ) -> anyhow::Result<AtlasArrowValues<String>> {
        if let Some(array) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
            let validity = if array.null_count() == 0 {
                None
            } else {
                Some(array.iter().map(|value| value.is_some()).collect())
            };
            let values = array
                .iter()
                .map(|value| {
                    value
                        .map(str::to_string)
                        .unwrap_or_else(Self::null_sentinel)
                })
                .collect();

            return Ok(AtlasArrowValues::new(values, validity));
        }

        if let Some(array) = array
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
        {
            let validity = if array.null_count() == 0 {
                None
            } else {
                Some(array.iter().map(|value| value.is_some()).collect())
            };
            let values = array
                .iter()
                .map(|value| {
                    value
                        .map(str::to_string)
                        .unwrap_or_else(Self::null_sentinel)
                })
                .collect();

            return Ok(AtlasArrowValues::new(values, validity));
        }

        Err(anyhow::anyhow!(
            "expected StringArray or LargeStringArray, got {:?}",
            array.data_type()
        ))
    }
}

impl AtlasArrowCompat for TimestampNanosecond {
    fn null_sentinel() -> Self {
        TimestampNanosecond(i64::MAX)
    }

    fn from_arrow_array_with_validity(
        array: &dyn arrow::array::Array,
    ) -> anyhow::Result<AtlasArrowValues<TimestampNanosecond>> {
        let array = array
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "expected TimestampNanosecondArray, got {:?}",
                    array.data_type()
                )
            })?;

        let validity = if array.null_count() == 0 {
            None
        } else {
            Some(array.iter().map(|value| value.is_some()).collect())
        };
        let values = array
            .iter()
            .map(|value| {
                value
                    .map(TimestampNanosecond)
                    .unwrap_or_else(Self::null_sentinel)
            })
            .collect();

        Ok(AtlasArrowValues::new(values, validity))
    }
}

#[cfg(test)]
mod tests {
    use super::{AtlasArrowCompat, AtlasArrowValues};
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
    fn converts_arrays_with_validity_mask() {
        let array = Int16Array::from(vec![Some(1), None, Some(3)]);
        let converted = <i16 as AtlasArrowCompat>::from_arrow_array_with_validity(&array).unwrap();

        assert_eq!(converted.values, vec![1, i16::MAX, 3]);
        assert_eq!(converted.validity, Some(vec![true, false, true]));
    }

    #[test]
    fn converts_timestamps_with_validity_mask() {
        let array = TimestampNanosecondArray::from(vec![Some(10_i64), None, Some(30_i64)]);
        let converted =
            <TimestampNanosecond as AtlasArrowCompat>::from_arrow_array_with_validity(&array)
                .unwrap();

        assert_eq!(
            converted.values,
            vec![
                TimestampNanosecond(10),
                TimestampNanosecond(i64::MAX),
                TimestampNanosecond(30),
            ]
        );
        assert_eq!(converted.validity, Some(vec![true, false, true]));
    }

    #[test]
    fn converts_strings_with_validity_mask() {
        let array = StringArray::from(vec![Some("alpha"), None, Some("beta")]);
        let converted =
            <String as AtlasArrowCompat>::from_arrow_array_with_validity(&array).unwrap();

        assert_eq!(
            converted,
            AtlasArrowValues::new(
                vec!["alpha".to_string(), "".to_string(), "beta".to_string()],
                Some(vec![true, false, true])
            )
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
