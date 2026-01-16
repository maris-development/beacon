//! Conversions to/from `ndarray`.
//!
//! This module is feature-gated behind `beacon-nd-arrow/ndarray`.
//!
//! Notes:
//! - `ndarray` has shapes but no axis names; therefore, converting
//!   `ndarray -> NdArrowArray` requires explicit dimension names.
//! - For `NdArrowArray -> ndarray`, nulls can be materialized by using a
//!   caller-provided (or default) `fill_value`.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};

use crate::{
    NdArrowArray,
    dimensions::{Dimension, Dimensions},
    error::NdArrayError,
};

/// Convert an `ndarray` into an [`NdArrowArray`].
///
/// Since `ndarray` does not store axis names, `dim_names` is required for
/// non-scalar arrays.
pub trait FromNdarray<T> {
    fn from_ndarray(
        array: &ndarray::ArrayD<T>,
        dim_names: &[String],
    ) -> Result<NdArrowArray, NdArrayError>;
}

/// Convert an [`NdArrowArray`] into an `ndarray`.
pub trait ToNdarray<T> {
    fn to_ndarray(&self) -> Result<ndarray::ArrayD<T>, NdArrayError>;
}

/// Provide a default fill value for representing Arrow nulls in an `ndarray`.
///
/// This is a convenience only. The default fill value may collide with real data;
/// callers that need a guaranteed sentinel should use
/// [`ToNdarrayWithNulls::to_ndarray_with_fill_value`].
pub trait DefaultFillValue: Sized {
    fn default_fill_value() -> Self;
}

/// Convert an [`NdArrowArray`] into an `ndarray`, replacing nulls with a fill value.
///
/// Returns the created ndarray along with the `fill_value` used to represent nulls.
pub trait ToNdarrayWithNulls<T> {
    fn to_ndarray_with_fill_value(
        &self,
        fill_value: T,
    ) -> Result<(ndarray::ArrayD<T>, T), NdArrayError>;

    fn to_ndarray_with_default_fill(&self) -> Result<(ndarray::ArrayD<T>, T), NdArrayError>
    where
        T: DefaultFillValue + Clone,
        Self: Sized,
    {
        let fill_value = T::default_fill_value();
        self.to_ndarray_with_fill_value(fill_value.clone())
    }
}

fn dimensions_from_shape(
    shape: &[usize],
    dim_names: &[String],
) -> Result<Dimensions, NdArrayError> {
    if shape.is_empty() {
        if !dim_names.is_empty() {
            return Err(NdArrayError::InvalidDimensions(
                "scalar arrays must not provide dim_names".to_string(),
            ));
        }
        return Ok(Dimensions::Scalar);
    }

    if dim_names.len() != shape.len() {
        return Err(NdArrayError::InvalidDimensions(format!(
            "dim_names length {} does not match rank {}",
            dim_names.len(),
            shape.len()
        )));
    }

    let dims = dim_names
        .iter()
        .cloned()
        .zip(shape.iter().copied())
        .map(|(name, size)| Dimension::try_new(name, size))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Dimensions::new(dims))
}

fn ensure_no_nulls(array: &ArrayRef) -> Result<(), NdArrayError> {
    if array.null_count() > 0 {
        return Err(NdArrayError::InvalidNdColumn(
            "ndarray conversion does not currently support nulls".to_string(),
        ));
    }
    Ok(())
}

macro_rules! impl_primitive {
    ($t:ty, $arrow_array:ty) => {
        impl FromNdarray<$t> for NdArrowArray {
            fn from_ndarray(
                array: &ndarray::ArrayD<$t>,
                dim_names: &[String],
            ) -> Result<NdArrowArray, NdArrayError> {
                let dims = dimensions_from_shape(array.shape(), dim_names)?;
                let values = array.iter().copied().collect::<Vec<_>>();
                Ok(NdArrowArray::new(
                    Arc::new(<$arrow_array>::from(values)),
                    dims,
                )?)
            }
        }

        impl ToNdarray<$t> for NdArrowArray {
            fn to_ndarray(&self) -> Result<ndarray::ArrayD<$t>, NdArrayError> {
                ensure_no_nulls(self.values())?;

                let shape = self.dimensions().shape();
                let arr = self
                    .values()
                    .as_any()
                    .downcast_ref::<$arrow_array>()
                    .ok_or_else(|| {
                        NdArrayError::InvalidNdColumn(format!(
                            "storage array type mismatch for ndarray conversion; expected {}",
                            stringify!($arrow_array)
                        ))
                    })?;

                let values = arr.values().to_vec();
                Ok(
                    ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(
                        |e| {
                            NdArrayError::InvalidNdColumn(format!(
                                "failed to construct ndarray: {e}"
                            ))
                        },
                    )?,
                )
            }
        }

        impl ToNdarrayWithNulls<$t> for NdArrowArray {
            fn to_ndarray_with_fill_value(
                &self,
                fill_value: $t,
            ) -> Result<(ndarray::ArrayD<$t>, $t), NdArrayError> {
                let shape = self.dimensions().shape();
                let arr = self
                    .values()
                    .as_any()
                    .downcast_ref::<$arrow_array>()
                    .ok_or_else(|| {
                        NdArrayError::InvalidNdColumn(format!(
                            "storage array type mismatch for ndarray conversion; expected {}",
                            stringify!($arrow_array)
                        ))
                    })?;

                let mut values = Vec::with_capacity(arr.len());
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        values.push(fill_value);
                    } else {
                        values.push(arr.value(i));
                    }
                }

                let out = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(
                    |e| NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}")),
                )?;
                Ok((out, fill_value))
            }
        }
    };
}

impl_primitive!(i32, Int32Array);
impl_primitive!(i64, Int64Array);
impl_primitive!(i8, Int8Array);
impl_primitive!(i16, Int16Array);
impl_primitive!(f32, Float32Array);
impl_primitive!(f64, Float64Array);
impl_primitive!(u8, UInt8Array);
impl_primitive!(u16, UInt16Array);
impl_primitive!(u32, UInt32Array);
impl_primitive!(u64, UInt64Array);

impl FromNdarray<bool> for NdArrowArray {
    fn from_ndarray(
        array: &ndarray::ArrayD<bool>,
        dim_names: &[String],
    ) -> Result<NdArrowArray, NdArrayError> {
        let dims = dimensions_from_shape(array.shape(), dim_names)?;
        let values = array.iter().copied().collect::<Vec<_>>();
        Ok(NdArrowArray::new(
            Arc::new(BooleanArray::from(values)),
            dims,
        )?)
    }
}

impl ToNdarray<bool> for NdArrowArray {
    fn to_ndarray(&self) -> Result<ndarray::ArrayD<bool>, NdArrayError> {
        ensure_no_nulls(self.values())?;

        let shape = self.dimensions().shape();
        let arr = self
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                NdArrayError::InvalidNdColumn(
                    "storage array type mismatch for ndarray conversion; expected BooleanArray"
                        .to_string(),
                )
            })?;

        let values = (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>();
        Ok(
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(|e| {
                NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}"))
            })?,
        )
    }
}

impl ToNdarrayWithNulls<bool> for NdArrowArray {
    fn to_ndarray_with_fill_value(
        &self,
        fill_value: bool,
    ) -> Result<(ndarray::ArrayD<bool>, bool), NdArrayError> {
        let shape = self.dimensions().shape();
        let arr = self
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                NdArrayError::InvalidNdColumn(
                    "storage array type mismatch for ndarray conversion; expected BooleanArray"
                        .to_string(),
                )
            })?;

        let mut values = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            if arr.is_null(i) {
                values.push(fill_value);
            } else {
                values.push(arr.value(i));
            }
        }

        let out = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(|e| {
            NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}"))
        })?;
        Ok((out, fill_value))
    }
}

impl FromNdarray<String> for NdArrowArray {
    fn from_ndarray(
        array: &ndarray::ArrayD<String>,
        dim_names: &[String],
    ) -> Result<NdArrowArray, NdArrayError> {
        let dims = dimensions_from_shape(array.shape(), dim_names)?;
        let values = array.iter().cloned().collect::<Vec<_>>();
        Ok(NdArrowArray::new(
            Arc::new(StringArray::from(values)),
            dims,
        )?)
    }
}

impl ToNdarray<String> for NdArrowArray {
    fn to_ndarray(&self) -> Result<ndarray::ArrayD<String>, NdArrayError> {
        ensure_no_nulls(self.values())?;

        let shape = self.dimensions().shape();
        let arr = self
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                NdArrayError::InvalidNdColumn(
                    "storage array type mismatch for ndarray conversion; expected StringArray"
                        .to_string(),
                )
            })?;

        let values = (0..arr.len()).map(|i| arr.value(i).to_string()).collect();
        Ok(
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(|e| {
                NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}"))
            })?,
        )
    }
}

impl ToNdarrayWithNulls<String> for NdArrowArray {
    fn to_ndarray_with_fill_value(
        &self,
        fill_value: String,
    ) -> Result<(ndarray::ArrayD<String>, String), NdArrayError> {
        let shape = self.dimensions().shape();
        let arr = self
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                NdArrayError::InvalidNdColumn(
                    "storage array type mismatch for ndarray conversion; expected StringArray"
                        .to_string(),
                )
            })?;

        let mut values = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            if arr.is_null(i) {
                values.push(fill_value.clone());
            } else {
                values.push(arr.value(i).to_string());
            }
        }

        let out = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(|e| {
            NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}"))
        })?;
        Ok((out, fill_value))
    }
}

fn timestamp_array_to_nanos(array: &ArrayRef) -> Result<Vec<i64>, NdArrayError> {
    ensure_no_nulls(array)?;

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "failed to downcast to TimestampNanosecondArray".to_string(),
                    )
                })?;
            Ok((0..a.len()).map(|i| a.value(i)).collect())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "failed to downcast to TimestampMicrosecondArray".to_string(),
                    )
                })?;
            Ok((0..a.len())
                .map(|i| a.value(i).saturating_mul(1_000))
                .collect())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "failed to downcast to TimestampMillisecondArray".to_string(),
                    )
                })?;
            Ok((0..a.len())
                .map(|i| a.value(i).saturating_mul(1_000_000))
                .collect())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "failed to downcast to TimestampSecondArray".to_string(),
                    )
                })?;
            Ok((0..a.len())
                .map(|i| a.value(i).saturating_mul(1_000_000_000))
                .collect())
        }
        other => Err(NdArrayError::InvalidNdColumn(format!(
            "expected Timestamp storage array, got {other:?}"
        ))),
    }
}

fn timestamp_array_to_nanos_with_fill(
    array: &ArrayRef,
    fill_nanos: i64,
) -> Result<Vec<i64>, NdArrayError> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "failed to downcast to TimestampNanosecondArray".to_string(),
                    )
                })?;
            Ok((0..a.len())
                .map(|i| if a.is_null(i) { fill_nanos } else { a.value(i) })
                .collect())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "failed to downcast to TimestampMicrosecondArray".to_string(),
                    )
                })?;
            Ok((0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        fill_nanos
                    } else {
                        a.value(i).saturating_mul(1_000)
                    }
                })
                .collect())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "failed to downcast to TimestampMillisecondArray".to_string(),
                    )
                })?;
            Ok((0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        fill_nanos
                    } else {
                        a.value(i).saturating_mul(1_000_000)
                    }
                })
                .collect())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "failed to downcast to TimestampSecondArray".to_string(),
                    )
                })?;
            Ok((0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        fill_nanos
                    } else {
                        a.value(i).saturating_mul(1_000_000_000)
                    }
                })
                .collect())
        }
        other => Err(NdArrayError::InvalidNdColumn(format!(
            "expected Timestamp storage array, got {other:?}"
        ))),
    }
}

fn naive_datetime_from_nanos(ns: i64) -> Option<chrono::NaiveDateTime> {
    // Euclidean division keeps remainder in [0, 1e9).
    let secs = ns.div_euclid(1_000_000_000);
    let subsec_nanos = ns.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, subsec_nanos).map(|dt| dt.naive_utc())
}

impl FromNdarray<chrono::NaiveDateTime> for NdArrowArray {
    fn from_ndarray(
        array: &ndarray::ArrayD<chrono::NaiveDateTime>,
        dim_names: &[String],
    ) -> Result<NdArrowArray, NdArrayError> {
        let dims = dimensions_from_shape(array.shape(), dim_names)?;
        let values = array
            .iter()
            .map(|dt| {
                dt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "chrono timestamp is out of range for i64 nanoseconds".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let storage =
            arrow::array::PrimitiveArray::<arrow::datatypes::TimestampNanosecondType>::from(values);
        let storage = storage.with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, None));

        Ok(NdArrowArray::new(Arc::new(storage), dims)?)
    }
}

impl FromNdarray<chrono::DateTime<chrono::Utc>> for NdArrowArray {
    fn from_ndarray(
        array: &ndarray::ArrayD<chrono::DateTime<chrono::Utc>>,
        dim_names: &[String],
    ) -> Result<NdArrowArray, NdArrayError> {
        let dims = dimensions_from_shape(array.shape(), dim_names)?;
        let values = array
            .iter()
            .map(|dt| {
                dt.timestamp_nanos_opt().ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(
                        "chrono timestamp is out of range for i64 nanoseconds".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let storage =
            arrow::array::PrimitiveArray::<arrow::datatypes::TimestampNanosecondType>::from(values);
        let storage = storage.with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, None));

        Ok(NdArrowArray::new(Arc::new(storage), dims)?)
    }
}

impl ToNdarray<chrono::NaiveDateTime> for NdArrowArray {
    fn to_ndarray(&self) -> Result<ndarray::ArrayD<chrono::NaiveDateTime>, NdArrayError> {
        let shape = self.dimensions().shape();
        let nanos = timestamp_array_to_nanos(self.values())?;

        let values = nanos
            .into_iter()
            .map(|ns| {
                naive_datetime_from_nanos(ns).ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(format!(
                        "invalid timestamp nanoseconds value for chrono: {ns}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(|e| {
                NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}"))
            })?,
        )
    }
}

impl ToNdarrayWithNulls<chrono::NaiveDateTime> for NdArrowArray {
    fn to_ndarray_with_fill_value(
        &self,
        fill_value: chrono::NaiveDateTime,
    ) -> Result<
        (
            ndarray::ArrayD<chrono::NaiveDateTime>,
            chrono::NaiveDateTime,
        ),
        NdArrayError,
    > {
        let shape = self.dimensions().shape();
        let fill_nanos = fill_value.and_utc().timestamp_nanos_opt().ok_or_else(|| {
            NdArrayError::InvalidNdColumn(
                "fill_value is out of range for i64 nanoseconds".to_string(),
            )
        })?;

        let nanos = timestamp_array_to_nanos_with_fill(self.values(), fill_nanos)?;

        let values = nanos
            .into_iter()
            .map(|ns| {
                naive_datetime_from_nanos(ns).ok_or_else(|| {
                    NdArrayError::InvalidNdColumn(format!(
                        "invalid timestamp nanoseconds value for chrono: {ns}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let out = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(|e| {
            NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}"))
        })?;
        Ok((out, fill_value))
    }
}

impl ToNdarray<chrono::DateTime<chrono::Utc>> for NdArrowArray {
    fn to_ndarray(&self) -> Result<ndarray::ArrayD<chrono::DateTime<chrono::Utc>>, NdArrayError> {
        ensure_no_nulls(self.values())?;
        let shape = self.dimensions().shape();
        let nanos = timestamp_array_to_nanos(self.values())?;

        let values = nanos
            .into_iter()
            .map(|ns| {
                let secs = ns.div_euclid(1_000_000_000);
                let subsec_nanos = ns.rem_euclid(1_000_000_000) as u32;
                chrono::DateTime::<chrono::Utc>::from_timestamp(secs, subsec_nanos).ok_or_else(
                    || {
                        NdArrayError::InvalidNdColumn(format!(
                            "invalid timestamp nanoseconds value for chrono: {ns}"
                        ))
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(|e| {
                NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}"))
            })?,
        )
    }
}

impl ToNdarrayWithNulls<chrono::DateTime<chrono::Utc>> for NdArrowArray {
    fn to_ndarray_with_fill_value(
        &self,
        fill_value: chrono::DateTime<chrono::Utc>,
    ) -> Result<
        (
            ndarray::ArrayD<chrono::DateTime<chrono::Utc>>,
            chrono::DateTime<chrono::Utc>,
        ),
        NdArrayError,
    > {
        let shape = self.dimensions().shape();
        let fill_nanos = fill_value.timestamp_nanos_opt().ok_or_else(|| {
            NdArrayError::InvalidNdColumn(
                "fill_value is out of range for i64 nanoseconds".to_string(),
            )
        })?;

        let nanos = timestamp_array_to_nanos_with_fill(self.values(), fill_nanos)?;

        let values = nanos
            .into_iter()
            .map(|ns| {
                let secs = ns.div_euclid(1_000_000_000);
                let subsec_nanos = ns.rem_euclid(1_000_000_000) as u32;
                chrono::DateTime::<chrono::Utc>::from_timestamp(secs, subsec_nanos).ok_or_else(
                    || {
                        NdArrayError::InvalidNdColumn(format!(
                            "invalid timestamp nanoseconds value for chrono: {ns}"
                        ))
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let out = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), values).map_err(|e| {
            NdArrayError::InvalidNdColumn(format!("failed to construct ndarray: {e}"))
        })?;
        Ok((out, fill_value))
    }
}

macro_rules! impl_default_fill_max_int {
    ($t:ty) => {
        impl DefaultFillValue for $t {
            fn default_fill_value() -> Self {
                <$t>::MAX
            }
        }
    };
}

impl_default_fill_max_int!(i8);
impl_default_fill_max_int!(i16);
impl_default_fill_max_int!(i32);
impl_default_fill_max_int!(i64);
impl_default_fill_max_int!(u8);
impl_default_fill_max_int!(u16);
impl_default_fill_max_int!(u32);
impl_default_fill_max_int!(u64);

impl DefaultFillValue for f32 {
    fn default_fill_value() -> Self {
        f32::NAN
    }
}

impl DefaultFillValue for f64 {
    fn default_fill_value() -> Self {
        f64::NAN
    }
}

impl DefaultFillValue for bool {
    fn default_fill_value() -> Self {
        false
    }
}

impl DefaultFillValue for String {
    fn default_fill_value() -> Self {
        String::new()
    }
}

impl DefaultFillValue for chrono::NaiveDateTime {
    fn default_fill_value() -> Self {
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .unwrap()
            .naive_utc()
    }
}

impl DefaultFillValue for chrono::DateTime<chrono::Utc> {
    fn default_fill_value() -> Self {
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{FromNdarray, ToNdarray, ToNdarrayWithNulls};
    use crate::NdArrowArray;
    use arrow::array::Int32Array;

    #[test]
    fn roundtrip_i32_ndarray() {
        let a =
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 3]), (1..=6).collect()).unwrap();
        let nd = NdArrowArray::from_ndarray(&a, &vec!["y".to_string(), "x".to_string()]).unwrap();
        assert_eq!(nd.dimensions().shape(), vec![2, 3]);

        let b: ndarray::ArrayD<i32> = nd.to_ndarray().unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn roundtrip_utf8_ndarray() {
        let a = ndarray::ArrayD::from_shape_vec(
            ndarray::IxDyn(&[2, 2]),
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ],
        )
        .unwrap();

        let nd = NdArrowArray::from_ndarray(&a, &vec!["y".to_string(), "x".to_string()]).unwrap();
        let b: ndarray::ArrayD<String> = nd.to_ndarray().unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn roundtrip_timestamp_naive_ndarray() {
        let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .unwrap()
            .naive_utc();
        let t1 = chrono::DateTime::<chrono::Utc>::from_timestamp(1, 500)
            .unwrap()
            .naive_utc();
        let a = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[2]), vec![t0, t1]).unwrap();

        let nd = NdArrowArray::from_ndarray(&a, &vec!["t".to_string()]).unwrap();
        let b: ndarray::ArrayD<chrono::NaiveDateTime> = nd.to_ndarray().unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn roundtrip_u32_ndarray() {
        let a =
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 2]), vec![1_u32, 2, 3, 4]).unwrap();
        let nd = NdArrowArray::from_ndarray(&a, &vec!["y".to_string(), "x".to_string()]).unwrap();
        let b: ndarray::ArrayD<u32> = nd.to_ndarray().unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn to_ndarray_with_nulls_uses_fill_value() {
        let values = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None]));
        let nd = NdArrowArray::new(
            values,
            crate::dimensions::Dimensions::new(vec![
                crate::dimensions::Dimension::try_new("y", 2).unwrap(),
                crate::dimensions::Dimension::try_new("x", 2).unwrap(),
            ]),
        )
        .unwrap();

        let (arr, fill) = nd.to_ndarray_with_fill_value(-1).unwrap();
        assert_eq!(fill, -1);
        let expected =
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 2]), vec![1, -1, 3, -1]).unwrap();
        assert_eq!(arr, expected);
    }
}
