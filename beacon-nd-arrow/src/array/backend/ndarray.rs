use std::{fmt::Debug, sync::Arc};

use crate::array::backend::ArrayBackend;

/// Conversion from a contiguous Rust slice into an Arrow [`ArrayRef`].
///
/// Implement this trait for any Rust type that should be storable inside an
/// [`NdArrayBackend`]. The conversion is infallible for all fixed-width
/// primitive types; it can fail for variable-width or range-limited types
/// (e.g. [`chrono::NaiveDateTime`] when the timestamp exceeds the `i64`
/// nanosecond range).
///
/// # Type mapping
///
/// | Rust type                        | Arrow array type                          |
/// |----------------------------------|-------------------------------------------|
/// | `i8` / `u8`                      | `Int8Array` / `UInt8Array`                |
/// | `i16` / `u16`                    | `Int16Array` / `UInt16Array`              |
/// | `i32` / `u32`                    | `Int32Array` / `UInt32Array`              |
/// | `i64` / `u64`                    | `Int64Array` / `UInt64Array`              |
/// | `f32` / `f64`                    | `Float32Array` / `Float64Array`           |
/// | `bool`                           | `BooleanArray`                            |
/// | `String`                         | `StringArray` (UTF-8, 32-bit offsets)     |
/// | `chrono::NaiveDateTime`          | `Timestamp(Nanosecond, None)`             |
/// | `chrono::DateTime<chrono::Utc>`  | `Timestamp(Nanosecond, Some("UTC"))`      |
///
/// [`ArrayRef`]: arrow::array::ArrayRef
pub trait ArrowTypeConversion: Debug + Send + Sync + 'static {
    /// Convert a contiguous slice of `Self` values into an Arrow [`ArrayRef`].
    ///
    /// The returned array must contain exactly `array.len()` elements in the
    /// same order as the input slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the conversion is not representable (e.g. a
    /// timestamp that overflows `i64` nanoseconds).
    ///
    /// [`ArrayRef`]: arrow::array::ArrayRef
    fn arrow_from_array_view(array: &[Self]) -> anyhow::Result<arrow::array::ArrayRef>
    where
        Self: Sized;
}

/// An [`ArrayBackend`] backed by an [`ndarray::ArcArrayD`] in host memory.
///
/// `NdArrayBackend` wraps a reference-counted N-dimensional array together
/// with a list of named dimensions. Slicing is performed synchronously on the
/// underlying contiguous memory buffer; the `async` surface exists only to
/// satisfy the [`ArrayBackend`] contract (which is designed to accommodate
/// remote / IO-bound backends as well).
///
/// # Type parameter
///
/// `T` must implement [`ArrowTypeConversion`] so that slices can be
/// materialised as Arrow arrays on demand.
#[derive(Debug)]
pub struct NdArrayBackend<T: ArrowTypeConversion> {
    /// The underlying N-dimensional array data, shared via reference counting.
    data: ndarray::ArcArrayD<T>,
    /// Ordered list of dimension names, one per axis of `data`.
    dimensions: Vec<String>,
}

impl<T: ArrowTypeConversion> NdArrayBackend<T> {
    /// Create a new `NdArrayBackend` from an existing arc array and a matching
    /// list of dimension names.
    ///
    /// # Panics
    ///
    /// Does not panic; dimension-name / shape mismatches are the caller's
    /// responsibility to avoid.
    pub fn new(data: ndarray::ArcArrayD<T>, dimensions: Vec<String>) -> Self {
        Self { data, dimensions }
    }
}

#[async_trait::async_trait]
impl<T: ArrowTypeConversion> ArrayBackend for NdArrayBackend<T> {
    /// Returns the total number of elements across all dimensions.
    fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns the size of each axis in major-to-minor order.
    fn shape(&self) -> Vec<usize> {
        self.data.shape().to_vec()
    }

    /// Returns the ordered list of dimension names for this array.
    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    /// Extract a flat (linearised) sub-range of the array as an Arrow array.
    ///
    /// Elements are taken from the flattened view of the underlying
    /// N-dimensional array in C (row-major) order, from index `start`
    /// (inclusive) up to `start + length` (exclusive).
    ///
    /// # Errors
    ///
    /// * `start + length` overflows `usize`.
    /// * `start + length > self.len()` (out-of-bounds).
    /// * The underlying ndarray is not contiguous in memory.
    /// * [`ArrowTypeConversion::arrow_from_array_view`] fails for the element
    ///   type (e.g. a timestamp out of the `i64` nanosecond range).
    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<arrow::array::ArrayRef> {
        // Guard against `start + length` wrapping around on 32-bit targets.
        let end = start.checked_add(length).ok_or_else(|| {
            anyhow::anyhow!(
                "Slice arithmetic overflow: start={}, length={}",
                start,
                length
            )
        })?;

        // Bounds check before touching the data buffer.
        if end > self.data.len() {
            return Err(anyhow::anyhow!(
                "Slice out of bounds: start={}, length={}, total={}",
                start,
                length,
                self.data.len()
            ));
        }

        // `to_slice()` succeeds only when the array is stored in a contiguous,
        // C-order (row-major) memory layout.
        let array_slice = self.data.view().to_slice().ok_or_else(|| {
            anyhow::anyhow!(
                "Failed to get a contiguous slice of the array data. \
                 Array may not be contiguous in memory."
            )
        })?;

        // Delegate the actual Rust→Arrow conversion to the type-level impl.
        T::arrow_from_array_view(&array_slice[start..end])
    }
}

/// Generates a boilerplate [`ArrowTypeConversion`] implementation for any
/// fixed-width primitive type that Arrow represents with a single,
/// homogeneous `PrimitiveArray`.
///
/// # Usage
///
/// ```text
/// impl_arrow_type_conversion_primitive!(rust_type, ArrowArrayType);
/// ```
///
/// The macro simply allocates a `Vec<$rust_ty>` from the input slice and
/// wraps the resulting Arrow array in an `Arc`.
macro_rules! impl_arrow_type_conversion_primitive {
    ($rust_ty:ty, $arrow_array_ty:ty) => {
        impl ArrowTypeConversion for $rust_ty {
            fn arrow_from_array_view(array: &[Self]) -> anyhow::Result<arrow::array::ArrayRef> {
                let arrow_array = <$arrow_array_ty>::from(array.to_vec());
                Ok(Arc::new(arrow_array))
            }
        }
    };
}

// ── Signed integers ────────────────────────────────────────────────────────
impl_arrow_type_conversion_primitive!(i8, arrow::array::Int8Array);
impl_arrow_type_conversion_primitive!(i16, arrow::array::Int16Array);
impl_arrow_type_conversion_primitive!(i32, arrow::array::Int32Array);
impl_arrow_type_conversion_primitive!(i64, arrow::array::Int64Array);

// ── Unsigned integers ──────────────────────────────────────────────────────
impl_arrow_type_conversion_primitive!(u8, arrow::array::UInt8Array);
impl_arrow_type_conversion_primitive!(u16, arrow::array::UInt16Array);
impl_arrow_type_conversion_primitive!(u32, arrow::array::UInt32Array);
impl_arrow_type_conversion_primitive!(u64, arrow::array::UInt64Array);

// ── Floating-point ─────────────────────────────────────────────────────────
impl_arrow_type_conversion_primitive!(f32, arrow::array::Float32Array);
impl_arrow_type_conversion_primitive!(f64, arrow::array::Float64Array);

// ── Boolean ────────────────────────────────────────────────────────────────

/// Converts a `bool` slice to an Arrow [`BooleanArray`].
///
/// Arrow stores boolean values as a packed bit-vector; there is no
/// zero-copy path from a `&[bool]`, so the slice is always materialised
/// into a new allocation.
///
/// [`BooleanArray`]: arrow::array::BooleanArray
impl ArrowTypeConversion for bool {
    fn arrow_from_array_view(array: &[Self]) -> anyhow::Result<arrow::array::ArrayRef> {
        let arrow_array = arrow::array::BooleanArray::from(array.to_vec());
        Ok(Arc::new(arrow_array))
    }
}

// ── UTF-8 string ───────────────────────────────────────────────────────────

/// Converts a `String` slice to an Arrow [`StringArray`] (UTF-8, 32-bit
/// offsets).
///
/// Uses [`from_iter_values`] to avoid an intermediate `Vec<&str>` allocation.
///
/// [`StringArray`]: arrow::array::StringArray
/// [`from_iter_values`]: arrow::array::StringArray::from_iter_values
impl ArrowTypeConversion for String {
    fn arrow_from_array_view(array: &[Self]) -> anyhow::Result<arrow::array::ArrayRef> {
        // `from_iter_values` iterates over `&str` references directly,
        // avoiding an intermediate `Vec<&str>` allocation.
        let arrow_array =
            arrow::array::StringArray::from_iter_values(array.iter().map(String::as_str));
        Ok(Arc::new(arrow_array))
    }
}

// ── Chrono timestamps (feature = "ndarray") ──────────────────────────────
//
// Both impls are gated by `#[cfg(feature = "ndarray")]` because `chrono` is
// pulled in as an optional dependency of that very feature.

/// Converts a [`chrono::NaiveDateTime`] slice to an Arrow
/// `Timestamp(Nanosecond, None)` array.
///
/// `NaiveDateTime` carries no timezone information; the resulting Arrow array
/// is therefore annotated without a timezone (`None`), representing a
/// wall-clock / local time.
///
/// Each value is converted to `i64` nanoseconds since the Unix epoch
/// (`1970-01-01T00:00:00`) by interpreting the `NaiveDateTime` as UTC.
///
/// # Errors
///
/// Returns an error if any timestamp falls outside the range representable as
/// `i64` nanoseconds (approximately `1677-09-21` – `2262-04-11`).
#[cfg(feature = "ndarray")]
impl ArrowTypeConversion for chrono::NaiveDateTime {
    fn arrow_from_array_view(array: &[Self]) -> anyhow::Result<arrow::array::ArrayRef> {
        // Convert each NaiveDateTime → i64 nanoseconds; propagate the first
        // out-of-range error eagerly rather than silently saturating.
        let nanos = array
            .iter()
            .map(|dt| {
                dt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                    anyhow::anyhow!("chrono::NaiveDateTime out of range for i64 nanoseconds: {dt}")
                })
            })
            .collect::<anyhow::Result<Vec<i64>>>()?;

        // No timezone annotation — this is an intentional wall-clock array.
        let arrow_array = arrow::array::TimestampNanosecondArray::from(nanos);
        Ok(Arc::new(arrow_array))
    }
}

/// Converts a [`chrono::DateTime<chrono::Utc>`] slice to an Arrow
/// `Timestamp(Nanosecond, Some("UTC"))` array.
///
/// The `"UTC"` timezone annotation is preserved in the Arrow metadata so
/// downstream consumers can unambiguously distinguish UTC instants from
/// timezone-naive (wall-clock) timestamps produced by [`chrono::NaiveDateTime`].
///
/// # Errors
///
/// Returns an error if any timestamp falls outside the range representable as
/// `i64` nanoseconds (approximately `1677-09-21` – `2262-04-11`).
#[cfg(feature = "ndarray")]
impl ArrowTypeConversion for chrono::DateTime<chrono::Utc> {
    fn arrow_from_array_view(array: &[Self]) -> anyhow::Result<arrow::array::ArrayRef> {
        // Convert each DateTime<Utc> → i64 nanoseconds; propagate the first
        // out-of-range error eagerly rather than silently saturating.
        let nanos = array
            .iter()
            .map(|dt| {
                dt.timestamp_nanos_opt().ok_or_else(|| {
                    anyhow::anyhow!("chrono::DateTime<Utc> out of range for i64 nanoseconds: {dt}")
                })
            })
            .collect::<anyhow::Result<Vec<i64>>>()?;

        // Attach the explicit "UTC" timezone so Arrow consumers can
        // distinguish these instants from NaiveDateTime arrays.
        let arrow_array = arrow::array::TimestampNanosecondArray::from(nanos).with_timezone("UTC");
        Ok(Arc::new(arrow_array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };

    /// Helper: create a 1-D backend and call `slice`, blocking on the future.
    fn slice_backend<T: ArrowTypeConversion + Clone>(
        data: Vec<T>,
        start: usize,
        length: usize,
    ) -> arrow::array::ArrayRef {
        let arc = ndarray::ArcArrayD::from_shape_vec(vec![data.len()], data).unwrap();
        let backend = NdArrayBackend::new(arc, vec!["dim0".to_string()]);
        futures::executor::block_on(backend.slice(start, length)).unwrap()
    }

    // ── integer types ──────────────────────────────────────────────────────────

    #[test]
    fn test_i8() {
        let arr = slice_backend::<i8>(vec![-2, -1, 0, 1, 2], 1, 3);
        let typed = arr.as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(typed.values(), &[-1_i8, 0, 1]);
    }

    #[test]
    fn test_u8() {
        let arr = slice_backend::<u8>(vec![10, 20, 30, 40], 0, 4);
        let typed = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(typed.values(), &[10_u8, 20, 30, 40]);
    }

    #[test]
    fn test_i16() {
        let arr = slice_backend::<i16>(vec![-300, 0, 300], 1, 2);
        let typed = arr.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(typed.values(), &[0_i16, 300]);
    }

    #[test]
    fn test_u16() {
        let arr = slice_backend::<u16>(vec![1000, 2000, 3000], 2, 1);
        let typed = arr.as_any().downcast_ref::<UInt16Array>().unwrap();
        assert_eq!(typed.values(), &[3000_u16]);
    }

    #[test]
    fn test_i32() {
        let arr = slice_backend::<i32>(vec![1, 2, 3, 4, 5], 1, 3);
        let typed = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(typed.values(), &[2_i32, 3, 4]);
    }

    #[test]
    fn test_u32() {
        let arr = slice_backend::<u32>(vec![100, 200, 300], 0, 2);
        let typed = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(typed.values(), &[100_u32, 200]);
    }

    #[test]
    fn test_i64() {
        let arr = slice_backend::<i64>(vec![i64::MIN, 0, i64::MAX], 0, 3);
        let typed = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(typed.values(), &[i64::MIN, 0, i64::MAX]);
    }

    #[test]
    fn test_u64() {
        let arr = slice_backend::<u64>(vec![0, u64::MAX / 2, u64::MAX], 1, 2);
        let typed = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(typed.values(), &[u64::MAX / 2, u64::MAX]);
    }

    // ── floating-point types ───────────────────────────────────────────────────

    #[test]
    fn test_f32() {
        let arr = slice_backend::<f32>(vec![1.0, 2.5, 3.5, 4.0], 1, 2);
        let typed = arr.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(typed.len(), 2);
        assert!((typed.value(0) - 2.5_f32).abs() < f32::EPSILON);
        assert!((typed.value(1) - 3.5_f32).abs() < f32::EPSILON);
    }

    #[test]
    fn test_f64() {
        let arr = slice_backend::<f64>(vec![1.0, 2.0, 3.0, 4.0], 1, 2);
        let typed = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(typed.len(), 2);
        assert_eq!(typed.value(0), 2.0_f64);
        assert_eq!(typed.value(1), 3.0_f64);
    }

    // ── boolean type ───────────────────────────────────────────────────────────

    #[test]
    fn test_bool() {
        let arr = slice_backend::<bool>(vec![true, false, true, false], 1, 2);
        let typed = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(typed.len(), 2);
        assert!(!typed.value(0));
        assert!(typed.value(1));
    }

    // ── edge-case / error-path tests ───────────────────────────────────────────

    #[test]
    fn test_slice_entire_array() {
        let arr = slice_backend::<i32>(vec![7, 8, 9], 0, 3);
        let typed = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(typed.len(), 3);
    }

    #[test]
    fn test_slice_zero_length() {
        let arr = slice_backend::<i32>(vec![1, 2, 3], 1, 0);
        let typed = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(typed.len(), 0);
    }

    #[test]
    fn test_slice_out_of_bounds_returns_error() {
        let arc = ndarray::ArcArrayD::from_shape_vec(vec![3], vec![1_i32, 2, 3]).unwrap();
        let backend = NdArrayBackend::new(arc, vec!["dim0".to_string()]);
        let result = futures::executor::block_on(backend.slice(2, 5));
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("out of bounds"), "unexpected error: {msg}");
    }

    #[test]
    fn test_is_empty() {
        let arc = ndarray::ArcArrayD::from_shape_vec(vec![0], Vec::<i32>::new()).unwrap();
        let backend = NdArrayBackend::new(arc, vec!["dim0".to_string()]);
        assert!(backend.is_empty());
    }

    #[test]
    fn test_shape_and_dimensions() {
        let arc = ndarray::ArcArrayD::from_shape_vec(vec![2, 3], vec![0_i32; 6]).unwrap();
        let backend = NdArrayBackend::new(arc, vec!["row".to_string(), "col".to_string()]);
        assert_eq!(backend.shape(), vec![2, 3]);
        assert_eq!(backend.dimensions(), vec!["row", "col"]);
        assert_eq!(backend.len(), 6);
    }

    // ── String type ────────────────────────────────────────────────────────────

    #[test]
    fn test_string() {
        let data = vec!["hello".to_string(), "world".to_string(), "foo".to_string()];
        let arr = slice_backend::<String>(data, 0, 3);
        let typed = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(typed.len(), 3);
        assert_eq!(typed.value(0), "hello");
        assert_eq!(typed.value(1), "world");
        assert_eq!(typed.value(2), "foo");
    }

    #[test]
    fn test_string_slice() {
        let data = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let arr = slice_backend::<String>(data, 1, 2);
        let typed = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(typed.value(0), "b");
        assert_eq!(typed.value(1), "c");
    }

    // ── chrono timestamp types ─────────────────────────────────────────────────

    #[cfg(feature = "ndarray")]
    #[test]
    fn test_naive_datetime() {
        use chrono::NaiveDateTime;

        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        let one_sec = chrono::DateTime::from_timestamp(1, 0).unwrap().naive_utc();
        let data = vec![epoch, one_sec];

        let arr = slice_backend::<NaiveDateTime>(data, 0, 2);
        let typed = arr
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();

        // No timezone on NaiveDateTime arrays.
        assert!(matches!(
            typed.data_type(),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        ));
        assert_eq!(typed.value(0), 0);
        assert_eq!(typed.value(1), 1_000_000_000);
    }

    #[cfg(feature = "ndarray")]
    #[test]
    fn test_datetime_utc() {
        use chrono::{DateTime, TimeZone, Utc};

        let epoch: DateTime<Utc> = Utc.timestamp_opt(0, 0).unwrap();
        let t1: DateTime<Utc> = Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap();
        let data = vec![epoch, t1];

        let arr = slice_backend::<DateTime<Utc>>(data, 0, 2);
        let typed = arr
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();

        // DateTime<Utc> arrays carry the "UTC" timezone.
        assert!(matches!(
            typed.data_type(),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some(_))
        ));
        if let arrow::datatypes::DataType::Timestamp(_, Some(tz)) = typed.data_type() {
            assert_eq!(tz.as_ref(), "UTC");
        }

        assert_eq!(typed.value(0), 0);
        assert_eq!(
            typed.value(1),
            1_700_000_000_i64 * 1_000_000_000 + 123_456_789
        );
    }

    #[cfg(feature = "ndarray")]
    #[test]
    fn test_datetime_utc_slice() {
        use chrono::{DateTime, TimeZone, Utc};

        let times: Vec<DateTime<Utc>> = (0..5)
            .map(|i| Utc.timestamp_opt(i * 60, 0).unwrap())
            .collect();
        let arr = slice_backend::<DateTime<Utc>>(times, 1, 3);
        let typed = arr
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();

        assert_eq!(typed.len(), 3);
        assert_eq!(typed.value(0), 60_i64 * 1_000_000_000);
        assert_eq!(typed.value(1), 120_i64 * 1_000_000_000);
        assert_eq!(typed.value(2), 180_i64 * 1_000_000_000);
    }

    #[test]
    fn test_arc_array_backend() {
        let arc = ndarray::ArcArrayD::from_shape_vec(vec![3], vec![1_i32, 2, 3]).unwrap();
        let backend =
            Arc::new(NdArrayBackend::new(arc, vec!["dim0".to_string()])) as Arc<dyn ArrayBackend>;
        assert_eq!(backend.len(), 3);
        assert_eq!(backend.shape(), vec![3]);
        assert_eq!(backend.dimensions(), vec!["dim0".to_string()]);
    }
}
