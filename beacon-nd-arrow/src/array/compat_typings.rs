use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::ByteArrayType;
use std::{fmt::Debug, sync::Arc};

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
/// | `TimestampNanosecond`             | `Timestamp(Nanosecond, None)`             |
/// | `bool`                           | `BooleanArray`                            |
/// | `String`                         | `StringArray` (UTF-8, 32-bit offsets)     |
/// | `chrono::NaiveDateTime`          | `Timestamp(Nanosecond, None)`             |
/// | `chrono::DateTime<chrono::Utc>`  | `Timestamp(Nanosecond, Some("UTC"))`      |
///
/// [`ArrayRef`]: arrow::array::ArrayRef
pub trait ArrowTypeConversion: Debug + Clone + Send + Sync + PartialEq + 'static {
    fn data_type() -> arrow::datatypes::DataType
    where
        Self: Sized;
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

    /// Convert a contiguous slice of `Self` values into an Arrow [`ArrayRef`],
    /// treating any element equal to `fill_value` as a null.
    ///
    /// # Memory efficiency
    ///
    /// The underlying data buffer is **shared** with the result of
    /// [`arrow_from_array_view`] — no second copy of the values is made.
    /// Null information is encoded as a compact validity bitmap
    /// (1 bit per element ≈ `n / 8` bytes of overhead).
    ///
    /// When no elements match `fill_value` the result is identical to calling
    /// [`arrow_from_array_view`] directly.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by [`arrow_from_array_view`].
    ///
    /// [`arrow_from_array_view`]: ArrowTypeConversion::arrow_from_array_view
    fn arrow_from_array_view_with_fill(
        array: &[Self],
        fill_value: &Self,
    ) -> anyhow::Result<arrow::array::ArrayRef>
    where
        Self: Sized,
    {
        // Build the validity bitmap: `true` (1) = valid, `false` (0) = null.
        // This is a packed bit-vector — one bit per element.
        let validity =
            arrow::buffer::BooleanBuffer::from_iter(array.iter().map(|v| v != fill_value));
        let null_buffer = arrow::buffer::NullBuffer::new(validity);
        let null_count = null_buffer.null_count();

        // Obtain the base Arrow array (data buffer only, all slots valid).
        let base = Self::arrow_from_array_view(array)?;

        // Short-circuit: if nothing matched the fill value, return as-is.
        if null_count == 0 {
            return Ok(base);
        }

        // Attach the null bitmap to the existing ArrayData without copying
        // the value buffer.  `to_data()` does a shallow clone — the inner
        // byte buffers are Arc-backed so no data is duplicated.  We then
        // re-assemble the ArrayData with the null bitmap grafted on.
        let new_data = base
            .to_data()
            .into_builder()
            .nulls(Some(null_buffer))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to attach null buffer: {e}"))?;

        Ok(arrow::array::make_array(new_data))
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
    ($rust_ty:ty, $arrow_array_ty:ty, $arrow_ty:ty) => {
        impl ArrowTypeConversion for $rust_ty {
            fn arrow_from_array_view(array: &[Self]) -> anyhow::Result<arrow::array::ArrayRef> {
                let arrow_array = <$arrow_array_ty>::from(array.to_vec());
                Ok(Arc::new(arrow_array))
            }
            fn data_type() -> arrow::datatypes::DataType {
                <$arrow_ty>::DATA_TYPE.clone()
            }
        }
    };
}

// ── Signed integers ────────────────────────────────────────────────────────
impl_arrow_type_conversion_primitive!(i8, arrow::array::Int8Array, arrow::datatypes::Int8Type);
impl_arrow_type_conversion_primitive!(i16, arrow::array::Int16Array, arrow::datatypes::Int16Type);
impl_arrow_type_conversion_primitive!(i32, arrow::array::Int32Array, arrow::datatypes::Int32Type);
impl_arrow_type_conversion_primitive!(i64, arrow::array::Int64Array, arrow::datatypes::Int64Type);

// ── Unsigned integers ──────────────────────────────────────────────────────
impl_arrow_type_conversion_primitive!(u8, arrow::array::UInt8Array, arrow::datatypes::UInt8Type);
impl_arrow_type_conversion_primitive!(u16, arrow::array::UInt16Array, arrow::datatypes::UInt16Type);
impl_arrow_type_conversion_primitive!(u32, arrow::array::UInt32Array, arrow::datatypes::UInt32Type);
impl_arrow_type_conversion_primitive!(u64, arrow::array::UInt64Array, arrow::datatypes::UInt64Type);

// ── Floating-point ─────────────────────────────────────────────────────────
impl_arrow_type_conversion_primitive!(
    f32,
    arrow::array::Float32Array,
    arrow::datatypes::Float32Type
);
impl_arrow_type_conversion_primitive!(
    f64,
    arrow::array::Float64Array,
    arrow::datatypes::Float64Type
);

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
    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::BooleanType::DATA_TYPE.clone()
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
    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::Utf8Type::DATA_TYPE.clone()
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

    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::TimestampNanosecondType::DATA_TYPE.clone()
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

    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::TimestampNanosecondType::DATA_TYPE.clone()
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct TimestampNanosecond(i64);

impl ArrowTypeConversion for TimestampNanosecond {
    fn arrow_from_array_view(array: &[Self]) -> anyhow::Result<arrow::array::ArrayRef>
    where
        Self: Sized,
    {
        // SAFETY: TimestampNanosecond is #[repr(transparent)] over an i64 and
        // implements bytemuck::Pod, so this is a safe zero-copy transmutation.
        let nanos: &[i64] = bytemuck::try_cast_slice(array)
            .map_err(|e| anyhow::anyhow!("Zero cost copy transmutation failed: {e}"))?;
        let arrow_array = arrow::array::TimestampNanosecondArray::from(nanos.to_vec());
        Ok(Arc::new(arrow_array))
    }

    fn data_type() -> arrow::datatypes::DataType
    where
        Self: Sized,
    {
        arrow::datatypes::TimestampNanosecondType::DATA_TYPE.clone()
    }
}
