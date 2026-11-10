//! One file's statistics for one column, and the way back to an Arrow array.
//!
//! A [`ColumnStat`] keeps its min and max as [`StatScalar`]s, and the declared
//! type next to them. Each row keeps its own type, so files that disagree on a
//! column type need no common type at write time.

use arrow::array::{Array, ArrayData, ArrayRef, AsArray, make_array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};

use super::error::{Result, StoreError};

/// One file's statistics for one column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStat {
    pub min: StatScalar,
    pub max: StatScalar,
    /// `None` when the format did not report it.
    ///
    /// Not zero. DataFusion prunes `IS NOT NULL` on `null_count != row_count`,
    /// so a pair of unknowns written as zeroes reads as "every value is null"
    /// and drops a file full of values.
    pub null_count: Option<u64>,
    /// `None` when the format did not report it. netCDF never does.
    pub row_count: Option<u64>,
    /// The type of the column in this file, in its storage form (see [`storage_type`]).
    pub data_type: DataType,
}

impl ColumnStat {
    /// Builds a statistic from the one-element min and max arrays that a format reports.
    ///
    /// # Returns
    ///
    /// `None` when `data_type` has no storage form, because such a range cannot prune.
    pub fn from_arrays(
        min: Option<&ArrayRef>,
        max: Option<&ArrayRef>,
        null_count: Option<u64>,
        row_count: Option<u64>,
        data_type: &DataType,
    ) -> Option<Self> {
        let scalar = |array: Option<&ArrayRef>| {
            array.map_or(StatScalar::Absent, |array| StatScalar::from_array(array, 0))
        };
        Some(Self {
            min: scalar(min),
            max: scalar(max),
            null_count,
            row_count,
            data_type: storage_type(data_type)?,
        })
    }
}

/// One min or max value. The logical type is kept beside it, in [`ColumnStat`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StatScalar {
    /// The statistic is absent for this file. Distinct from a value of null.
    Absent,
    Bool(bool),
    /// Every signed integer, and every date, time, timestamp, and duration.
    /// The logical type travels beside the value, not inside it.
    I64(i64),
    U64(u64),
    F64(f64),
    I128(i128),
    /// Utf8 and binary, in their source bytes.
    Bytes(Vec<u8>),
}

impl StatScalar {
    /// Read element `index` out of a statistics array.
    ///
    /// Beacon's format layer reports min/max as one-element arrays (as
    /// `FileFormat::infer_stats` does), so `index` is normally 0.
    pub fn from_array(array: &ArrayRef, index: usize) -> Self {
        if index >= array.len() || array.is_null(index) {
            return Self::Absent;
        }
        match array.data_type() {
            DataType::Boolean => Self::Bool(array.as_boolean().value(index)),

            DataType::Int8 => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Int8Type>()
                    .value(index) as i64,
            ),
            DataType::Int16 => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Int16Type>()
                    .value(index) as i64,
            ),
            DataType::Int32 => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .value(index) as i64,
            ),
            DataType::Int64 => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(index),
            ),

            DataType::UInt8 => Self::U64(
                array
                    .as_primitive::<arrow::datatypes::UInt8Type>()
                    .value(index) as u64,
            ),
            DataType::UInt16 => Self::U64(
                array
                    .as_primitive::<arrow::datatypes::UInt16Type>()
                    .value(index) as u64,
            ),
            DataType::UInt32 => Self::U64(
                array
                    .as_primitive::<arrow::datatypes::UInt32Type>()
                    .value(index) as u64,
            ),
            DataType::UInt64 => Self::U64(
                array
                    .as_primitive::<arrow::datatypes::UInt64Type>()
                    .value(index),
            ),

            DataType::Float16 => Self::F64(
                array
                    .as_primitive::<arrow::datatypes::Float16Type>()
                    .value(index)
                    .to_f64(),
            ),
            DataType::Float32 => Self::F64(
                array
                    .as_primitive::<arrow::datatypes::Float32Type>()
                    .value(index) as f64,
            ),
            DataType::Float64 => Self::F64(
                array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(index),
            ),

            DataType::Date32 => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Date32Type>()
                    .value(index) as i64,
            ),
            DataType::Date64 => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Date64Type>()
                    .value(index),
            ),
            DataType::Time32(TimeUnit::Second) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Time32SecondType>()
                    .value(index) as i64,
            ),
            DataType::Time32(TimeUnit::Millisecond) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Time32MillisecondType>()
                    .value(index) as i64,
            ),
            DataType::Time64(TimeUnit::Microsecond) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Time64MicrosecondType>()
                    .value(index),
            ),
            DataType::Time64(TimeUnit::Nanosecond) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::Time64NanosecondType>()
                    .value(index),
            ),
            DataType::Timestamp(TimeUnit::Second, _) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::TimestampSecondType>()
                    .value(index),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                    .value(index),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                    .value(index),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                    .value(index),
            ),
            DataType::Duration(TimeUnit::Second) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::DurationSecondType>()
                    .value(index),
            ),
            DataType::Duration(TimeUnit::Millisecond) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::DurationMillisecondType>()
                    .value(index),
            ),
            DataType::Duration(TimeUnit::Microsecond) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::DurationMicrosecondType>()
                    .value(index),
            ),
            DataType::Duration(TimeUnit::Nanosecond) => Self::I64(
                array
                    .as_primitive::<arrow::datatypes::DurationNanosecondType>()
                    .value(index),
            ),

            DataType::Decimal128(_, _) => Self::I128(
                array
                    .as_primitive::<arrow::datatypes::Decimal128Type>()
                    .value(index),
            ),

            DataType::Utf8 => {
                Self::Bytes(array.as_string::<i32>().value(index).as_bytes().to_vec())
            }
            DataType::LargeUtf8 => {
                Self::Bytes(array.as_string::<i64>().value(index).as_bytes().to_vec())
            }
            DataType::Utf8View => {
                Self::Bytes(array.as_string_view().value(index).as_bytes().to_vec())
            }
            DataType::Binary => Self::Bytes(array.as_binary::<i32>().value(index).to_vec()),
            DataType::LargeBinary => Self::Bytes(array.as_binary::<i64>().value(index).to_vec()),
            DataType::BinaryView => Self::Bytes(array.as_binary_view().value(index).to_vec()),

            // Nested, interval, and everything else carries no useful range.
            // Absent is honest: the reader then prunes nothing on this column.
            _ => Self::Absent,
        }
    }
}

/// The form in which the store keeps a column type, or `None` when it keeps no range for it.
///
/// The view and half-width types collapse onto their plain forms: a statistic
/// is one value, so the view layout buys nothing.
pub(crate) fn storage_type(data_type: &DataType) -> Option<DataType> {
    let normalized = match data_type {
        DataType::Float16 => DataType::Float64,
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        other => other.clone(),
    };
    match &normalized {
        DataType::Boolean
        | DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary => Some(normalized),
        other if fixed_width(other).is_some() => Some(normalized),
        _ => None,
    }
}

/// The byte width of a fixed-size storage type.
fn fixed_width(data_type: &DataType) -> Option<usize> {
    use DataType::*;
    Some(match data_type {
        Int8 | UInt8 => 1,
        Int16 | UInt16 => 2,
        Int32 | UInt32 | Float32 | Date32 | Time32(_) => 4,
        Int64 | UInt64 | Float64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_) => 8,
        Decimal128(_, _) => 16,
        _ => return None,
    })
}

/// Builds an array of `data_type` from `values`. An absent or mismatched value is null.
///
/// `data_type` is the storage type that each value came from, so each narrowing
/// cast here is exact.
///
/// # Errors
///
/// Returns an error when `data_type` has no storage form, or the array is invalid.
pub(crate) fn scalars_to_array(values: &[StatScalar], data_type: &DataType) -> Result<ArrayRef> {
    let mut valid = ValidityBuilder::new(values.len());
    let buffers = match data_type {
        DataType::Boolean => {
            let mut bits = vec![0u8; values.len().div_ceil(8)];
            for (index, value) in values.iter().enumerate() {
                let StatScalar::Bool(value) = value else {
                    valid.push(false);
                    continue;
                };
                if *value {
                    bits[index / 8] |= 1 << (index % 8);
                }
                valid.push(true);
            }
            vec![Buffer::from_vec(bits)]
        }
        DataType::Utf8 | DataType::Binary => byte_buffers::<i32>(values, &mut valid)?,
        DataType::LargeUtf8 | DataType::LargeBinary => byte_buffers::<i64>(values, &mut valid)?,
        other => {
            let width = fixed_width(other)
                .ok_or_else(|| StoreError::Codec(format!("no statistics storage for {other}")))?;
            let mut bytes = Vec::with_capacity(values.len() * width);
            for value in values {
                let fits = push_fixed(&mut bytes, value, other);
                if !fits {
                    bytes.resize(bytes.len() + width, 0);
                }
                valid.push(fits);
            }
            vec![Buffer::from_vec(bytes)]
        }
    };
    let data = ArrayData::builder(data_type.clone())
        .len(values.len())
        .buffers(buffers)
        .null_bit_buffer(valid.finish().map(Buffer::from_vec))
        .build()?;
    Ok(make_array(data))
}

/// Appends `value` in the little-endian form of `data_type`.
///
/// # Returns
///
/// `false`, with nothing appended, when `value` does not belong to `data_type`.
fn push_fixed(bytes: &mut Vec<u8>, value: &StatScalar, data_type: &DataType) -> bool {
    use DataType::*;
    use StatScalar as S;
    // `as` narrows without a check; the value was read from this same type.
    match (data_type, value) {
        (Int8, S::I64(x)) => bytes.extend_from_slice(&(*x as i8).to_le_bytes()),
        (Int16, S::I64(x)) => bytes.extend_from_slice(&(*x as i16).to_le_bytes()),
        (Int32 | Date32 | Time32(_), S::I64(x)) => {
            bytes.extend_from_slice(&(*x as i32).to_le_bytes());
        }
        (Int64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_), S::I64(x)) => {
            bytes.extend_from_slice(&x.to_le_bytes());
        }
        (UInt8, S::U64(x)) => bytes.extend_from_slice(&(*x as u8).to_le_bytes()),
        (UInt16, S::U64(x)) => bytes.extend_from_slice(&(*x as u16).to_le_bytes()),
        (UInt32, S::U64(x)) => bytes.extend_from_slice(&(*x as u32).to_le_bytes()),
        (UInt64, S::U64(x)) => bytes.extend_from_slice(&x.to_le_bytes()),
        (Float32, S::F64(x)) => bytes.extend_from_slice(&(*x as f32).to_le_bytes()),
        (Float64, S::F64(x)) => bytes.extend_from_slice(&x.to_le_bytes()),
        (Decimal128(_, _), S::I128(x)) => bytes.extend_from_slice(&x.to_le_bytes()),
        _ => return false,
    }
    true
}

/// The offset and data buffers of a string or binary array.
///
/// `O` is the offset type: `i32` for `Utf8` and `Binary`, `i64` for the large forms.
fn byte_buffers<O>(values: &[StatScalar], valid: &mut ValidityBuilder) -> Result<Vec<Buffer>>
where
    O: TryFrom<usize> + arrow::datatypes::ArrowNativeType,
{
    let offset = |position: usize| {
        O::try_from(position)
            .map_err(|_| StoreError::Codec("statistics values exceed the offset width".into()))
    };
    let mut data: Vec<u8> = Vec::new();
    let mut offsets: Vec<O> = Vec::with_capacity(values.len() + 1);
    offsets.push(offset(0)?);
    for value in values {
        let bytes = match value {
            StatScalar::Bytes(bytes) => Some(bytes),
            _ => None,
        };
        if let Some(bytes) = bytes {
            data.extend_from_slice(bytes);
        }
        valid.push(bytes.is_some());
        offsets.push(offset(data.len())?);
    }
    Ok(vec![Buffer::from_vec(offsets), Buffer::from_vec(data)])
}

/// Packs an Arrow validity bitmap, least significant bit first.
struct ValidityBuilder {
    bits: Vec<u8>,
    len: usize,
    any_null: bool,
}

impl ValidityBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            bits: vec![0u8; capacity.div_ceil(8)],
            len: 0,
            any_null: false,
        }
    }

    fn push(&mut self, valid: bool) {
        if valid {
            self.bits[self.len / 8] |= 1 << (self.len % 8);
        } else {
            self.any_null = true;
        }
        self.len += 1;
    }

    /// The bitmap, or `None` when each value is valid.
    fn finish(self) -> Option<Vec<u8>> {
        self.any_null.then_some(self.bits)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BooleanArray, Decimal128Array, Float32Array, Int16Array, StringArray,
        TimestampNanosecondArray, UInt8Array,
    };

    use super::*;

    /// Reads `array` back through `from_array` and `scalars_to_array`.
    fn round_trip(array: ArrayRef) -> ArrayRef {
        let data_type = storage_type(array.data_type()).expect("a storage type");
        let values: Vec<StatScalar> = (0..array.len())
            .map(|index| StatScalar::from_array(&array, index))
            .collect();
        scalars_to_array(&values, &data_type).expect("an array")
    }

    #[test]
    fn each_storage_class_survives_a_round_trip() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int16Array::from(vec![Some(-7), None, Some(3)])),
            Arc::new(UInt8Array::from(vec![Some(200), Some(1)])),
            Arc::new(Float32Array::from(vec![Some(0.5), None])),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(1_700_000_000_000_000_000)])
                    .with_timezone("UTC"),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(12_345), None])
                    .with_precision_and_scale(10, 2)
                    .expect("a valid decimal"),
            ),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(StringArray::from(vec![Some("ab"), None, Some("")])),
        ];
        for array in arrays {
            assert_eq!(&round_trip(Arc::clone(&array)), &array);
        }
    }

    #[test]
    fn a_value_of_another_class_reads_as_null() {
        let values = [StatScalar::Bytes(b"x".to_vec()), StatScalar::I64(4)];

        let array = scalars_to_array(&values, &DataType::Int64).expect("an array");

        assert_eq!(array.null_count(), 1);
        assert!(array.is_valid(1));
    }

    #[test]
    fn view_and_half_types_keep_their_plain_form() {
        assert_eq!(storage_type(&DataType::Utf8View), Some(DataType::Utf8));
        assert_eq!(storage_type(&DataType::Float16), Some(DataType::Float64));
        assert_eq!(storage_type(&DataType::Null), None);
    }

    #[test]
    fn a_type_with_no_storage_form_gives_no_stat() {
        let stat = ColumnStat::from_arrays(None, None, Some(0), Some(1), &DataType::Null);
        assert!(stat.is_none());
    }

    #[test]
    fn reads_scalars_out_of_one_element_arrays() {
        let ints: ArrayRef = Arc::new(Int16Array::from(vec![Some(-7)]));
        assert_eq!(StatScalar::from_array(&ints, 0), StatScalar::I64(-7));

        let floats: ArrayRef = Arc::new(Float32Array::from(vec![Some(0.5)]));
        assert_eq!(StatScalar::from_array(&floats, 0), StatScalar::F64(0.5));

        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("ab")]));
        assert_eq!(
            StatScalar::from_array(&strings, 0),
            StatScalar::Bytes(b"ab".to_vec())
        );
    }

    #[test]
    fn a_null_or_out_of_range_element_is_absent() {
        let ints: ArrayRef = Arc::new(Int16Array::from(vec![None]));
        assert_eq!(StatScalar::from_array(&ints, 0), StatScalar::Absent);
        assert_eq!(StatScalar::from_array(&ints, 5), StatScalar::Absent);
    }
}
