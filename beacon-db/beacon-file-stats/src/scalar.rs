//! The build-time statistic value, and the super-type rules that make a block
//! homogeneous.
//!
//! # Why a tagged enum here and not on disk
//!
//! [`StatScalar`] carries its own tag, which costs ~16 bytes per value. On disk
//! that would be the wrong trade: at 50M cells a tagged min/max pair costs 32
//! bytes where a typed `Float64` pair costs 16, and a narrow `Int16` pair costs
//! 4. It also cannot delta-encode or share a validity bitmap.
//!
//! In the builder the trade reverses. A batch holds ~500K cells, so the tag
//! costs a few MB and buys one uniform representation across a column whose
//! files disagree about type. [`SegmentBuilder`](crate::segment::SegmentBuilder)
//! resolves the disagreement once, at finish, with [`super_type`], then writes
//! narrow typed buffers.

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

/// One min or max value, held only while a segment is under construction.
#[derive(Debug, Clone, PartialEq)]
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

            DataType::Int8 => Self::I64(array.as_primitive::<arrow::datatypes::Int8Type>().value(index) as i64),
            DataType::Int16 => Self::I64(array.as_primitive::<arrow::datatypes::Int16Type>().value(index) as i64),
            DataType::Int32 => Self::I64(array.as_primitive::<arrow::datatypes::Int32Type>().value(index) as i64),
            DataType::Int64 => Self::I64(array.as_primitive::<arrow::datatypes::Int64Type>().value(index)),

            DataType::UInt8 => Self::U64(array.as_primitive::<arrow::datatypes::UInt8Type>().value(index) as u64),
            DataType::UInt16 => Self::U64(array.as_primitive::<arrow::datatypes::UInt16Type>().value(index) as u64),
            DataType::UInt32 => Self::U64(array.as_primitive::<arrow::datatypes::UInt32Type>().value(index) as u64),
            DataType::UInt64 => Self::U64(array.as_primitive::<arrow::datatypes::UInt64Type>().value(index)),

            DataType::Float16 => {
                Self::F64(array.as_primitive::<arrow::datatypes::Float16Type>().value(index).to_f64())
            }
            DataType::Float32 => {
                Self::F64(array.as_primitive::<arrow::datatypes::Float32Type>().value(index) as f64)
            }
            DataType::Float64 => Self::F64(array.as_primitive::<arrow::datatypes::Float64Type>().value(index)),

            DataType::Date32 => Self::I64(array.as_primitive::<arrow::datatypes::Date32Type>().value(index) as i64),
            DataType::Date64 => Self::I64(array.as_primitive::<arrow::datatypes::Date64Type>().value(index)),
            DataType::Time32(TimeUnit::Second) => {
                Self::I64(array.as_primitive::<arrow::datatypes::Time32SecondType>().value(index) as i64)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                Self::I64(array.as_primitive::<arrow::datatypes::Time32MillisecondType>().value(index) as i64)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Self::I64(array.as_primitive::<arrow::datatypes::Time64MicrosecondType>().value(index))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::I64(array.as_primitive::<arrow::datatypes::Time64NanosecondType>().value(index))
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                Self::I64(array.as_primitive::<arrow::datatypes::TimestampSecondType>().value(index))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Self::I64(array.as_primitive::<arrow::datatypes::TimestampMillisecondType>().value(index))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::I64(array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>().value(index))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::I64(array.as_primitive::<arrow::datatypes::TimestampNanosecondType>().value(index))
            }
            DataType::Duration(TimeUnit::Second) => {
                Self::I64(array.as_primitive::<arrow::datatypes::DurationSecondType>().value(index))
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                Self::I64(array.as_primitive::<arrow::datatypes::DurationMillisecondType>().value(index))
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Self::I64(array.as_primitive::<arrow::datatypes::DurationMicrosecondType>().value(index))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Self::I64(array.as_primitive::<arrow::datatypes::DurationNanosecondType>().value(index))
            }

            DataType::Decimal128(_, _) => {
                Self::I128(array.as_primitive::<arrow::datatypes::Decimal128Type>().value(index))
            }

            DataType::Utf8 => Self::Bytes(array.as_string::<i32>().value(index).as_bytes().to_vec()),
            DataType::LargeUtf8 => Self::Bytes(array.as_string::<i64>().value(index).as_bytes().to_vec()),
            DataType::Utf8View => Self::Bytes(array.as_string_view().value(index).as_bytes().to_vec()),
            DataType::Binary => Self::Bytes(array.as_binary::<i32>().value(index).to_vec()),
            DataType::LargeBinary => Self::Bytes(array.as_binary::<i64>().value(index).to_vec()),
            DataType::BinaryView => Self::Bytes(array.as_binary_view().value(index).to_vec()),

            // Nested, interval, and everything else carries no useful range.
            // Absent is honest: the reader then prunes nothing on this column.
            _ => Self::Absent,
        }
    }

    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }
}

/// The narrowest type that represents both `a` and `b` without changing the
/// meaning of a range comparison.
///
/// `None` means the two cannot share a block. That costs pruning on the column,
/// never correctness: the reader treats a missing statistic as unknown.
///
/// The rules stay conservative on purpose. `Timestamp` values with different
/// units or zones do not merge, because rescaling a range silently at write time
/// is how a pruning index starts dropping real rows.
pub fn super_type(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;

    if a == b {
        return Some(a.clone());
    }

    match (a, b) {
        (Null, other) | (other, Null) => Some(other.clone()),

        // Signed integers widen to the wider of the two.
        (Int8 | Int16 | Int32 | Int64, Int8 | Int16 | Int32 | Int64) => {
            Some(wider(int_rank(a), int_rank(b), &[Int8, Int16, Int32, Int64]))
        }
        // Unsigned likewise.
        (UInt8 | UInt16 | UInt32 | UInt64, UInt8 | UInt16 | UInt32 | UInt64) => {
            Some(wider(uint_rank(a), uint_rank(b), &[UInt8, UInt16, UInt32, UInt64]))
        }
        // Mixed signedness needs a signed type wide enough for the unsigned
        // side. `UInt64` does not fit in `Int64`, so that pair goes to `Float64`
        // and accepts the precision loss, which widens a range rather than
        // narrowing it.
        (Int8 | Int16 | Int32 | Int64, UInt8 | UInt16 | UInt32)
        | (UInt8 | UInt16 | UInt32, Int8 | Int16 | Int32 | Int64) => Some(Int64),
        (Int8 | Int16 | Int32 | Int64, UInt64) | (UInt64, Int8 | Int16 | Int32 | Int64) => {
            Some(Float64)
        }

        // Any float in the pair pulls the result to a float.
        (Float16 | Float32 | Float64, Float16 | Float32 | Float64) => Some(Float64),
        (
            Float16 | Float32 | Float64,
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64,
        )
        | (
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64,
            Float16 | Float32 | Float64,
        ) => Some(Float64),

        (Date32, Date64) | (Date64, Date32) => Some(Date64),

        (Utf8, LargeUtf8) | (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8 | LargeUtf8 | Utf8View, Utf8View) | (Utf8View, Utf8 | LargeUtf8) => Some(LargeUtf8),
        (Binary, LargeBinary) | (LargeBinary, Binary) => Some(LargeBinary),
        (Binary | LargeBinary | BinaryView, BinaryView) | (BinaryView, Binary | LargeBinary) => {
            Some(LargeBinary)
        }

        // Deliberately unmerged: Timestamp across units or zones, Decimal across
        // precision or scale, Interval, and every nested type.
        (Interval(_), _) | (_, Interval(_)) => None,
        _ => None,
    }
}

fn int_rank(t: &DataType) -> usize {
    match t {
        DataType::Int8 => 0,
        DataType::Int16 => 1,
        DataType::Int32 => 2,
        _ => 3,
    }
}

fn uint_rank(t: &DataType) -> usize {
    match t {
        DataType::UInt8 => 0,
        DataType::UInt16 => 1,
        DataType::UInt32 => 2,
        _ => 3,
    }
}

fn wider(a: usize, b: usize, ladder: &[DataType]) -> DataType {
    ladder[a.max(b)].clone()
}

/// Whether `t` is a type this crate stores statistics for at all.
pub fn is_supported(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Timestamp(_, _)
            | Duration(_)
            | Decimal128(_, _)
            | Utf8
            | LargeUtf8
            | Utf8View
            | Binary
            | LargeBinary
            | BinaryView
    ) && !matches!(t, Interval(IntervalUnit::DayTime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Int16Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn reads_scalars_out_of_one_element_arrays() {
        let ints: ArrayRef = Arc::new(Int16Array::from(vec![Some(-7)]));
        assert_eq!(StatScalar::from_array(&ints, 0), StatScalar::I64(-7));

        let floats: ArrayRef = Arc::new(Float32Array::from(vec![Some(0.5)]));
        assert_eq!(StatScalar::from_array(&floats, 0), StatScalar::F64(0.5));

        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("ab")]));
        assert_eq!(StatScalar::from_array(&strings, 0), StatScalar::Bytes(b"ab".to_vec()));
    }

    #[test]
    fn a_null_or_out_of_range_element_is_absent() {
        let ints: ArrayRef = Arc::new(Int16Array::from(vec![None]));
        assert!(StatScalar::from_array(&ints, 0).is_absent());
        assert!(StatScalar::from_array(&ints, 5).is_absent());
    }

    #[test]
    fn integers_widen_and_floats_dominate() {
        assert_eq!(super_type(&DataType::Int16, &DataType::Int64), Some(DataType::Int64));
        assert_eq!(super_type(&DataType::UInt8, &DataType::UInt32), Some(DataType::UInt32));
        assert_eq!(super_type(&DataType::Int16, &DataType::Float32), Some(DataType::Float64));
        assert_eq!(super_type(&DataType::Int32, &DataType::UInt16), Some(DataType::Int64));
    }

    /// `UInt64` does not fit in `Int64`. Widening to `Float64` keeps the range a
    /// superset, which is the only direction pruning may err in.
    #[test]
    fn unsigned_sixty_four_escapes_to_float() {
        assert_eq!(super_type(&DataType::Int64, &DataType::UInt64), Some(DataType::Float64));
    }

    #[test]
    fn strings_and_binaries_widen_within_their_family() {
        assert_eq!(super_type(&DataType::Utf8, &DataType::LargeUtf8), Some(DataType::LargeUtf8));
        assert_eq!(
            super_type(&DataType::Binary, &DataType::LargeBinary),
            Some(DataType::LargeBinary)
        );
    }

    /// Rescaling a timestamp range at write time can drop real rows. The pair
    /// stays unmerged, and the column simply loses pruning in that segment.
    #[test]
    fn timestamps_with_different_units_do_not_merge() {
        let ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert_eq!(super_type(&ms, &ns), None);
        assert_eq!(super_type(&ms, &ms), Some(ms));
    }

    #[test]
    fn unrelated_families_do_not_merge() {
        assert_eq!(super_type(&DataType::Utf8, &DataType::Int64), None);
    }
}
