//! The rule for the casts that keep each value. The prune and the statistics use it.

use arrow::datatypes::{DataType, TimeUnit};

/// True when a cast from `from` to `to` keeps the order and each value.
///
/// The steps that keep each value:
/// - the same type;
/// - a signed integer to a wider signed integer;
/// - an unsigned integer to a wider unsigned integer or to a wider signed integer;
/// - Int8, Int16, Int32, UInt8, UInt16 and UInt32 to Float64;
/// - Int8, Int16, UInt8 and UInt16 to Float32;
/// - Float32 to Float64;
/// - a timestamp to a finer unit with the same time zone.
///
/// Each other step can change which rows meet a compare, so it gives `false`.
pub(crate) fn keeps_values(from: &DataType, to: &DataType) -> bool {
    if from == to {
        return true;
    }
    match (integer_bits(from), integer_bits(to)) {
        // A signed integer to an unsigned one loses the negative values.
        (Some((from_signed, from_bits)), Some((to_signed, to_bits))) => {
            (to_signed || !from_signed) && to_bits > from_bits
        }
        (Some((_, bits)), None) => match to {
            DataType::Float64 => bits <= 32,
            DataType::Float32 => bits <= 16,
            _ => false,
        },
        (None, Some(_)) => false,
        (None, None) => match (from, to) {
            (DataType::Float32, DataType::Float64) => true,
            (DataType::Timestamp(from_unit, from_zone), DataType::Timestamp(to_unit, to_zone)) => {
                from_zone == to_zone && unit_nanos(*to_unit) < unit_nanos(*from_unit)
            }
            _ => false,
        },
    }
}

/// Whether an integer type is signed, and its width in bits. `None` for another type.
fn integer_bits(data_type: &DataType) -> Option<(bool, u32)> {
    Some(match data_type {
        DataType::Int8 => (true, 8),
        DataType::Int16 => (true, 16),
        DataType::Int32 => (true, 32),
        DataType::Int64 => (true, 64),
        DataType::UInt8 => (false, 8),
        DataType::UInt16 => (false, 16),
        DataType::UInt32 => (false, 32),
        DataType::UInt64 => (false, 64),
        _ => return None,
    })
}

/// The length of one step of `unit`, in nanoseconds.
pub(crate) fn unit_nanos(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Nanosecond => 1,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Second => 1_000_000_000,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::keeps_values;

    #[test]
    fn the_value_keeping_steps_are_the_listed_ones() {
        use DataType::*;
        let seconds = Timestamp(TimeUnit::Second, None);
        let nanos = Timestamp(TimeUnit::Nanosecond, None);
        let utc_millis = Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let utc_micros = Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let utc_seconds = Timestamp(TimeUnit::Second, Some("UTC".into()));
        let kept = [
            (Int32, Int32),
            (Float64, Float64),
            (Int8, Int16),
            (Int16, Int64),
            (Int32, Int64),
            (UInt8, UInt16),
            (UInt32, UInt64),
            (UInt8, Int16),
            (UInt32, Int64),
            (Int8, Float64),
            (Int32, Float64),
            (UInt32, Float64),
            (Int8, Float32),
            (Int16, Float32),
            (UInt8, Float32),
            (UInt16, Float32),
            (Float32, Float64),
            (seconds.clone(), nanos.clone()),
            (utc_millis.clone(), utc_micros),
        ];
        for (from, to) in kept {
            assert!(keeps_values(&from, &to), "{from} to {to} keeps values");
        }
        let changed = [
            (Float64, Float32),
            (Int64, Float64),
            (UInt64, Float64),
            (Int32, Float32),
            (UInt32, Float32),
            (Int64, Int32),
            (Int8, UInt16),
            (UInt16, Int16),
            (Float32, Int64),
            (Int64, nanos.clone()),
            (nanos.clone(), seconds.clone()),
            (utc_millis, utc_seconds.clone()),
            (seconds.clone(), utc_seconds.clone()),
            (utc_seconds, seconds),
        ];
        for (from, to) in changed {
            assert!(
                !keeps_values(&from, &to),
                "{from} to {to} can change values"
            );
        }
    }
}
