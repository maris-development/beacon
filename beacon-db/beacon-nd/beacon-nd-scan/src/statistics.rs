//! The DataFusion statistics of one file.

use arrow::datatypes::{DataType, Schema, TimeUnit};
use beacon_nd_dataset::{NdDataset, NdVariable, value_range};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

use crate::error::to_datafusion;

/// The statistics of one file for the columns of `schema`.
///
/// A variable of rank 0 or 1 gets its exact minimum and maximum in the type of its field.
/// Each other column is unknown, also a column that the file does not have.
/// A column is also unknown when the cast to the field type can change a value,
/// or when its minimum or maximum does not fit in the field type.
/// The row count is not set. A larger variable is not read, because a full read costs too much.
pub async fn file_statistics(dataset: &dyn NdDataset, schema: &Schema) -> Result<Statistics> {
    let mut statistics = Statistics::default();
    for field in schema.fields() {
        let column = match dataset.variable(field.name()) {
            Some(variable)
                if variable.dimensions().rank() <= 1
                    && keeps_values(variable.data_type(), field.data_type()) =>
            {
                column_statistics(variable.as_ref(), field.data_type()).await?
            }
            _ => ColumnStatistics::new_unknown(),
        };
        statistics = statistics.add_column_statistics(column);
    }
    Ok(statistics)
}

/// The exact minimum and maximum of `variable` in `field_type`, or unknown when it has no range.
async fn column_statistics(
    variable: &dyn NdVariable,
    field_type: &DataType,
) -> Result<ColumnStatistics> {
    let Some((min, max)) = value_range(variable).await.map_err(to_datafusion)? else {
        return Ok(ColumnStatistics::new_unknown());
    };
    // DataFusion merges the statistics of the files in the type of the table.
    let min = ScalarValue::try_from_array(&min, 0)?.cast_to(field_type);
    let max = ScalarValue::try_from_array(&max, 0)?.cast_to(field_type);
    // A value that does not fit in the field type gives no statistics.
    let (Ok(min), Ok(max)) = (min, max) else {
        return Ok(ColumnStatistics::new_unknown());
    };
    if min.is_null() || max.is_null() {
        return Ok(ColumnStatistics::new_unknown());
    }
    Ok(ColumnStatistics::new_unknown()
        .with_min_value(Precision::Exact(min))
        .with_max_value(Precision::Exact(max)))
}

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
/// Each other step can change the minimum or the maximum, so it gives `false`.
fn keeps_values(from: &DataType, to: &DataType) -> bool {
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
fn unit_nanos(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Nanosecond => 1,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Second => 1_000_000_000,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use beacon_nd_dataset::{Dimension, Dimensions, MemoryDataset, MemoryVariable, NdArrowArray};
    use datafusion::common::ColumnStatistics;
    use datafusion::common::stats::Precision;
    use datafusion::scalar::ScalarValue;

    use super::{file_statistics, keeps_values};
    use crate::testing::grid_dataset;

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

    #[tokio::test]
    async fn statistics_cover_the_rank_0_and_rank_1_variables() {
        let dataset = grid_dataset("a.nd");
        let schema = Schema::new(
            ["lat", "temp", "salt"]
                .map(|name| Field::new(name, DataType::Float64, true))
                .to_vec(),
        );

        let statistics = file_statistics(&dataset, &schema).await.unwrap();

        assert_eq!(statistics.num_rows, Precision::Absent);
        assert_eq!(statistics.column_statistics.len(), 3);
        let lat = &statistics.column_statistics[0];
        assert_eq!(
            lat.min_value,
            Precision::Exact(ScalarValue::Float64(Some(0.0)))
        );
        assert_eq!(
            lat.max_value,
            Precision::Exact(ScalarValue::Float64(Some(7.0)))
        );
        assert_eq!(
            statistics.column_statistics[1],
            ColumnStatistics::new_unknown()
        );
        assert_eq!(
            statistics.column_statistics[2],
            ColumnStatistics::new_unknown()
        );
        assert_eq!(dataset.reads("temp"), 0);
    }

    /// A dataset with the 1-D variable `n` of `values`, and a table schema with `n` in `field_type`.
    fn one_variable(values: ArrayRef, field_type: DataType) -> (MemoryDataset, Schema) {
        let x = Dimensions::try_new([Dimension::new("x", values.len())]).unwrap();
        let n = MemoryVariable::new("n", NdArrowArray::try_new(values, x).unwrap());
        let dataset = MemoryDataset::try_new("a.nd", vec![n]).unwrap();
        (
            dataset,
            Schema::new(vec![Field::new("n", field_type, true)]),
        )
    }

    #[tokio::test]
    async fn an_int32_variable_under_a_float64_field_gives_float64_statistics() {
        let values = Arc::new(Int32Array::from(vec![1, 5, 3]));
        let (dataset, schema) = one_variable(values, DataType::Float64);

        let statistics = file_statistics(&dataset, &schema).await.unwrap();

        let n = &statistics.column_statistics[0];
        assert_eq!(
            n.min_value,
            Precision::Exact(ScalarValue::Float64(Some(1.0)))
        );
        assert_eq!(
            n.max_value,
            Precision::Exact(ScalarValue::Float64(Some(5.0)))
        );
    }

    #[tokio::test]
    async fn a_timestamp_that_overflows_the_field_unit_has_no_statistics() {
        // The year 9999 in seconds does not fit in nanoseconds.
        let values = Arc::new(TimestampSecondArray::from(vec![0, 253_402_300_799]));
        let field_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let (dataset, schema) = one_variable(values, field_type);

        let statistics = file_statistics(&dataset, &schema).await.unwrap();

        assert_eq!(
            statistics.column_statistics[0],
            ColumnStatistics::new_unknown()
        );
    }

    #[tokio::test]
    async fn a_timestamp_that_fits_the_field_unit_has_statistics() {
        let values = Arc::new(TimestampSecondArray::from(vec![1, 5, 3]));
        let field_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let (dataset, schema) = one_variable(values, field_type);

        let statistics = file_statistics(&dataset, &schema).await.unwrap();

        let n = &statistics.column_statistics[0];
        assert_eq!(
            n.min_value,
            Precision::Exact(ScalarValue::TimestampNanosecond(Some(1_000_000_000), None))
        );
        assert_eq!(
            n.max_value,
            Precision::Exact(ScalarValue::TimestampNanosecond(Some(5_000_000_000), None))
        );
    }

    #[tokio::test]
    async fn a_float64_variable_under_a_float32_field_has_no_statistics() {
        let values = Arc::new(Float64Array::from(vec![1.0, 5.0, 3.0]));
        let (dataset, schema) = one_variable(values, DataType::Float32);

        let statistics = file_statistics(&dataset, &schema).await.unwrap();

        assert_eq!(
            statistics.column_statistics[0],
            ColumnStatistics::new_unknown()
        );
    }
}
