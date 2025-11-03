use std::{collections::HashMap, ops::Range, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, PrimitiveArray, Scalar, StringArray},
    compute::CastOptions,
    datatypes::{Int8Type, SchemaRef},
    util::display::FormatOptions,
};
use beacon_arrow_zarr::{
    array_slice_pushdown::ArraySlicePushDown, reader::AsyncArrowZarrGroupReader,
};
use datafusion::{
    common::{ColumnStatistics, Statistics, stats::Precision},
    scalar::ScalarValue,
};
use nd_arrow_array::NdArrowArray;
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::expr_util::ZarrFilterRange;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ZarrPushDownStatistics {
    pub arrays: Vec<String>,
}

impl ZarrPushDownStatistics {
    pub fn has_array(&self, array_name: &str) -> bool {
        self.arrays.contains(&array_name.to_string())
    }
}

pub async fn generate_statistics_from_zarr_group(
    schema: &SchemaRef,
    pushdown: &ZarrPushDownStatistics,
    zarr_group: Arc<Group<dyn AsyncReadableListableStorageTraits>>,
) -> Option<Statistics> {
    let zarr_reader = AsyncArrowZarrGroupReader::new(zarr_group.clone())
        .await
        .ok()?;
    let mut statistics = Statistics::default();
    for field in schema.fields() {
        if pushdown.has_array(field.name()) {
            // Read the full array
            if let Ok(Some(array)) = zarr_reader.read_array_full(field.name()).await {
                // Compute statistics for the array
                if let Some(array_stats) = compute_statistics_for_array(&array) {
                    statistics = statistics.add_column_statistics(array_stats.clone());
                } else {
                    statistics = statistics.add_column_statistics(ColumnStatistics::new_unknown());
                }
            } else {
                statistics = statistics.add_column_statistics(ColumnStatistics::new_unknown());
            }
        } else {
            statistics = statistics.add_column_statistics(ColumnStatistics::new_unknown());
        }
    }

    Some(statistics)
}

fn compute_statistics_for_array(array: &arrow::array::ArrayRef) -> Option<ColumnStatistics> {
    let data_type = array.data_type().clone();
    let null_count = array.null_count();
    let mut statistics = ColumnStatistics {
        null_count: Precision::Exact(null_count),
        distinct_count: Precision::Absent,
        max_value: Precision::Absent,
        min_value: Precision::Absent,
        sum_value: Precision::Absent,
    };

    match data_type {
        arrow::datatypes::DataType::Boolean => {
            let boolean_array = array.as_boolean();
            arrow::compute::min_boolean(boolean_array).map(|min| {
                statistics.min_value = Precision::Exact(ScalarValue::Boolean(Some(min)))
            });
            arrow::compute::max_boolean(boolean_array).map(|max| {
                statistics.max_value = Precision::Exact(ScalarValue::Boolean(Some(max)))
            });
        }
        arrow::datatypes::DataType::Int8 => {
            let int_array = array.as_primitive::<Int8Type>();
            arrow::compute::min(int_array)
                .map(|min| statistics.min_value = Precision::Exact(ScalarValue::Int8(Some(min))));
            arrow::compute::max(int_array)
                .map(|max| statistics.max_value = Precision::Exact(ScalarValue::Int8(Some(max))));
        }
        arrow::datatypes::DataType::Int16 => {
            let int_array = array.as_primitive::<arrow::datatypes::Int16Type>();
            arrow::compute::min(int_array)
                .map(|min| statistics.min_value = Precision::Exact(ScalarValue::Int16(Some(min))));
            arrow::compute::max(int_array)
                .map(|max| statistics.max_value = Precision::Exact(ScalarValue::Int16(Some(max))));
        }
        arrow::datatypes::DataType::Int32 => {
            let int_array = array.as_primitive::<arrow::datatypes::Int32Type>();
            arrow::compute::min(int_array)
                .map(|min| statistics.min_value = Precision::Exact(ScalarValue::Int32(Some(min))));
            arrow::compute::max(int_array)
                .map(|max| statistics.max_value = Precision::Exact(ScalarValue::Int32(Some(max))));
        }
        arrow::datatypes::DataType::Int64 => {
            let int_array = array.as_primitive::<arrow::datatypes::Int64Type>();
            arrow::compute::min(int_array)
                .map(|min| statistics.min_value = Precision::Exact(ScalarValue::Int64(Some(min))));
            arrow::compute::max(int_array)
                .map(|max| statistics.max_value = Precision::Exact(ScalarValue::Int64(Some(max))));
        }
        arrow::datatypes::DataType::UInt8 => {
            let int_array = array.as_primitive::<arrow::datatypes::UInt8Type>();
            arrow::compute::min(int_array)
                .map(|min| statistics.min_value = Precision::Exact(ScalarValue::UInt8(Some(min))));
            arrow::compute::max(int_array)
                .map(|max| statistics.max_value = Precision::Exact(ScalarValue::UInt8(Some(max))));
        }
        arrow::datatypes::DataType::UInt16 => {
            let int_array = array.as_primitive::<arrow::datatypes::UInt16Type>();
            arrow::compute::min(int_array)
                .map(|min| statistics.min_value = Precision::Exact(ScalarValue::UInt16(Some(min))));
            arrow::compute::max(int_array)
                .map(|max| statistics.max_value = Precision::Exact(ScalarValue::UInt16(Some(max))));
        }
        arrow::datatypes::DataType::UInt32 => {
            let int_array = array.as_primitive::<arrow::datatypes::UInt32Type>();
            arrow::compute::min(int_array)
                .map(|min| statistics.min_value = Precision::Exact(ScalarValue::UInt32(Some(min))));
            arrow::compute::max(int_array)
                .map(|max| statistics.max_value = Precision::Exact(ScalarValue::UInt32(Some(max))));
        }
        arrow::datatypes::DataType::UInt64 => {
            let int_array = array.as_primitive::<arrow::datatypes::UInt64Type>();
            arrow::compute::min(int_array)
                .map(|min| statistics.min_value = Precision::Exact(ScalarValue::UInt64(Some(min))));
            arrow::compute::max(int_array)
                .map(|max| statistics.max_value = Precision::Exact(ScalarValue::UInt64(Some(max))));
        }
        arrow::datatypes::DataType::Float32 => {
            let float_array = array.as_primitive::<arrow::datatypes::Float32Type>();
            arrow::compute::min(float_array).map(|min| {
                statistics.min_value = Precision::Exact(ScalarValue::Float32(Some(min)))
            });
            arrow::compute::max(float_array).map(|max| {
                statistics.max_value = Precision::Exact(ScalarValue::Float32(Some(max)))
            });
        }
        arrow::datatypes::DataType::Float64 => {
            let float_array = array.as_primitive::<arrow::datatypes::Float64Type>();
            arrow::compute::min(float_array).map(|min| {
                statistics.min_value = Precision::Exact(ScalarValue::Float64(Some(min)))
            });
            arrow::compute::max(float_array).map(|max| {
                statistics.max_value = Precision::Exact(ScalarValue::Float64(Some(max)))
            });
        }
        arrow::datatypes::DataType::Timestamp(time_unit, _) => match time_unit {
            arrow::datatypes::TimeUnit::Second => {
                let ts_array = array.as_primitive::<arrow::datatypes::TimestampSecondType>();
                arrow::compute::min(ts_array).map(|min| {
                    statistics.min_value =
                        Precision::Exact(ScalarValue::TimestampSecond(Some(min), None))
                });
                arrow::compute::max(ts_array).map(|max| {
                    statistics.max_value =
                        Precision::Exact(ScalarValue::TimestampSecond(Some(max), None))
                });
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                let ts_array = array.as_primitive::<arrow::datatypes::TimestampMillisecondType>();
                arrow::compute::min(ts_array).map(|min| {
                    statistics.min_value =
                        Precision::Exact(ScalarValue::TimestampMillisecond(Some(min), None))
                });
                arrow::compute::max(ts_array).map(|max| {
                    statistics.max_value =
                        Precision::Exact(ScalarValue::TimestampMillisecond(Some(max), None))
                });
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                let ts_array = array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
                arrow::compute::min(ts_array).map(|min| {
                    statistics.min_value =
                        Precision::Exact(ScalarValue::TimestampMicrosecond(Some(min), None))
                });
                arrow::compute::max(ts_array).map(|max| {
                    statistics.max_value =
                        Precision::Exact(ScalarValue::TimestampMicrosecond(Some(max), None))
                });
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                let ts_array = array.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
                arrow::compute::min(ts_array).map(|min| {
                    statistics.min_value =
                        Precision::Exact(ScalarValue::TimestampNanosecond(Some(min), None))
                });
                arrow::compute::max(ts_array).map(|max| {
                    statistics.max_value =
                        Precision::Exact(ScalarValue::TimestampNanosecond(Some(max), None))
                });
            }
        },
        arrow::datatypes::DataType::Binary => {
            let binary_array = array.as_binary::<i32>();
            arrow::compute::min_binary(binary_array).map(|min| {
                statistics.min_value = Precision::Exact(ScalarValue::Binary(Some(min.to_vec())))
            });
            arrow::compute::max_binary(binary_array).map(|max| {
                statistics.max_value = Precision::Exact(ScalarValue::Binary(Some(max.to_vec())))
            });
        }
        arrow::datatypes::DataType::Utf8 => {
            let string_array = array.as_string::<i32>();
            arrow::compute::min_string(string_array).map(|min| {
                statistics.min_value = Precision::Exact(ScalarValue::Utf8(Some(min.to_string())))
            });
            arrow::compute::max_string(string_array).map(|max| {
                statistics.max_value = Precision::Exact(ScalarValue::Utf8(Some(max.to_string())))
            });
        }
        _ => {} // Unsupported data type for statistics
    }

    Some(statistics)
}

pub struct ZarrStatistics {
    arrays: HashMap<String, ZarrArrayStatistics>,
}

pub struct ZarrArrayStatistics {
    column_statistics: ColumnStatistics,
    array_ref: NdArrowArray,
}

impl ZarrArrayStatistics {
    pub fn column_statistics(&self) -> &ColumnStatistics {
        &self.column_statistics
    }

    pub fn get_slice_pushdown(&self, value_range: ZarrFilterRange) -> Option<ArraySlicePushDown> {
        if self.array_ref.dimensions().num_dims() > 1 {
            return None;
        }

        let cast_options = CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        };

        let target_data_type = self.array_ref.data_type();

        let min_value = value_range
            .min_value()
            .map(|s| s.cast_to_with_options(target_data_type, &cast_options).ok())
            .flatten();

        let max_value = value_range
            .max_value()
            .map(|s| s.cast_to_with_options(target_data_type, &cast_options).ok())
            .flatten();

        todo!()
    }

    fn find_flattened_min_index(array: ArrayRef, min_value: &ScalarValue) -> Option<usize> {
        assert_eq!(array.data_type(), &min_value.data_type());
        todo!()
    }
}

/// Find a single range [start..end) covering all indices that are in [min, max].
pub fn arrow_value_range(
    values: &ArrayRef,
    min: Option<Scalar<ArrayRef>>,
    max: Option<Scalar<ArrayRef>>,
    min_inclusive: bool,
    max_inclusive: bool,
) -> Option<Range<usize>> {
    match values.data_type() {
        // ---- Numeric types ----
        DataType::Int8 => typed_range::<Int8Type>(values, min, max, min_inclusive, max_inclusive),
        DataType::Int16 => typed_range::<Int16Type>(values, min, max, min_inclusive, max_inclusive),
        DataType::Int32 => typed_range::<Int32Type>(values, min, max, min_inclusive, max_inclusive),
        DataType::Int64 => typed_range::<Int64Type>(values, min, max, min_inclusive, max_inclusive),
        DataType::UInt8 => typed_range::<UInt8Type>(values, min, max, min_inclusive, max_inclusive),
        DataType::UInt16 => {
            typed_range::<UInt16Type>(values, min, max, min_inclusive, max_inclusive)
        }
        DataType::UInt32 => {
            typed_range::<UInt32Type>(values, min, max, min_inclusive, max_inclusive)
        }
        DataType::UInt64 => {
            typed_range::<UInt64Type>(values, min, max, min_inclusive, max_inclusive)
        }
        DataType::Float32 => {
            typed_range::<Float32Type>(values, min, max, min_inclusive, max_inclusive)
        }
        DataType::Float64 => {
            typed_range::<Float64Type>(values, min, max, min_inclusive, max_inclusive)
        }

        // ---- Timestamp types ----
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                typed_range::<TimestampSecondType>(values, min, max, min_inclusive, max_inclusive)
            }
            TimeUnit::Millisecond => typed_range::<TimestampMillisecondType>(
                values,
                min,
                max,
                min_inclusive,
                max_inclusive,
            ),
            TimeUnit::Microsecond => typed_range::<TimestampMicrosecondType>(
                values,
                min,
                max,
                min_inclusive,
                max_inclusive,
            ),
            TimeUnit::Nanosecond => typed_range::<TimestampNanosecondType>(
                values,
                min,
                max,
                min_inclusive,
                max_inclusive,
            ),
        },

        _ => None,
    }
}

/// Generic typed implementation for any PrimitiveArray<T>
fn typed_range<T>(
    values: &ArrayRef,
    min: Scalar<ArrayRef>,
    max: Scalar<ArrayRef>,
) -> Option<Vec<Range<usize>>>
where
    T: arrow::datatypes::ArrowNumericType,
    T::Native: PartialOrd + Copy,
{
    let arr = values.as_any().downcast_ref::<PrimitiveArray<T>>()?;
    let min = min
        .into_inner()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()?
        .value(0);
    let max = max
        .into_inner()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()?
        .value(0);

    Some(value_ranges_arrow(arr, min, max))
}

/// Helper for numeric/temporal arrays
fn value_ranges_arrow<T>(
    arr: &arrow::array::PrimitiveArray<T>,
    min: T::Native,
    max: T::Native,
) -> Vec<Range<usize>>
where
    T: arrow::datatypes::ArrowNumericType,
    T::Native: PartialOrd + Copy,
{
    let mut ranges = Vec::new();
    let mut start: Option<usize> = None;

    for i in 0..arr.len() {
        if arr.is_null(i) {
            if let Some(s) = start.take() {
                ranges.push(s..i);
            }
            continue;
        }

        let v = arr.value(i);
        let in_range = v >= min && v <= max;

        match (in_range, start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                ranges.push(s..i);
                start = None;
            }
            _ => {}
        }
    }

    if let Some(s) = start {
        ranges.push(s..arr.len());
    }

    ranges
}

/// Helper for string arrays
fn value_ranges_arrow_str(arr: &StringArray, min: &str, max: &str) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut start: Option<usize> = None;

    for i in 0..arr.len() {
        if arr.is_null(i) {
            if let Some(s) = start.take() {
                ranges.push(s..i);
            }
            continue;
        }

        let v = arr.value(i);
        let in_range = v >= min && v <= max;

        match (in_range, start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                ranges.push(s..i);
                start = None;
            }
            _ => {}
        }
    }

    if let Some(s) = start {
        ranges.push(s..arr.len());
    }

    ranges
}
