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

pub async fn generate_zarr_statistics_from_zarr_group(
    schema: &SchemaRef,
    pushdown: &ZarrPushDownStatistics,
    zarr_group: Arc<Group<dyn AsyncReadableListableStorageTraits>>,
) -> ZarrStatistics {
    todo!()
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

#[derive(Debug, Clone)]
pub struct ZarrStatistics {
    table_schema: SchemaRef,
    arrays: HashMap<String, ZarrArrayStatistics>,
}

impl ZarrStatistics {
    pub fn as_extensions(&self) -> Arc<dyn std::any::Any + Send + Sync> {
        Arc::new(self.clone())
    }

    pub fn statistics(&self) -> Statistics {
        let mut stats = Statistics::default();

        for field in self.table_schema.fields() {
            if let Some(array_stats) = self.arrays.get(field.name()) {
                stats = stats.add_column_statistics(array_stats.column_statistics().clone());
            } else {
                stats = stats.add_column_statistics(ColumnStatistics::new_unknown());
            }
        }

        stats
    }
}

#[derive(Debug, Clone)]
pub struct ZarrArrayStatistics {
    column_statistics: ColumnStatistics,
    array_ref: NdArrowArray,
}

impl ZarrArrayStatistics {
    pub fn column_statistics(&self) -> &ColumnStatistics {
        &self.column_statistics
    }

    pub fn get_slice_pushdown(&self, value_range: ZarrFilterRange) -> Option<ArraySlicePushDown> {
        if self.array_ref.dimensions().num_dims() != 1 {
            return None;
        }

        let cast_options = CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        };

        let target_data_type = self.array_ref.data_type();

        let min_value = value_range
            .min_value()
            .and_then(|s| s.cast_to_with_options(target_data_type, &cast_options).ok())
            .and_then(|s| s.to_scalar().ok());

        let min_inclusive = value_range.is_min_inclusive().unwrap_or_default();
        let max_inclusive = value_range.is_max_inclusive().unwrap_or_default();

        let max_value = value_range
            .max_value()
            .and_then(|s| s.cast_to_with_options(target_data_type, &cast_options).ok())
            .and_then(|s| s.to_scalar().ok());

        let maybe_range = find_range(
            self.array_ref.as_arrow_array(),
            min_value,
            max_value,
            min_inclusive,
            max_inclusive,
        );

        let dimension = &self.array_ref.dimensions().as_multi_dimensional().unwrap()[0];

        maybe_range.map(|range| {
            ArraySlicePushDown::new(dimension.name.clone(), Some(range.start), Some(range.end))
        })
    }
}

fn find_range(
    values: &ArrayRef,
    min: Option<Scalar<ArrayRef>>,
    max: Option<Scalar<ArrayRef>>,
    min_inclusive: bool,
    max_inclusive: bool,
) -> Option<Range<usize>> {
    None
}

/// Generic typed implementation for any PrimitiveArray<T>
fn typed_range<T>(
    values: &ArrayRef,
    min: Scalar<ArrayRef>,
    max: Scalar<ArrayRef>,
    min_inclusive: bool,
    max_inclusive: bool,
) -> Option<Range<usize>>
where
    T: arrow::datatypes::ArrowNumericType,
    T::Native: PartialOrd + Copy,
{
    let arr = values.as_any().downcast_ref::<PrimitiveArray<T>>()?;
    let min_arr = min.into_inner();
    let max_arr = max.into_inner();
    let min = min_arr.as_any().downcast_ref::<PrimitiveArray<T>>()?;
    let max = max_arr.as_any().downcast_ref::<PrimitiveArray<T>>()?;

    let min_opt = if !min.is_empty() && !min.is_null(0) {
        Some(min.value(0))
    } else {
        None
    };

    let max_opt = if !max.is_empty() && !max.is_null(0) {
        Some(max.value(0))
    } else {
        None
    };

    value_range_arrow::<T>(arr, min_opt, max_opt, min_inclusive, max_inclusive)
}

fn value_range_arrow<T>(
    arr: &PrimitiveArray<T>,
    min: Option<T::Native>,
    max: Option<T::Native>,
    min_inclusive: bool,
    max_inclusive: bool,
) -> Option<Range<usize>>
where
    T: arrow::datatypes::ArrowNumericType,
    T::Native: PartialOrd + Copy,
{
    // Normalize bounds so that (low, high) always in correct order
    let (low, high, low_incl, high_incl) = match (min, max) {
        (Some(a), Some(b)) if a > b => (Some(b), Some(a), max_inclusive, min_inclusive),
        _ => (min, max, min_inclusive, max_inclusive),
    };

    let mut first: Option<usize> = None;
    let mut last: Option<usize> = None;

    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }

        let v = arr.value(i);

        let lower_ok = match (low, low_incl) {
            (Some(m), true) => v >= m,
            (Some(m), false) => v > m,
            (None, _) => true,
        };

        let upper_ok = match (high, high_incl) {
            (Some(m), true) => v <= m,
            (Some(m), false) => v < m,
            (None, _) => true,
        };

        if lower_ok && upper_ok {
            if first.is_none() {
                first = Some(i);
            }
            last = Some(i);
        }
    }

    match (first, last) {
        (Some(s), Some(e)) => Some(s..e + 1),
        _ => None,
    }
}

/// String array logic (Utf8 and LargeUtf8).
fn string_range(
    values: &ArrayRef,
    min: Option<Scalar<ArrayRef>>,
    max: Option<Scalar<ArrayRef>>,
    min_inclusive: bool,
    max_inclusive: bool,
) -> Option<Range<usize>> {
    let arr = values.as_string::<i32>();

    let min_val = min.and_then(|s| Some(s.into_inner().as_string::<i32>().value(0).to_string()));
    let max_val = max.and_then(|s| Some(s.into_inner().as_string::<i32>().value(0).to_string()));

    // Normalize bounds lexicographically if both are present
    let (low, high, low_incl, high_incl) = match (min_val.clone(), max_val.clone()) {
        (Some(a), Some(b)) if a > b => (Some(b), Some(a), max_inclusive, min_inclusive),
        _ => (min_val, max_val, min_inclusive, max_inclusive),
    };

    let mut first = None;
    let mut last = None;

    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        let v = arr.value(i);

        let lower_ok = match (low.as_ref(), low_incl) {
            (Some(m), true) => v >= m.as_str(),
            (Some(m), false) => v > m.as_str(),
            (None, _) => true,
        };

        let upper_ok = match (high.as_ref(), high_incl) {
            (Some(m), true) => v <= m.as_str(),
            (Some(m), false) => v < m.as_str(),
            (None, _) => true,
        };

        if lower_ok && upper_ok {
            if first.is_none() {
                first = Some(i);
            }
            last = Some(i);
        }
    }

    match (first, last) {
        (Some(s), Some(e)) => Some(s..e + 1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Float64Array, TimestampMillisecondArray},
        datatypes::Float64Type,
    };

    fn make_float_array(values: &[f64]) -> PrimitiveArray<Float64Type> {
        Float64Array::from(values.to_vec())
    }

    #[test]
    fn test_basic_range() {
        let arr = make_float_array(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let range = value_range_arrow(&arr, Some(2.0), Some(4.0), true, true);
        assert_eq!(range, Some(1..4)); // covers 2.0, 3.0, 4.0
    }

    #[test]
    fn test_exclusive_bounds() {
        let arr = make_float_array(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let range = value_range_arrow(&arr, Some(2.0), Some(4.0), false, false);
        assert_eq!(range, Some(2..3)); // only 3.0 fits (2< v <4)
    }

    #[test]
    fn test_open_upper_bound() {
        let arr = make_float_array(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let range = value_range_arrow(&arr, Some(3.0), None, true, true);
        assert_eq!(range, Some(2..5)); // 3,4,5
    }

    #[test]
    fn test_open_lower_bound() {
        let arr = make_float_array(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let range = value_range_arrow(&arr, None, Some(3.0), true, true);
        assert_eq!(range, Some(0..3)); // 1,2,3
    }

    #[test]
    fn test_no_matches() {
        let arr = make_float_array(&[1.0, 2.0, 3.0]);
        let range = value_range_arrow(&arr, Some(10.0), Some(20.0), true, true);
        assert_eq!(range, None);
    }

    #[test]
    fn test_reversed_bounds() {
        let arr = make_float_array(&[30.0, 25.0, 20.0, 15.0, 10.0, 5.0, 0.0]);
        let range = value_range_arrow(&arr, Some(10.0), Some(20.0), true, true);
        assert_eq!(range, Some(2..5)); // 20,15,10
    }

    #[test]
    fn test_reversed_input_bounds() {
        let arr = make_float_array(&[30.0, 25.0, 20.0, 15.0, 10.0, 5.0, 0.0]);
        let range = value_range_arrow(&arr, Some(20.0), Some(10.0), true, true);
        assert_eq!(range, Some(2..5)); // reversed min/max still valid
    }

    #[test]
    fn test_random_scattered_values() {
        let arr = make_float_array(&[5.0, 40.0, 3.0, 22.0, 50.0, 12.0, 45.0, 7.0, 25.0, 2.0]);
        let range = value_range_arrow(&arr, Some(10.0), Some(30.0), true, true);
        assert_eq!(range, Some(3..9)); // indices 3,5,8 → covers all
    }

    #[test]
    fn test_descending_with_swapped_bounds() {
        let arr = make_float_array(&[30.0, 25.0, 20.0, 15.0, 10.0, 5.0, 0.0, -5.0]);
        let range = value_range_arrow(&arr, Some(10.0), Some(-10.0), true, true);
        assert_eq!(range, Some(4..8)); // descending but still matches
    }

    #[test]
    fn test_timestamp_array() {
        use arrow::array::TimestampMillisecondArray;
        use chrono::{TimeZone, Utc};

        let times: Vec<i64> = vec![
            Utc.ymd(2024, 1, 1).and_hms(0, 0, 0).timestamp_millis(),
            Utc.ymd(2024, 1, 2).and_hms(0, 0, 0).timestamp_millis(),
            Utc.ymd(2024, 1, 3).and_hms(0, 0, 0).timestamp_millis(),
            Utc.ymd(2024, 1, 4).and_hms(0, 0, 0).timestamp_millis(),
        ];
        let arr = TimestampMillisecondArray::from(times);
        let start = Utc.ymd(2024, 1, 2).and_hms(0, 0, 0).timestamp_millis();
        let end = Utc.ymd(2024, 1, 3).and_hms(0, 0, 0).timestamp_millis();

        let range = value_range_arrow(&arr, Some(start), Some(end), true, true);
        assert_eq!(range, Some(1..3));
    }

    #[test]
    fn test_nulls_ignored() {
        let arr = Float64Array::from(vec![Some(1.0), None, Some(3.0), Some(4.0), None]);
        let range = value_range_arrow(&arr, Some(2.0), Some(4.0), true, true);
        assert_eq!(range, Some(2..3)); // nulls ignored
    }

    fn make_scalar(value: &str) -> Scalar<ArrayRef> {
        Scalar::new(Arc::new(StringArray::from(vec![value])))
    }

    fn make_scalar_opt(opt: Option<&str>) -> Option<Scalar<ArrayRef>> {
        opt.map(|v| Scalar::new(Arc::new(StringArray::from(vec![v])) as ArrayRef))
    }

    fn make_array(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    #[test]
    fn test_string_basic_range() {
        let arr = make_array(&["apple", "banana", "cherry", "date", "elderberry"]);

        let min = make_scalar("banana");
        let max = make_scalar("date");

        let range = string_range(&arr, Some(min), Some(max), true, true);
        assert_eq!(range, Some(1..4)); // banana, cherry, date
    }

    #[test]
    fn test_string_exclusive_bounds() {
        let arr = make_array(&["apple", "banana", "cherry", "date", "elderberry"]);

        let min = make_scalar("banana");
        let max = make_scalar("date");

        let range = string_range(&arr, Some(min), Some(max), false, false);
        assert_eq!(range, Some(2..3)); // only "cherry"
    }

    #[test]
    fn test_string_open_lower_bound() {
        let arr = make_array(&["apple", "banana", "cherry", "date", "elderberry"]);

        let max = make_scalar("cherry");
        let range = string_range(&arr, None, Some(max), true, true);
        assert_eq!(range, Some(0..3)); // apple, banana, cherry
    }

    #[test]
    fn test_string_open_upper_bound() {
        let arr = make_array(&["apple", "banana", "cherry", "date", "elderberry"]);

        let min = make_scalar("cherry");
        let range = string_range(&arr, Some(min), None, true, true);
        assert_eq!(range, Some(2..5)); // cherry, date, elderberry
    }

    #[test]
    fn test_string_reversed_bounds() {
        let arr = make_array(&["apple", "banana", "cherry", "date", "elderberry"]);

        let min = make_scalar("elderberry");
        let max = make_scalar("banana");

        // min > max lexicographically → still returns correct range
        let range = string_range(&arr, Some(min), Some(max), true, true);
        assert_eq!(range, Some(1..5)); // banana..elderberry
    }

    #[test]
    fn test_string_no_matches() {
        let arr = make_array(&["apple", "banana", "cherry"]);

        let min = make_scalar("xenon");
        let max = make_scalar("zebra");

        let range = string_range(&arr, Some(min), Some(max), true, true);
        assert_eq!(range, None);
    }

    #[test]
    fn test_string_with_nulls() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
            Some("cherry"),
            None,
        ]));

        let min = make_scalar("banana");
        let max = make_scalar("cherry");

        let range = string_range(&arr, Some(min), Some(max), true, true);
        assert_eq!(range, Some(2..4)); // skips nulls
    }

    #[test]
    fn test_string_random_scattered_values() {
        let arr = make_array(&["kiwi", "apple", "pear", "banana", "mango", "fig"]);

        let min = make_scalar("banana");
        let max = make_scalar("pear");

        let range = string_range(&arr, Some(min), Some(max), true, true);
        // matches scattered → overall enclosing range
        assert_eq!(range, Some(0..5)); // apple..pear → full span
    }

    #[test]
    fn test_string_min_only() {
        let arr = make_array(&["ant", "bee", "cat", "dog"]);
        let min = make_scalar("cat");

        let range = string_range(&arr, Some(min), None, true, true);
        assert_eq!(range, Some(2..3)); // cat, dog
    }

    #[test]
    fn test_string_max_only() {
        let arr = make_array(&["ant", "bee", "cat", "dog"]);
        let max = make_scalar("cat");

        let range = string_range(&arr, None, Some(max), true, true);
        assert_eq!(range, Some(0..2)); // ant, bee, cat
    }
}
