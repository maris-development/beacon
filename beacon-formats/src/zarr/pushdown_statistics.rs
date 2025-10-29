use std::sync::Arc;

use arrow::{
    array::AsArray,
    datatypes::{Int8Type, SchemaRef},
};
use beacon_arrow_zarr::reader::AsyncArrowZarrGroupReader;
use datafusion::{
    common::{ColumnStatistics, Statistics, stats::Precision},
    scalar::ScalarValue,
};
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

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
