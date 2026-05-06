use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::DataType;
use beacon_nd_array::datatypes::NdArrayDataType;
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::scalar::ScalarValue;

use crate::format::statistics::{ColumnStatistics, StatisticsValue};

/// Pre-computed Arrow arrays for a single column's statistics.
///
/// Created from a [`ColumnStatistics`] and its [`NdArrayDataType`], this struct
/// holds the min/max/null/row arrays ready for use in pruning.
pub struct PackedColumnStatistics {
    min_values: ArrayRef,
    max_values: ArrayRef,
    null_counts: ArrayRef,
    row_counts: ArrayRef,
}

impl PackedColumnStatistics {
    /// Build packed statistics from a [`ColumnStatistics`] and its data type.
    ///
    /// `num_containers` determines the length of the output arrays.
    /// Entries beyond the length of `stats` are treated as missing (null).
    pub fn new(stats: &ColumnStatistics, dtype: NdArrayDataType, num_containers: usize) -> Self {
        build_column_arrays(stats, dtype, num_containers)
    }
}

/// A [`PruningStatistics`] implementation backed by Atlas [`ColumnStatistics`].
///
/// Eagerly converts per-dataset statistics into typed Arrow arrays at
/// construction time, so that the trait methods are cheap lookups.
pub struct AtlasPruningStatistics {
    num_containers: usize,
    columns: HashMap<String, PackedColumnStatistics>,
}

impl AtlasPruningStatistics {
    /// Build pruning statistics from a map of column name → packed statistics.
    ///
    /// `num_containers` is the total number of datasets (determines array lengths).
    /// Each [`PackedColumnStatistics`] should have been created with the same
    /// `num_containers` value.
    pub fn new(num_containers: usize, columns: HashMap<String, PackedColumnStatistics>) -> Self {
        Self {
            num_containers,
            columns,
        }
    }
}

impl PruningStatistics for AtlasPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns
            .get(column.name())
            .map(|c| c.min_values.clone())
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns
            .get(column.name())
            .map(|c| c.max_values.clone())
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns
            .get(column.name())
            .map(|c| c.null_counts.clone())
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns
            .get(column.name())
            .map(|c| c.row_counts.clone())
    }

    fn num_containers(&self) -> usize {
        self.num_containers
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<arrow::array::BooleanArray> {
        None
    }
}

/// Convert a [`ColumnStatistics`] into typed Arrow arrays.
fn build_column_arrays(
    stats: &ColumnStatistics,
    dtype: NdArrayDataType,
    num_containers: usize,
) -> PackedColumnStatistics {
    let entries = stats.entries();

    // Build null_counts and row_counts (always UInt64).
    let mut null_counts = UInt64Builder::with_capacity(num_containers);
    let mut row_counts = UInt64Builder::with_capacity(num_containers);

    for i in 0..num_containers {
        match entries.get(i).and_then(|e| e.as_ref()) {
            Some(entry) => {
                null_counts.append_value(entry.null_count);
                row_counts.append_value(entry.row_count);
            }
            None => {
                null_counts.append_null();
                row_counts.append_null();
            }
        }
    }

    // Build min/max arrays typed by NdArrayDataType.
    let (min_values, max_values) = build_min_max_arrays(entries, dtype, num_containers);

    PackedColumnStatistics {
        min_values,
        max_values,
        null_counts: Arc::new(null_counts.finish()),
        row_counts: Arc::new(row_counts.finish()),
    }
}

/// Cast a [`StatisticsValue`] to a target numeric type.
///
/// Returns `None` for non-numeric source variants (String, Timestamp).
macro_rules! cast_stat_numeric {
    ($val:expr, $target:ty) => {
        match $val {
            StatisticsValue::Bool(v) => Some((*v as u8) as $target),
            StatisticsValue::I8(v) => Some(*v as $target),
            StatisticsValue::I16(v) => Some(*v as $target),
            StatisticsValue::I32(v) => Some(*v as $target),
            StatisticsValue::I64(v) => Some(*v as $target),
            StatisticsValue::U8(v) => Some(*v as $target),
            StatisticsValue::U16(v) => Some(*v as $target),
            StatisticsValue::U32(v) => Some(*v as $target),
            StatisticsValue::U64(v) => Some(*v as $target),
            StatisticsValue::F32(v) => Some(*v as $target),
            StatisticsValue::F64(v) => Some(*v as $target),
            _ => None,
        }
    };
}

/// Build min/max arrays for a numeric target type, casting from any numeric source.
macro_rules! build_cast_arrays {
    ($entries:expr, $num:expr, $builder_ty:ty, $target:ty) => {{
        let mut min_builder = <$builder_ty>::with_capacity($num);
        let mut max_builder = <$builder_ty>::with_capacity($num);

        for i in 0..$num {
            match $entries.get(i).and_then(|e| e.as_ref()) {
                Some(entry) => {
                    match &entry.min_value {
                        Some(v) => match cast_stat_numeric!(v, $target) {
                            Some(casted) => min_builder.append_value(casted),
                            None => min_builder.append_null(),
                        },
                        None => min_builder.append_null(),
                    }
                    match &entry.max_value {
                        Some(v) => match cast_stat_numeric!(v, $target) {
                            Some(casted) => max_builder.append_value(casted),
                            None => max_builder.append_null(),
                        },
                        None => max_builder.append_null(),
                    }
                }
                None => {
                    min_builder.append_null();
                    max_builder.append_null();
                }
            }
        }

        (
            Arc::new(min_builder.finish()) as ArrayRef,
            Arc::new(max_builder.finish()) as ArrayRef,
        )
    }};
}

fn build_min_max_arrays(
    entries: &[Option<crate::format::statistics::DatasetStatisticsEntry>],
    dtype: NdArrayDataType,
    num_containers: usize,
) -> (ArrayRef, ArrayRef) {
    match dtype {
        NdArrayDataType::Bool => {
            // Bool is not cast from other types — only accept Bool source.
            let mut min_builder = BooleanBuilder::with_capacity(num_containers);
            let mut max_builder = BooleanBuilder::with_capacity(num_containers);
            for i in 0..num_containers {
                match entries.get(i).and_then(|e| e.as_ref()) {
                    Some(entry) => {
                        match &entry.min_value {
                            Some(StatisticsValue::Bool(v)) => min_builder.append_value(*v),
                            _ => min_builder.append_null(),
                        }
                        match &entry.max_value {
                            Some(StatisticsValue::Bool(v)) => max_builder.append_value(*v),
                            _ => max_builder.append_null(),
                        }
                    }
                    None => {
                        min_builder.append_null();
                        max_builder.append_null();
                    }
                }
            }
            (
                Arc::new(min_builder.finish()) as ArrayRef,
                Arc::new(max_builder.finish()) as ArrayRef,
            )
        }
        NdArrayDataType::I8 => build_cast_arrays!(entries, num_containers, Int8Builder, i8),
        NdArrayDataType::I16 => build_cast_arrays!(entries, num_containers, Int16Builder, i16),
        NdArrayDataType::I32 => build_cast_arrays!(entries, num_containers, Int32Builder, i32),
        NdArrayDataType::I64 => build_cast_arrays!(entries, num_containers, Int64Builder, i64),
        NdArrayDataType::U8 => build_cast_arrays!(entries, num_containers, UInt8Builder, u8),
        NdArrayDataType::U16 => build_cast_arrays!(entries, num_containers, UInt16Builder, u16),
        NdArrayDataType::U32 => build_cast_arrays!(entries, num_containers, UInt32Builder, u32),
        NdArrayDataType::U64 => build_cast_arrays!(entries, num_containers, UInt64Builder, u64),
        NdArrayDataType::F32 => build_cast_arrays!(entries, num_containers, Float32Builder, f32),
        NdArrayDataType::F64 => build_cast_arrays!(entries, num_containers, Float64Builder, f64),
        NdArrayDataType::String | NdArrayDataType::Timestamp | NdArrayDataType::Binary => {
            // String, Timestamp, and Binary columns produce all-null min/max arrays.
            let arrow_dt: DataType = dtype.into();
            let null_array = arrow::array::new_null_array(&arrow_dt, num_containers);
            (null_array.clone(), null_array)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::statistics::{DatasetStatisticsEntry, StatisticsValue};
    use crate::schema::DatasetId;
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, UInt64Array};

    fn make_column_stats(entries: Vec<Option<DatasetStatisticsEntry>>) -> ColumnStatistics {
        let mut stats = ColumnStatistics::new();
        for (i, entry) in entries.into_iter().enumerate() {
            if let Some(e) = entry {
                stats.set(DatasetId::from_raw(i as u32), e);
            }
        }
        stats
    }

    #[test]
    fn test_i32_column() {
        let stats = make_column_stats(vec![
            Some(DatasetStatisticsEntry {
                row_count: 100,
                null_count: 5,
                min_value: Some(StatisticsValue::I32(10)),
                max_value: Some(StatisticsValue::I32(50)),
            }),
            Some(DatasetStatisticsEntry {
                row_count: 200,
                null_count: 0,
                min_value: Some(StatisticsValue::I32(-3)),
                max_value: Some(StatisticsValue::I32(99)),
            }),
            None,
        ]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::I32, 3);
        let pruning =
            AtlasPruningStatistics::new(3, HashMap::from([("temperature".to_string(), packed)]));

        assert_eq!(pruning.num_containers(), 3);

        let col = Column::from_name("temperature");
        let min_arr = pruning.min_values(&col).unwrap();
        let min_i32 = min_arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(min_i32.value(0), 10);
        assert_eq!(min_i32.value(1), -3);
        assert!(min_i32.is_null(2));

        let max_arr = pruning.max_values(&col).unwrap();
        let max_i32 = max_arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(max_i32.value(0), 50);
        assert_eq!(max_i32.value(1), 99);
        assert!(max_i32.is_null(2));

        let null_arr = pruning.null_counts(&col).unwrap();
        let nulls = null_arr.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(nulls.value(0), 5);
        assert_eq!(nulls.value(1), 0);
        assert!(nulls.is_null(2));

        let row_arr = pruning.row_counts(&col).unwrap();
        let rows = row_arr.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(rows.value(0), 100);
        assert_eq!(rows.value(1), 200);
        assert!(rows.is_null(2));
    }

    #[test]
    fn test_f64_column() {
        let stats = make_column_stats(vec![
            Some(DatasetStatisticsEntry {
                row_count: 50,
                null_count: 2,
                min_value: Some(StatisticsValue::F64(1.5)),
                max_value: Some(StatisticsValue::F64(99.9)),
            }),
            Some(DatasetStatisticsEntry {
                row_count: 50,
                null_count: 50,
                min_value: None,
                max_value: None,
            }),
        ]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::F64, 2);
        let pruning =
            AtlasPruningStatistics::new(2, HashMap::from([("depth".to_string(), packed)]));

        let col = Column::from_name("depth");
        let min_arr = pruning.min_values(&col).unwrap();
        let min_f64 = min_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(min_f64.value(0), 1.5);
        assert!(min_f64.is_null(1));

        let max_arr = pruning.max_values(&col).unwrap();
        let max_f64 = max_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(max_f64.value(0), 99.9);
        assert!(max_f64.is_null(1));
    }

    #[test]
    fn test_string_column_all_null() {
        let stats = make_column_stats(vec![Some(DatasetStatisticsEntry {
            row_count: 10,
            null_count: 0,
            min_value: Some(StatisticsValue::String("alpha".into())),
            max_value: Some(StatisticsValue::String("zeta".into())),
        })]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::String, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("name".to_string(), packed)]));

        let col = Column::from_name("name");
        // String columns produce all-null min/max arrays.
        let min_arr = pruning.min_values(&col).unwrap();
        assert!(min_arr.is_null(0));
        let max_arr = pruning.max_values(&col).unwrap();
        assert!(max_arr.is_null(0));
    }

    #[test]
    fn test_missing_column_returns_none() {
        let stats = make_column_stats(vec![]);
        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::I32, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("x".to_string(), packed)]));

        let col = Column::from_name("not_present");
        assert!(pruning.min_values(&col).is_none());
        assert!(pruning.max_values(&col).is_none());
        assert!(pruning.null_counts(&col).is_none());
        assert!(pruning.row_counts(&col).is_none());
    }

    #[test]
    fn test_cross_type_numeric_cast() {
        // Column is declared as I32, stored value is F64 — should cast via truncation.
        let stats = make_column_stats(vec![Some(DatasetStatisticsEntry {
            row_count: 10,
            null_count: 0,
            min_value: Some(StatisticsValue::F64(1.7)),
            max_value: Some(StatisticsValue::F64(9.9)),
        })]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::I32, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("col".to_string(), packed)]));

        let col = Column::from_name("col");
        let min_arr = pruning.min_values(&col).unwrap();
        let min_i32 = min_arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(min_i32.value(0), 1); // truncated from 1.7

        let max_arr = pruning.max_values(&col).unwrap();
        let max_i32 = max_arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(max_i32.value(0), 9); // truncated from 9.9
    }

    #[test]
    fn test_cast_i8_to_f64() {
        // I8 source → F64 target: lossless widening.
        let stats = make_column_stats(vec![Some(DatasetStatisticsEntry {
            row_count: 5,
            null_count: 0,
            min_value: Some(StatisticsValue::I8(-10)),
            max_value: Some(StatisticsValue::I8(127)),
        })]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::F64, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("x".to_string(), packed)]));

        let col = Column::from_name("x");
        let min_arr = pruning.min_values(&col).unwrap();
        let min_f64 = min_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(min_f64.value(0), -10.0);

        let max_arr = pruning.max_values(&col).unwrap();
        let max_f64 = max_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(max_f64.value(0), 127.0);
    }

    #[test]
    fn test_cast_u64_to_i64() {
        // U64 source → I64 target: wrapping cast.
        let stats = make_column_stats(vec![Some(DatasetStatisticsEntry {
            row_count: 1,
            null_count: 0,
            min_value: Some(StatisticsValue::U64(42)),
            max_value: Some(StatisticsValue::U64(1000)),
        })]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::I64, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("x".to_string(), packed)]));

        let col = Column::from_name("x");
        let min_arr = pruning.min_values(&col).unwrap();
        let arr = min_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 42);

        let max_arr = pruning.max_values(&col).unwrap();
        let arr = max_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 1000);
    }

    #[test]
    fn test_cast_i32_to_f32() {
        // I32 source → F32 target.
        let stats = make_column_stats(vec![Some(DatasetStatisticsEntry {
            row_count: 1,
            null_count: 0,
            min_value: Some(StatisticsValue::I32(100)),
            max_value: Some(StatisticsValue::I32(200)),
        })]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::F32, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("x".to_string(), packed)]));

        let col = Column::from_name("x");
        let min_arr = pruning.min_values(&col).unwrap();
        let arr = min_arr
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 100.0);
    }

    #[test]
    fn test_cast_string_source_to_numeric_produces_null() {
        // String source value cannot be cast to a numeric target → null.
        let stats = make_column_stats(vec![Some(DatasetStatisticsEntry {
            row_count: 1,
            null_count: 0,
            min_value: Some(StatisticsValue::String("hello".into())),
            max_value: Some(StatisticsValue::String("world".into())),
        })]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::I64, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("x".to_string(), packed)]));

        let col = Column::from_name("x");
        let min_arr = pruning.min_values(&col).unwrap();
        let arr = min_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));

        let max_arr = pruning.max_values(&col).unwrap();
        let arr = max_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn test_cast_timestamp_source_to_numeric_produces_null() {
        // Timestamp source value cannot be cast to a numeric target → null.
        let stats = make_column_stats(vec![Some(DatasetStatisticsEntry {
            row_count: 1,
            null_count: 0,
            min_value: Some(StatisticsValue::Timestamp(1_000_000_000)),
            max_value: Some(StatisticsValue::Timestamp(2_000_000_000)),
        })]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::I64, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("t".to_string(), packed)]));

        let col = Column::from_name("t");
        let min_arr = pruning.min_values(&col).unwrap();
        let arr = min_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn test_cast_mixed_entries() {
        // Multiple entries with different source types, all cast to F64.
        let stats = make_column_stats(vec![
            Some(DatasetStatisticsEntry {
                row_count: 10,
                null_count: 0,
                min_value: Some(StatisticsValue::I32(5)),
                max_value: Some(StatisticsValue::U8(200)),
            }),
            Some(DatasetStatisticsEntry {
                row_count: 10,
                null_count: 0,
                min_value: Some(StatisticsValue::F32(1.5)),
                max_value: Some(StatisticsValue::I64(9999)),
            }),
            Some(DatasetStatisticsEntry {
                row_count: 10,
                null_count: 0,
                min_value: Some(StatisticsValue::String("x".into())), // → null
                max_value: Some(StatisticsValue::F64(42.0)),
            }),
        ]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::F64, 3);
        let pruning = AtlasPruningStatistics::new(3, HashMap::from([("m".to_string(), packed)]));

        let col = Column::from_name("m");
        let min_arr = pruning.min_values(&col).unwrap();
        let min_f64 = min_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(min_f64.value(0), 5.0); // I32 → F64
        assert_eq!(min_f64.value(1), 1.5); // F32 → F64
        assert!(min_f64.is_null(2)); // String → null

        let max_arr = pruning.max_values(&col).unwrap();
        let max_f64 = max_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(max_f64.value(0), 200.0); // U8 → F64
        assert_eq!(max_f64.value(1), 9999.0); // I64 → F64
        assert_eq!(max_f64.value(2), 42.0); // F64 → F64
    }

    #[test]
    fn test_string_column_produces_null_minmax() {
        let stats = make_column_stats(vec![Some(DatasetStatisticsEntry {
            row_count: 10,
            null_count: 0,
            min_value: Some(StatisticsValue::String("alpha".into())),
            max_value: Some(StatisticsValue::String("zeta".into())),
        })]);

        let packed = PackedColumnStatistics::new(&stats, NdArrayDataType::String, 1);
        let pruning = AtlasPruningStatistics::new(1, HashMap::from([("s".to_string(), packed)]));

        let col = Column::from_name("s");
        let min_arr = pruning.min_values(&col).unwrap();
        // All values should be null for String columns
        assert!(min_arr.is_null(0));
    }
}
