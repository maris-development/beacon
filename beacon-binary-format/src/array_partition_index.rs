use std::{fs::File, sync::Arc};

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, BooleanBuilder, LargeBinaryBuilder,
    LargeStringBuilder, ListArray, PrimitiveBuilder, RecordBatch, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{
    ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_ipc::reader::FileReader;
use arrow_schema::DataType;

use crate::error::{BBFResult, BBFWritingError};

/// Returns a pruning schema containing min, max, null-count, and row-count fields
/// derived from `data_type` for the provided `array_name`.
///
/// The schema matches the layout produced by [`build_pruning_index`] so that metadata
/// batches can be concatenated cheaply across array partitions.
fn schema_for_data_type(array_name: &str, data_type: &DataType) -> arrow::datatypes::Schema {
    let min_field =
        arrow::datatypes::Field::new(format!("{}:min", array_name), data_type.clone(), true);
    let max_field =
        arrow::datatypes::Field::new(format!("{}:max", array_name), data_type.clone(), true);
    let null_count_field = arrow::datatypes::Field::new(
        format!("{}:null_count", array_name),
        DataType::UInt64,
        false,
    );
    let row_count_field =
        arrow::datatypes::Field::new(format!("{}:row_count", array_name), DataType::UInt64, false);

    arrow::datatypes::Schema::new(vec![
        min_field,
        max_field,
        null_count_field,
        row_count_field,
    ])
}

/// Builds a pruning index [`RecordBatch`] for `array_name` by scanning the IPC `file`
/// and computing min/max metadata for each nested list according to `data_type`.
///
/// The IPC file is expected to contain a single column with list-arrays. Each nested
/// list is summarized into four columns (min, max, null_count, row_count). The output
/// is already aligned with the pruning schema which allows callers to persist the batch
/// directly or concatenate it with previous index fragments.
pub fn build_pruning_index(
    array_name: &str,
    file: &mut File,
    data_type: DataType,
) -> BBFResult<Option<RecordBatch>> {
    let reader = FileReader::try_new(file, None)
        .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
    let pruning_schema = schema_for_data_type(array_name, &data_type);
    let mut pruning_batches = Vec::new();

    // Iterate through all batches and build pruning index
    for maybe_batch in reader {
        let batch =
            maybe_batch.map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
        let values_column = batch.column(0).clone();
        let values_list_column = values_column.as_list::<i32>();
        if let Some(pruning_batch) = build_from_values(&pruning_schema, values_list_column)? {
            pruning_batches.push(pruning_batch);
        }
    }

    let result = match pruning_batches.len() {
        0 => None,
        1 => pruning_batches.pop(),
        _ => {
            let schema_ref = pruning_batches[0].schema();
            let concatenated = arrow::compute::concat_batches(&schema_ref, &pruning_batches)
                .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
            Some(concatenated)
        }
    };

    Ok(result)
}

/// Derives a pruning [`RecordBatch`] from a [`ListArray`], returning `None` when there
/// are no rows to index.
///
/// This function accepts the list column extracted from a [`RecordBatch`] and performs
/// type-directed dispatch to the appropriate min/max implementation. Unsupported data
/// types simply skip pruning generation to keep the caller fast-failing.
fn build_from_values(
    pruning_schema: &arrow::datatypes::Schema,
    list_array: &ListArray,
) -> BBFResult<Option<RecordBatch>> {
    if list_array.len() == 0 {
        return Ok(None);
    }

    let mut null_count_builder = UInt64Builder::with_capacity(list_array.len());
    let mut row_count_builder = UInt64Builder::with_capacity(list_array.len());

    let maybe_arrays = match list_array.value_type() {
        DataType::Boolean => Some(build_boolean_min_max(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::Int8 => Some(build_primitive_min_max::<Int8Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::Int16 => Some(build_primitive_min_max::<Int16Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::Int32 => Some(build_primitive_min_max::<Int32Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::Int64 => Some(build_primitive_min_max::<Int64Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::UInt8 => Some(build_primitive_min_max::<UInt8Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::UInt16 => Some(build_primitive_min_max::<UInt16Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::UInt32 => Some(build_primitive_min_max::<UInt32Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::UInt64 => Some(build_primitive_min_max::<UInt64Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::Float32 => Some(build_primitive_min_max::<Float32Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::Float64 => Some(build_primitive_min_max::<Float64Type>(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::Timestamp(time_unit, _) => match time_unit {
            arrow_schema::TimeUnit::Second => Some(build_primitive_min_max::<TimestampSecondType>(
                list_array,
                &mut null_count_builder,
                &mut row_count_builder,
            )),
            arrow_schema::TimeUnit::Millisecond => {
                Some(build_primitive_min_max::<TimestampMillisecondType>(
                    list_array,
                    &mut null_count_builder,
                    &mut row_count_builder,
                ))
            }
            arrow_schema::TimeUnit::Microsecond => {
                Some(build_primitive_min_max::<TimestampMicrosecondType>(
                    list_array,
                    &mut null_count_builder,
                    &mut row_count_builder,
                ))
            }
            arrow_schema::TimeUnit::Nanosecond => {
                Some(build_primitive_min_max::<TimestampNanosecondType>(
                    list_array,
                    &mut null_count_builder,
                    &mut row_count_builder,
                ))
            }
        },
        DataType::Binary => Some(build_binary_min_max(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::LargeBinary => Some(build_large_binary_min_max(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::Utf8 => Some(build_utf8_min_max(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        DataType::LargeUtf8 => Some(build_large_utf8_min_max(
            list_array,
            &mut null_count_builder,
            &mut row_count_builder,
        )),
        _ => None,
    };

    let Some((min_array, max_array)) = maybe_arrays else {
        return Ok(None);
    };

    let null_count_array: ArrayRef = Arc::new(null_count_builder.finish());
    let row_count_array: ArrayRef = Arc::new(row_count_builder.finish());

    let batch = RecordBatch::try_new(
        Arc::new(pruning_schema.clone()),
        vec![min_array, max_array, null_count_array, row_count_array],
    )
    .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

    Ok(Some(batch))
}

/// Computes min/max pairs for primitive list values while tracking row and null counts.
///
/// The helper leverages Arrow's `min`/`max` kernels which avoid materializing temporary
/// vectors. Builders are appended in lockstep so every parent row contributes exactly one
/// entry to the resulting metadata columns.
fn build_primitive_min_max<T: ArrowPrimitiveType>(
    list_array: &ListArray,
    null_count_builder: &mut UInt64Builder,
    row_count_builder: &mut UInt64Builder,
) -> (ArrayRef, ArrayRef) {
    let mut min_builder = PrimitiveBuilder::<T>::with_capacity(list_array.len());
    let mut max_builder = PrimitiveBuilder::<T>::with_capacity(list_array.len());

    // Iterate over each parent list row, emitting metadata no matter the child length.
    for idx in 0..list_array.len() {
        if list_array.is_null(idx) {
            min_builder.append_null();
            max_builder.append_null();
            append_zero_counts(null_count_builder, row_count_builder);
            continue;
        }

        let values = list_array.value(idx);
        let typed_values = values.as_primitive::<T>();
        append_counts(
            null_count_builder,
            row_count_builder,
            typed_values.len(),
            typed_values.null_count(),
        );

        match arrow::compute::min(typed_values) {
            Some(value) => min_builder.append_value(value),
            None => min_builder.append_null(),
        }

        match arrow::compute::max(typed_values) {
            Some(value) => max_builder.append_value(value),
            None => max_builder.append_null(),
        }
    }

    (
        Arc::new(min_builder.finish()) as ArrayRef,
        Arc::new(max_builder.finish()) as ArrayRef,
    )
}

/// Computes min/max values for boolean lists using Arrow's dedicated reducers.
fn build_boolean_min_max(
    list_array: &ListArray,
    null_count_builder: &mut UInt64Builder,
    row_count_builder: &mut UInt64Builder,
) -> (ArrayRef, ArrayRef) {
    let mut min_builder = BooleanBuilder::with_capacity(list_array.len());
    let mut max_builder = BooleanBuilder::with_capacity(list_array.len());

    // Boolean lists rely on their own min/max kernels for cheap aggregation.
    for idx in 0..list_array.len() {
        if list_array.is_null(idx) {
            min_builder.append_null();
            max_builder.append_null();
            append_zero_counts(null_count_builder, row_count_builder);
            continue;
        }

        let values = list_array.value(idx);
        let boolean_values = values.as_boolean();
        append_counts(
            null_count_builder,
            row_count_builder,
            boolean_values.len(),
            boolean_values.null_count(),
        );

        match arrow::compute::min_boolean(boolean_values) {
            Some(value) => min_builder.append_value(value),
            None => min_builder.append_null(),
        }

        match arrow::compute::max_boolean(boolean_values) {
            Some(value) => max_builder.append_value(value),
            None => max_builder.append_null(),
        }
    }

    (
        Arc::new(min_builder.finish()) as ArrayRef,
        Arc::new(max_builder.finish()) as ArrayRef,
    )
}

/// Computes lexical min/max for binary lists that use 32-bit offsets.
fn build_binary_min_max(
    list_array: &ListArray,
    null_count_builder: &mut UInt64Builder,
    row_count_builder: &mut UInt64Builder,
) -> (ArrayRef, ArrayRef) {
    let mut min_builder = BinaryBuilder::new();
    let mut max_builder = BinaryBuilder::new();

    // For lexical ordering the Arrow kernels operate on slices, no extra copies needed.
    for idx in 0..list_array.len() {
        if list_array.is_null(idx) {
            min_builder.append_null();
            max_builder.append_null();
            append_zero_counts(null_count_builder, row_count_builder);
            continue;
        }

        let values = list_array.value(idx);
        let binary_values = values.as_binary::<i32>();
        append_counts(
            null_count_builder,
            row_count_builder,
            binary_values.len(),
            binary_values.null_count(),
        );

        match arrow::compute::min_binary(binary_values) {
            Some(bytes) => min_builder.append_value(bytes),
            None => min_builder.append_null(),
        }

        match arrow::compute::max_binary(binary_values) {
            Some(bytes) => max_builder.append_value(bytes),
            None => max_builder.append_null(),
        }
    }

    (
        Arc::new(min_builder.finish()) as ArrayRef,
        Arc::new(max_builder.finish()) as ArrayRef,
    )
}

/// Computes lexical min/max for binary lists that use 64-bit offsets.
fn build_large_binary_min_max(
    list_array: &ListArray,
    null_count_builder: &mut UInt64Builder,
    row_count_builder: &mut UInt64Builder,
) -> (ArrayRef, ArrayRef) {
    let mut min_builder = LargeBinaryBuilder::new();
    let mut max_builder = LargeBinaryBuilder::new();

    for idx in 0..list_array.len() {
        if list_array.is_null(idx) {
            min_builder.append_null();
            max_builder.append_null();
            append_zero_counts(null_count_builder, row_count_builder);
            continue;
        }

        let values = list_array.value(idx);
        let binary_values = values.as_binary::<i64>();
        append_counts(
            null_count_builder,
            row_count_builder,
            binary_values.len(),
            binary_values.null_count(),
        );

        match arrow::compute::min_binary(binary_values) {
            Some(bytes) => min_builder.append_value(bytes),
            None => min_builder.append_null(),
        }

        match arrow::compute::max_binary(binary_values) {
            Some(bytes) => max_builder.append_value(bytes),
            None => max_builder.append_null(),
        }
    }

    (
        Arc::new(min_builder.finish()) as ArrayRef,
        Arc::new(max_builder.finish()) as ArrayRef,
    )
}

/// Computes lexical min/max for UTF-8 string lists that use 32-bit offsets.
fn build_utf8_min_max(
    list_array: &ListArray,
    null_count_builder: &mut UInt64Builder,
    row_count_builder: &mut UInt64Builder,
) -> (ArrayRef, ArrayRef) {
    let mut min_builder = StringBuilder::new();
    let mut max_builder = StringBuilder::new();

    // UTF-8 min/max is performed lexically, matching the semantics of Arrow comparisons.
    for idx in 0..list_array.len() {
        if list_array.is_null(idx) {
            min_builder.append_null();
            max_builder.append_null();
            append_zero_counts(null_count_builder, row_count_builder);
            continue;
        }

        let values = list_array.value(idx);
        let string_values = values.as_string::<i32>();
        append_counts(
            null_count_builder,
            row_count_builder,
            string_values.len(),
            string_values.null_count(),
        );

        match arrow::compute::min_string(string_values) {
            Some(value) => min_builder.append_value(value),
            None => min_builder.append_null(),
        }

        match arrow::compute::max_string(string_values) {
            Some(value) => max_builder.append_value(value),
            None => max_builder.append_null(),
        }
    }

    (
        Arc::new(min_builder.finish()) as ArrayRef,
        Arc::new(max_builder.finish()) as ArrayRef,
    )
}

/// Computes lexical min/max for UTF-8 string lists that use 64-bit offsets.
fn build_large_utf8_min_max(
    list_array: &ListArray,
    null_count_builder: &mut UInt64Builder,
    row_count_builder: &mut UInt64Builder,
) -> (ArrayRef, ArrayRef) {
    let mut min_builder = LargeStringBuilder::new();
    let mut max_builder = LargeStringBuilder::new();

    for idx in 0..list_array.len() {
        if list_array.is_null(idx) {
            min_builder.append_null();
            max_builder.append_null();
            append_zero_counts(null_count_builder, row_count_builder);
            continue;
        }

        let values = list_array.value(idx);
        let string_values = values.as_string::<i64>();
        append_counts(
            null_count_builder,
            row_count_builder,
            string_values.len(),
            string_values.null_count(),
        );

        match arrow::compute::min_string(string_values) {
            Some(value) => min_builder.append_value(value),
            None => min_builder.append_null(),
        }

        match arrow::compute::max_string(string_values) {
            Some(value) => max_builder.append_value(value),
            None => max_builder.append_null(),
        }
    }

    (
        Arc::new(min_builder.finish()) as ArrayRef,
        Arc::new(max_builder.finish()) as ArrayRef,
    )
}

/// Records zero row/null counts for parent entries that were null.
fn append_zero_counts(
    null_count_builder: &mut UInt64Builder,
    row_count_builder: &mut UInt64Builder,
) {
    null_count_builder.append_value(0);
    row_count_builder.append_value(0);
}

/// Records the total row count and null count for non-null parent entries.
fn append_counts(
    null_count_builder: &mut UInt64Builder,
    row_count_builder: &mut UInt64Builder,
    len: usize,
    nulls: usize,
) {
    row_count_builder.append_value(len as u64);
    null_count_builder.append_value(nulls as u64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanBuilder, ListArray, ListBuilder, StringBuilder};
    use arrow::datatypes::{DataType, Int32Type, UInt64Type};

    fn build_boolean_list(rows: &[Option<Vec<Option<bool>>>]) -> ListArray {
        let mut builder = ListBuilder::new(BooleanBuilder::new());
        for row in rows {
            match row {
                Some(values) => {
                    for value in values {
                        match value {
                            Some(bit) => {
                                builder.values().append_value(*bit);
                            }
                            None => {
                                builder.values().append_null();
                            }
                        }
                    }
                    builder.append(true);
                }
                None => {
                    builder.append(false);
                }
            }
        }

        builder.finish()
    }

    fn build_utf8_list(rows: &[Option<Vec<Option<&str>>>]) -> ListArray {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                Some(values) => {
                    for value in values {
                        match value {
                            Some(text) => {
                                builder.values().append_value(text);
                            }
                            None => {
                                builder.values().append_null();
                            }
                        }
                    }
                    builder.append(true);
                }
                None => {
                    builder.append(false);
                }
            }
        }

        builder.finish()
    }

    #[test]
    fn primitive_lists_generate_expected_pruning_batch() {
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3), Some(1), None]),
            Some(vec![Some(8), Some(4)]),
            None,
        ]);

        let schema = schema_for_data_type("values", &DataType::Int32);
        let batch = build_from_values(&schema, &list_array)
            .expect("build succeeds")
            .expect("batch generated");

        let min_values = batch.column(0).as_primitive::<Int32Type>();
        assert_eq!(min_values.len(), 3);
        assert_eq!(min_values.value(0), 1);
        assert_eq!(min_values.value(1), 4);
        assert!(min_values.is_null(2));

        let max_values = batch.column(1).as_primitive::<Int32Type>();
        assert_eq!(max_values.value(0), 3);
        assert_eq!(max_values.value(1), 8);
        assert!(max_values.is_null(2));

        let null_counts = batch.column(2).as_primitive::<UInt64Type>();
        assert_eq!(null_counts.value(0), 1);
        assert_eq!(null_counts.value(1), 0);
        assert_eq!(null_counts.value(2), 0);

        let row_counts = batch.column(3).as_primitive::<UInt64Type>();
        assert_eq!(row_counts.value(0), 3);
        assert_eq!(row_counts.value(1), 2);
        assert_eq!(row_counts.value(2), 0);
    }

    #[test]
    fn empty_list_array_produces_no_batch() {
        let empty_lists: Vec<Option<Vec<Option<i32>>>> = Vec::new();
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(empty_lists);
        let schema = schema_for_data_type("values", &DataType::Int32);

        let result = build_from_values(&schema, &list_array).expect("build succeeds");
        assert!(result.is_none());
    }

    #[test]
    fn boolean_lists_report_expected_bounds() {
        let list_array = build_boolean_list(&[
            Some(vec![Some(true), Some(false), Some(true)]),
            Some(vec![None, Some(true)]),
            None,
        ]);
        let schema = schema_for_data_type("flags", &DataType::Boolean);
        let batch = build_from_values(&schema, &list_array)
            .expect("build succeeds")
            .expect("batch generated");

        let min_values = batch.column(0).as_boolean();
        assert_eq!(min_values.len(), 3);
        assert!(!min_values.value(0));
        assert!(min_values.value(1));
        assert!(min_values.is_null(2));

        let max_values = batch.column(1).as_boolean();
        assert!(max_values.value(0));
        assert!(max_values.value(1));
        assert!(max_values.is_null(2));

        let null_counts = batch.column(2).as_primitive::<UInt64Type>();
        assert_eq!(null_counts.value(0), 0);
        assert_eq!(null_counts.value(1), 1);
        assert_eq!(null_counts.value(2), 0);

        let row_counts = batch.column(3).as_primitive::<UInt64Type>();
        assert_eq!(row_counts.value(0), 3);
        assert_eq!(row_counts.value(1), 2);
        assert_eq!(row_counts.value(2), 0);
    }

    #[test]
    fn utf8_lists_report_expected_bounds() {
        let list_array = build_utf8_list(&[
            Some(vec![Some("delta"), Some("alpha")]),
            Some(vec![None, Some("zulu"), Some("beta")]),
            None,
        ]);
        let schema = schema_for_data_type("labels", &DataType::Utf8);
        let batch = build_from_values(&schema, &list_array)
            .expect("build succeeds")
            .expect("batch generated");

        let min_values = batch.column(0).as_string::<i32>();
        assert_eq!(min_values.value(0), "alpha");
        assert_eq!(min_values.value(1), "beta");
        assert!(min_values.is_null(2));

        let max_values = batch.column(1).as_string::<i32>();
        assert_eq!(max_values.value(0), "delta");
        assert_eq!(max_values.value(1), "zulu");
        assert!(max_values.is_null(2));

        let null_counts = batch.column(2).as_primitive::<UInt64Type>();
        assert_eq!(null_counts.value(0), 0);
        assert_eq!(null_counts.value(1), 1);
        assert_eq!(null_counts.value(2), 0);

        let row_counts = batch.column(3).as_primitive::<UInt64Type>();
        assert_eq!(row_counts.value(0), 2);
        assert_eq!(row_counts.value(1), 3);
        assert_eq!(row_counts.value(2), 0);
    }
}
