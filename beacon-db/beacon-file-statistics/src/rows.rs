//! The record batch that holds the rows of analyzed files.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray, TimestampMillisecondArray,
    UInt64Array,
};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::scalar::ScalarValue;

use crate::analyze::{AnalyzedFile, ColumnValues};
use crate::layout::{Layout, range_fields};

/// One row for each file of `files`, in the schema of `layout`.
///
/// # Errors
///
/// Returns an error when a value does not have the type of its column.
pub(crate) fn record_batch(layout: &Layout, files: &[AnalyzedFile]) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(files.iter().map(|f| f.stamp))),
        Arc::new(StringArray::from_iter_values(
            files.iter().map(|f| f.object.location.as_ref()),
        )),
        Arc::new(UInt64Array::from_iter_values(
            files.iter().map(|f| f.object.size),
        )),
        Arc::new(
            TimestampMillisecondArray::from_iter_values(
                files
                    .iter()
                    .map(|f| f.object.last_modified.timestamp_millis()),
            )
            .with_timezone("UTC"),
        ),
        Arc::new(BooleanArray::from_iter(
            files.iter().map(|f| Some(f.failed)),
        )),
        Arc::new(UInt64Array::from_iter(files.iter().map(|f| f.row_count))),
    ];
    for (index, column) in layout.columns.iter().enumerate() {
        let values = files.iter().map(|file| &file.columns[index]);
        columns.push(range_array(&column.data_type, values)?);
    }
    Ok(RecordBatch::try_new(Arc::clone(&layout.schema), columns)?)
}

/// The struct array `min, max, null_count` of one statistics column.
fn range_array<'a>(
    data_type: &DataType,
    values: impl Iterator<Item = &'a ColumnValues> + Clone,
) -> Result<ArrayRef> {
    // A typed null, for each file without a bound.
    let null = ScalarValue::try_from(data_type)?;
    let bound = |pick: fn(&ColumnValues) -> &Option<ScalarValue>| {
        ScalarValue::iter_to_array(
            values
                .clone()
                .map(|value| pick(value).clone().unwrap_or_else(|| null.clone())),
        )
    };
    let min = bound(|value| &value.min)?;
    let max = bound(|value| &value.max)?;
    let null_count: ArrayRef =
        Arc::new(UInt64Array::from_iter(values.map(|value| value.null_count)));
    Ok(Arc::new(StructArray::try_new(
        range_fields(data_type),
        vec![min, max, null_count],
        None,
    )?))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Field, Float64Type, Schema};
    use object_store::ObjectMeta;
    use object_store::path::Path;

    use super::*;
    use crate::layout::{FAILED, PATH};

    fn file(path: &str, min: Option<f64>, failed: bool) -> AnalyzedFile {
        AnalyzedFile {
            object: ObjectMeta {
                location: Path::from(path),
                last_modified: chrono::DateTime::from_timestamp_millis(1_000)
                    .expect("a valid time"),
                size: 3,
                e_tag: None,
                version: None,
            },
            stamp: 7,
            failed,
            row_count: Some(5),
            columns: vec![ColumnValues {
                min: min.map(|v| ScalarValue::Float64(Some(v))),
                max: min.map(|v| ScalarValue::Float64(Some(v + 1.0))),
                null_count: Some(0),
            }],
        }
    }

    #[test]
    fn each_file_gives_one_row_with_typed_nulls_for_missing_bounds() {
        let table = Schema::new(vec![Field::new("TEMP", DataType::Float64, true)]);
        let layout = Layout::new(&table, None).expect("a layout");

        let batch = record_batch(
            &layout,
            &[
                file("a.parquet", Some(1.5), false),
                file("b.parquet", None, true),
            ],
        )
        .expect("a batch");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema(), layout.schema);
        let paths = batch
            .column_by_name(PATH)
            .expect("paths")
            .as_string::<i32>();
        assert_eq!(paths.value(1), "b.parquet");
        let failed = batch.column_by_name(FAILED).expect("failed").as_boolean();
        assert!(failed.value(1));
        let temp = batch.column_by_name("TEMP").expect("TEMP").as_struct();
        let min = temp.column(0).as_primitive::<Float64Type>();
        assert_eq!(min.value(0), 1.5);
        assert!(min.is_null(1));
    }
}
