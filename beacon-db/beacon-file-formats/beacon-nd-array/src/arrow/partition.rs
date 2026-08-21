//! The `PARTITIONED BY` columns of a scan, as columns of a file's batches.
//!
//! A partition column lives in the *path* of a file rather than inside it, so
//! every row that file contributes carries the same value. DataFusion's
//! `FileStream` appends that value per file, which it can do because a plan
//! entry is a file. An nd scan reads a whole collection behind one entry, so it
//! appends the values itself — per morsel, which is per file.
//!
//! The value repeats over the whole file, and an nd array of rank 0 says
//! exactly that: one value, no axis. It broadcasts onto whatever grid the
//! file's own columns define, so every row gets it and nothing is built per
//! row. A scan that reads no column of the file has no such grid, so there the
//! column is stated over the `row` axis instead. See
//! [`FilePartitions::row_columns`].

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{FieldRef, Schema};
use beacon_datafusion_ext::nd::encoding::{encode_nd_array, nd_value_type};
use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray};
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;

/// The axis a partition column is stated over when the scan reads no column of
/// the file. The same name [`encode_flat_batch_as_nd`] uses, so the two agree
/// when they meet in one batch.
///
/// [`encode_flat_batch_as_nd`]: beacon_datafusion_ext::nd::encode_flat_batch_as_nd
const ROW_AXIS: &str = "row";

/// A table's `PARTITIONED BY` fields, and one file's value for each of them.
///
/// The fields are nd-encoded, as they stand in the scan's schema: a format
/// encodes its partition columns along with everything else, so that every
/// column of a batch decodes the same way. The value type of each is read back
/// off the encoding.
#[derive(Debug, Clone, Default)]
pub struct FilePartitions {
    /// The scan's partition fields, nd-encoded, in table order.
    fields: Vec<FieldRef>,
    /// This file's value per field, in the same order.
    values: Vec<ScalarValue>,
}

impl FilePartitions {
    /// An unpartitioned table: no columns to add.
    pub fn none() -> Self {
        Self::default()
    }

    /// The scan's partition fields and one file's values for them.
    ///
    /// A value missing from `values` reads as null, which is what a path that
    /// does not state the column means.
    pub fn new(fields: Vec<FieldRef>, values: Vec<ScalarValue>) -> Self {
        Self { fields, values }
    }

    /// Whether the table has any partition column at all.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Whether `name` is one of the partition columns.
    ///
    /// A partition column shadows a variable of the same name, exactly as it
    /// does for every other format: the value in the path wins.
    pub fn holds(&self, name: &str) -> bool {
        self.fields.iter().any(|field| field.name() == name)
    }

    /// The partition columns `projected` names, as rank-0 arrays.
    ///
    /// One value each, which broadcasts onto the grid the file's own columns
    /// define. In the order `projected` names them, so they append to a batch
    /// read in that same order.
    pub fn scalar_columns(&self, projected: &Schema) -> Result<Vec<ArrayRef>> {
        self.columns(projected, None)
    }

    /// The same columns over a `row` axis of `rows` cells.
    ///
    /// For a scan that reads no column of the file — `SELECT year FROM t`, where
    /// `year` is a partition column. Rank-0 columns alone define a rank-0 grid,
    /// which holds one row, and the file's rows would be lost. The row count of
    /// the read says how many there are, so the axis is stated here.
    pub fn row_columns(&self, projected: &Schema, rows: usize) -> Result<Vec<ArrayRef>> {
        self.columns(projected, Some(rows))
    }

    /// The fields of [`scalar_columns`](Self::scalar_columns), in the same
    /// order: what the columns it returns are called.
    pub fn projected_fields(&self, projected: &Schema) -> Vec<FieldRef> {
        projected
            .fields()
            .iter()
            .filter(|field| self.holds(field.name()))
            .cloned()
            .collect()
    }

    fn columns(&self, projected: &Schema, rows: Option<usize>) -> Result<Vec<ArrayRef>> {
        projected
            .fields()
            .iter()
            .filter(|field| self.holds(field.name()))
            .map(|field| self.column(field, rows))
            .collect()
    }

    /// One partition column, nd-encoded.
    fn column(&self, field: &FieldRef, rows: Option<usize>) -> Result<ArrayRef> {
        let value_type = nd_value_type(field.data_type())?;
        let index = self
            .fields
            .iter()
            .position(|declared| declared.name() == field.name())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "'{}' is not a partition column of this scan",
                    field.name()
                ))
            })?;
        let value = self.values.get(index).cloned().unwrap_or(ScalarValue::Null);

        let values = value.to_array_of_size(rows.unwrap_or(1)).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to build the partition column '{}': {e}",
                field.name()
            ))
        })?;
        // The scan declared the type; a path value that parsed to another one
        // is made to fit rather than reaching the plan as a surprise.
        let values = if values.data_type() == &value_type {
            values
        } else {
            arrow::compute::cast(values.as_ref(), &value_type)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        };

        let dims = match rows {
            None => Dimensions::scalar(),
            Some(rows) => Dimensions::try_new(vec![Dimension::new(ROW_AXIS, rows)])?,
        };
        Ok(encode_nd_array(&NdArrowArray::try_new(values, dims)?))
    }
}

/// A scan's partition fields, nd-encoded so that every column of a batch
/// decodes the same way.
///
/// A format calls this where it encodes its file schema, and hands the result
/// to `TableSchema::new`. What comes back out above the scan is the logical
/// field again, so the table keeps the schema the user declared.
///
/// Nullability is kept, unlike a file column's. A file column is null-filled
/// where a file of the collection lacks it, so it becomes nullable whatever the
/// table said; a partition column is never read from a file, so it stays as the
/// table declared it — and a plan built on that declaration, an aggregate for
/// one, expects nothing else.
pub fn encoded_partition_cols(cols: &[FieldRef]) -> Vec<FieldRef> {
    cols.iter()
        .map(|field| {
            Arc::new(
                beacon_datafusion_ext::nd::nd_encoded_field(field.name(), field.data_type())
                    .with_nullable(field.is_nullable()),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{DataType, Field};
    use beacon_datafusion_ext::nd::encoding::decode_nd_array;

    use super::*;

    fn year() -> FieldRef {
        Arc::new(Field::new("year", DataType::Utf8, false))
    }

    fn partitions() -> FilePartitions {
        FilePartitions::new(
            encoded_partition_cols(&[year()]),
            vec![ScalarValue::Utf8(Some("2023".to_string()))],
        )
    }

    /// The projected schema of a scan that reads `year` and nothing else.
    fn projected() -> Schema {
        Schema::new(encoded_partition_cols(&[year()]))
    }

    /// A partition column carries the file's value, once, on no axis.
    #[test]
    fn a_scalar_column_holds_the_value_once() {
        let columns = partitions()
            .scalar_columns(&projected())
            .expect("the column builds");

        assert_eq!(columns.len(), 1);
        let decoded = decode_nd_array(&columns[0], 0).expect("it decodes");
        assert_eq!(decoded.dims().rank(), 0, "no axis: one value for the file");
        assert_eq!(decoded.values().as_string::<i32>().value(0), "2023");
    }

    /// Over the `row` axis it holds one value per row of the read.
    #[test]
    fn a_row_column_holds_one_value_per_row() {
        let columns = partitions()
            .row_columns(&projected(), 3)
            .expect("the column builds");

        let decoded = decode_nd_array(&columns[0], 0).expect("it decodes");
        assert_eq!(decoded.dims().shape(), vec![3]);
        let values = decoded.values().as_string::<i32>();
        assert_eq!(
            (0..3).map(|i| values.value(i)).collect::<Vec<_>>(),
            ["2023"; 3]
        );
    }

    /// A column the scan does not project is not built.
    #[test]
    fn an_unprojected_partition_column_is_left_out() {
        let month = Arc::new(Field::new("month", DataType::Utf8, false));
        let partitions = FilePartitions::new(
            encoded_partition_cols(&[year(), month]),
            vec![
                ScalarValue::Utf8(Some("2023".to_string())),
                ScalarValue::Utf8(Some("07".to_string())),
            ],
        );

        // The scan asks for `year` alone.
        let columns = partitions
            .scalar_columns(&projected())
            .expect("the column builds");
        assert_eq!(columns.len(), 1, "only what the projection names");
    }

    /// A path that states no value gives a null, not an error.
    #[test]
    fn a_missing_value_reads_as_null() {
        let partitions = FilePartitions::new(encoded_partition_cols(&[year()]), vec![]);

        let columns = partitions
            .scalar_columns(&projected())
            .expect("the column builds");
        let decoded = decode_nd_array(&columns[0], 0).expect("it decodes");
        assert!(decoded.values().is_null(0), "the value is null");
    }

    /// An unpartitioned table adds nothing.
    #[test]
    fn an_unpartitioned_table_adds_no_column() {
        let none = FilePartitions::none();
        assert!(none.is_empty());
        assert!(!none.holds("year"));
        assert!(none.scalar_columns(&projected()).unwrap().is_empty());
    }
}
