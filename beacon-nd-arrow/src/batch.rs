use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::{Field, Schema, SchemaRef},
};

use crate::{
    broadcast::broadcast_dimensions, column::NdArrowArrayColumn, error::NdArrayError, extension,
};

/// A batch of ND columns.
///
/// Each column is an [`NdArrowArrayColumn`], meaning each row stores an independent ND array
/// (potentially with a different shape per row).
#[derive(Debug, Clone)]
pub struct NdRecordBatch {
    schema: SchemaRef,
    columns: Vec<NdArrowArrayColumn>,
}

impl NdRecordBatch {
    /// Construct an ND record batch directly from named ND columns.
    ///
    /// This convenience constructor:
    /// - Builds the Arrow `Field`s using [`extension::nd_column_field`].
    /// - Uses the storage type inferred from each [`NdArrowArrayColumn`].
    /// - Marks fields as nullable.
    pub fn from_named_columns<I, S>(columns: I) -> Result<Self, NdArrayError>
    where
        I: IntoIterator<Item = (S, NdArrowArrayColumn)>,
        S: Into<String>,
    {
        let mut fields = Vec::new();
        let mut cols = Vec::new();

        for (name, col) in columns {
            let field = extension::nd_column_field(name.into(), col.storage_type().clone(), true)?;
            fields.push(field);
            cols.push(col);
        }

        Self::new(fields, cols)
    }

    /// Construct an ND record batch from Arrow fields and ND columns.
    ///
    /// Requirements:
    /// - `fields.len() == columns.len()`
    /// - All columns have the same number of rows.
    /// - Each field is tagged as an ND column field and its storage type matches the column.
    pub fn new(fields: Vec<Field>, columns: Vec<NdArrowArrayColumn>) -> Result<Self, NdArrayError> {
        if fields.len() != columns.len() {
            return Err(NdArrayError::InvalidNdBatch(
                "number of fields must match number of columns".into(),
            ));
        }

        if let Some((first, rest)) = columns.split_first() {
            let expected_rows = first.len();
            for (idx, col) in rest.iter().enumerate() {
                if col.len() != expected_rows {
                    return Err(NdArrayError::InvalidNdBatch(format!(
                        "column row count mismatch at index {}: {} != {}",
                        idx + 1,
                        col.len(),
                        expected_rows
                    )));
                }
            }
        }

        for (idx, (field, col)) in fields.iter().zip(columns.iter()).enumerate() {
            if !extension::is_nd_column_field(field) {
                return Err(NdArrayError::InvalidNdBatch(format!(
                    "field at index {idx} is not tagged as an ND column field"
                )));
            }

            let storage = extension::nd_storage_type_from_field(field)?;
            if &storage != col.storage_type() {
                return Err(NdArrayError::InvalidNdBatch(format!(
                    "field/column storage type mismatch at index {idx}: {storage:?} != {:?}",
                    col.storage_type()
                )));
            }
        }

        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            columns,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn columns(&self) -> &[NdArrowArrayColumn] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn num_rows(&self) -> usize {
        self.columns.first().map(|c| c.len()).unwrap_or(0)
    }

    /// Convert to a plain Arrow `RecordBatch` containing the physical ND column arrays.
    pub fn to_arrow_record_batch(&self) -> Result<RecordBatch, NdArrayError> {
        let arrays = self
            .columns
            .iter()
            .cloned()
            .map(|c| c.into_array_ref())
            .collect::<Vec<_>>();

        Ok(RecordBatch::try_new(self.schema(), arrays)?)
    }

    /// Broadcast values **within each row** across all columns, returning a new batch.
    ///
    /// For each row `r`:
    /// 1) Read each column's `NdArrowArray` value.
    /// 2) Compute a common broadcast target dimensions for that row.
    /// 3) Broadcast each value to that target and materialize it.
    ///
    /// This preserves the per-row nature of ND columns: the resulting batch is still a set of ND
    /// columns, but now all columns share the same dimensions per row.
    pub fn broadcast_rows(&self) -> Result<Self, NdArrayError> {
        let ncols = self.columns.len();
        let nrows = self.num_rows();

        if ncols == 0 || nrows == 0 {
            return Ok(self.clone());
        }

        let mut out_rows_per_col = (0..ncols)
            .map(|_| Vec::with_capacity(nrows))
            .collect::<Vec<_>>();

        for row_idx in 0..nrows {
            let mut row_vals = Vec::with_capacity(ncols);
            for col in &self.columns {
                row_vals.push(col.row(row_idx)?);
            }

            let dims = row_vals
                .iter()
                .map(|v| v.dimensions().clone())
                .collect::<Vec<_>>();
            let target = broadcast_dimensions(&dims).map_err(NdArrayError::BroadcastingError)?;

            for (col_idx, v) in row_vals.into_iter().enumerate() {
                out_rows_per_col[col_idx].push(v.broadcast_to(&target)?);
            }
        }

        let out_columns = out_rows_per_col
            .into_iter()
            .map(NdArrowArrayColumn::from_rows)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            schema: self.schema.clone(),
            columns: out_columns,
        })
    }

    /// Convenience: broadcast rows and return a physical Arrow `RecordBatch`.
    pub fn to_broadcasted_arrow_record_batch(&self) -> Result<RecordBatch, NdArrayError> {
        self.broadcast_rows()?.to_arrow_record_batch()
    }

    /// Broadcast a single row across all columns and return a *flat* Arrow `RecordBatch`.
    ///
    /// The output `RecordBatch` has one Arrow field per ND column, but the columns are the
    /// materialized flat storage arrays (e.g. `Int32Array`) for that row after broadcasting.
    ///
    /// This is useful when you want to evaluate elementwise expressions across multiple ND
    /// variables: broadcasting aligns the shapes, then the resulting columns are plain Arrow
    /// arrays of identical length.
    pub fn broadcast_row_to_record_batch(
        &self,
        row_idx: usize,
    ) -> Result<RecordBatch, NdArrayError> {
        let ncols = self.columns.len();
        let nrows = self.num_rows();

        if row_idx >= nrows {
            return Err(NdArrayError::InvalidNdBatch(
                "row index out of bounds".into(),
            ));
        }

        if ncols == 0 {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        let mut row_vals = Vec::with_capacity(ncols);
        for col in &self.columns {
            row_vals.push(col.row(row_idx)?);
        }

        let dims = row_vals
            .iter()
            .map(|v| v.dimensions().clone())
            .collect::<Vec<_>>();
        let target = broadcast_dimensions(&dims).map_err(NdArrayError::BroadcastingError)?;

        let mut out_arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
        for v in row_vals {
            out_arrays.push(v.broadcast_to(&target)?.values().clone());
        }

        let flat_fields = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                let storage = extension::nd_storage_type_from_field(f)?;
                Ok(Field::new(f.name().clone(), storage, f.is_nullable()))
            })
            .collect::<Result<Vec<_>, NdArrayError>>()?;

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(flat_fields)),
            out_arrays,
        )?)
    }

    /// Broadcast each row independently into a *flat* Arrow `RecordBatch`.
    ///
    /// Unlike [`NdRecordBatch::broadcast_rows`], this does not short-circuit on error.
    /// Instead, it returns one `Result<RecordBatch, NdArrayError>` per input row.
    pub fn broadcast_rows_to_record_batches(&self) -> Vec<Result<RecordBatch, NdArrayError>> {
        (0..self.num_rows())
            .map(|row_idx| self.broadcast_row_to_record_batch(row_idx))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use arrow_schema::DataType;

    use super::NdRecordBatch;
    use crate::{
        NdArrowArray,
        column::NdArrowArrayColumn,
        dimensions::{Dimension, Dimensions},
        error::NdArrayError,
        extension,
    };

    #[test]
    fn nd_record_batch_broadcasts() {
        // Two columns with 2 rows each.
        // Row 0 requires broadcasting: (t=1,x=3) with (t=2,x=3) -> (t=2,x=3)
        // Row 1 already matches: both (t=2,x=3)
        let a0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let a1 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![4, 5, 6, 7, 8, 9])),
            Dimensions::new(vec![
                Dimension::try_new("t", 2).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let b0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            Dimensions::new(vec![
                Dimension::try_new("t", 2).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let b1 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![70, 80, 90, 100, 110, 120])),
            Dimensions::new(vec![
                Dimension::try_new("t", 2).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let col_a = NdArrowArrayColumn::from_rows(vec![a0, a1]).unwrap();
        let col_b = NdArrowArrayColumn::from_rows(vec![b0, b1]).unwrap();

        let fields = vec![
            extension::nd_column_field("a", DataType::Int32, true).unwrap(),
            extension::nd_column_field("b", DataType::Int32, true).unwrap(),
        ];
        let nd = NdRecordBatch::new(fields, vec![col_a, col_b]).unwrap();

        let broadcasted = nd.broadcast_rows().unwrap();
        assert_eq!(broadcasted.num_rows(), 2);

        // After broadcasting, row 0 of column a should be repeated twice over t.
        let a0b = broadcasted.columns()[0].row(0).unwrap();
        assert_eq!(a0b.dimensions().shape(), vec![2, 3]);
        let a0_vals = a0b.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a0_vals.values(), &[1, 2, 3, 1, 2, 3]);

        // Column b row 0 already matches, should remain unchanged.
        let b0b = broadcasted.columns()[1].row(0).unwrap();
        assert_eq!(b0b.dimensions().shape(), vec![2, 3]);
        let b0_vals = b0b.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(b0_vals.values(), &[10, 20, 30, 40, 50, 60]);

        // Conversion to Arrow RecordBatch should preserve 2 rows.
        let rb = broadcasted.to_arrow_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.num_columns(), 2);
    }

    #[test]
    fn nd_record_batch_new_rejects_non_nd_field() {
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let col_a = NdArrowArrayColumn::from_rows(vec![a]).unwrap();

        // Not tagged as an ND extension field.
        let fields = vec![Field::new("a", DataType::Int32, true)];
        let err = NdRecordBatch::new(fields, vec![col_a]).unwrap_err();
        assert!(matches!(err, NdArrayError::InvalidNdBatch(_)));
    }

    #[test]
    fn nd_record_batch_new_rejects_row_count_mismatch() {
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let b = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let col_a = NdArrowArrayColumn::from_rows(vec![a.clone(), b.clone()]).unwrap();
        let col_b = NdArrowArrayColumn::from_rows(vec![a]).unwrap();

        let fields = vec![
            extension::nd_column_field("a", DataType::Int32, true).unwrap(),
            extension::nd_column_field("b", DataType::Int32, true).unwrap(),
        ];

        let err = NdRecordBatch::new(fields, vec![col_a, col_b]).unwrap_err();
        assert!(matches!(err, NdArrayError::InvalidNdBatch(_)));
    }

    #[test]
    fn nd_record_batch_new_rejects_storage_type_mismatch() {
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let col_a = NdArrowArrayColumn::from_rows(vec![a]).unwrap();

        // Field claims Float64, but column stores Int32.
        let fields = vec![extension::nd_column_field("a", DataType::Float64, true).unwrap()];
        let err = NdRecordBatch::new(fields, vec![col_a]).unwrap_err();
        assert!(matches!(err, NdArrayError::InvalidNdBatch(_)));
    }

    #[test]
    fn nd_record_batch_to_broadcasted_arrow_record_batch() {
        let a0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let b0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            Dimensions::new(vec![
                Dimension::try_new("t", 2).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let col_a = NdArrowArrayColumn::from_rows(vec![a0]).unwrap();
        let col_b = NdArrowArrayColumn::from_rows(vec![b0]).unwrap();
        let fields = vec![
            extension::nd_column_field("a", DataType::Int32, true).unwrap(),
            extension::nd_column_field("b", DataType::Int32, true).unwrap(),
        ];
        let nd = NdRecordBatch::new(fields, vec![col_a, col_b]).unwrap();

        let rb = nd.to_broadcasted_arrow_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 2);
    }

    #[test]
    fn nd_record_batch_from_named_columns_builds_schema() {
        let a = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let b = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let col_a = NdArrowArrayColumn::from_rows(vec![a]).unwrap();
        let col_b = NdArrowArrayColumn::from_rows(vec![b]).unwrap();

        let nd = NdRecordBatch::from_named_columns(vec![
            ("a".to_string(), col_a),
            ("b".to_string(), col_b),
        ])
        .unwrap();
        assert_eq!(nd.schema().fields().len(), 2);
        assert!(extension::is_nd_column_field(nd.schema().field(0)));
        assert!(extension::is_nd_column_field(nd.schema().field(1)));

        let rb = nd.to_arrow_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 2);
    }

    #[test]
    fn nd_record_batch_from_named_columns_rejects_row_mismatch() {
        let a0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let a1 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let col_a = NdArrowArrayColumn::from_rows(vec![a0.clone(), a1]).unwrap();
        let col_b = NdArrowArrayColumn::from_rows(vec![a0]).unwrap();

        let err = NdRecordBatch::from_named_columns(vec![
            ("a".to_string(), col_a),
            ("b".to_string(), col_b),
        ])
        .unwrap_err();
        assert!(matches!(err, NdArrayError::InvalidNdBatch(_)));
    }

    #[test]
    fn nd_record_batch_broadcast_row_to_record_batch() {
        let a0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let b0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            Dimensions::new(vec![
                Dimension::try_new("t", 2).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let col_a = NdArrowArrayColumn::from_rows(vec![a0]).unwrap();
        let col_b = NdArrowArrayColumn::from_rows(vec![b0]).unwrap();

        let nd = NdRecordBatch::from_named_columns(vec![
            ("a".to_string(), col_a),
            ("b".to_string(), col_b),
        ])
        .unwrap();

        let rb = nd.broadcast_row_to_record_batch(0).unwrap();
        assert_eq!(rb.num_columns(), 2);
        assert_eq!(rb.num_rows(), 6);

        let a_vals = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let b_vals = rb.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a_vals.values(), &[1, 2, 3, 1, 2, 3]);
        assert_eq!(b_vals.values(), &[10, 20, 30, 40, 50, 60]);
    }

    #[test]
    fn nd_record_batch_broadcast_rows_to_record_batches_per_row_results() {
        // Two rows: row 0 is broadcastable, row 1 is not.
        let a0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let a1 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Dimensions::new(vec![
                Dimension::try_new("t", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let b0 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            Dimensions::new(vec![
                Dimension::try_new("t", 2).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        // Incompatible with a1: x=2 cannot broadcast with x=3.
        let b1 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![7, 8, 9, 10])),
            Dimensions::new(vec![
                Dimension::try_new("t", 2).unwrap(),
                Dimension::try_new("x", 2).unwrap(),
            ]),
        )
        .unwrap();

        let col_a = NdArrowArrayColumn::from_rows(vec![a0, a1]).unwrap();
        let col_b = NdArrowArrayColumn::from_rows(vec![b0, b1]).unwrap();

        let nd = NdRecordBatch::from_named_columns(vec![
            ("a".to_string(), col_a),
            ("b".to_string(), col_b),
        ])
        .unwrap();

        let per_row = nd.broadcast_rows_to_record_batches();
        assert_eq!(per_row.len(), 2);
        assert!(per_row[0].is_ok());
        assert!(per_row[1].is_err());

        let rb0 = per_row[0].as_ref().unwrap();
        assert_eq!(rb0.num_rows(), 6);
        assert_eq!(rb0.num_columns(), 2);
    }

    #[test]
    fn nd_record_batch_broadcast_row_to_record_batch_scalar_only() {
        let a0 =
            NdArrowArray::new(Arc::new(Int32Array::from(vec![1])), Dimensions::Scalar).unwrap();
        let b0 =
            NdArrowArray::new(Arc::new(Int32Array::from(vec![2])), Dimensions::Scalar).unwrap();

        let col_a = NdArrowArrayColumn::from_rows(vec![a0]).unwrap();
        let col_b = NdArrowArrayColumn::from_rows(vec![b0]).unwrap();
        let nd = NdRecordBatch::from_named_columns(vec![
            ("a".to_string(), col_a),
            ("b".to_string(), col_b),
        ])
        .unwrap();

        let rb = nd.broadcast_row_to_record_batch(0).unwrap();
        assert_eq!(rb.num_columns(), 2);
        assert_eq!(rb.num_rows(), 1);

        let a_vals = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let b_vals = rb.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a_vals.values(), &[1]);
        assert_eq!(b_vals.values(), &[2]);
    }

    #[test]
    fn nd_record_batch_broadcast_row_to_record_batch_scalar_and_nd() {
        let scalar =
            NdArrowArray::new(Arc::new(Int32Array::from(vec![7])), Dimensions::Scalar).unwrap();
        let nd = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            Dimensions::new(vec![
                Dimension::try_new("t", 2).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let col_scalar = NdArrowArrayColumn::from_rows(vec![scalar]).unwrap();
        let col_nd = NdArrowArrayColumn::from_rows(vec![nd]).unwrap();
        let batch = NdRecordBatch::from_named_columns(vec![
            ("s".to_string(), col_scalar),
            ("v".to_string(), col_nd),
        ])
        .unwrap();

        let rb = batch.broadcast_row_to_record_batch(0).unwrap();
        assert_eq!(rb.num_columns(), 2);
        assert_eq!(rb.num_rows(), 6);

        let s_vals = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let v_vals = rb.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(s_vals.values(), &[7, 7, 7, 7, 7, 7]);
        assert_eq!(v_vals.values(), &[10, 20, 30, 40, 50, 60]);
    }
}
