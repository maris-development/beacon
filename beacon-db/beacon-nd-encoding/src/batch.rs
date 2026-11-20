//! Grid-shaped record batches: columns with heterogeneous dimension subsets
//! over a shared target grid.

use arrow::array::{Array, UInt64Array};
use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_common::plan_err;

use crate::array::NdArrowArray;
use crate::dimensions::Dimensions;

/// A record batch whose columns are [`NdArrowArray`]s over a shared target
/// grid. Each column may live on a subset of the target's dimensions (a scalar
/// attribute, a 1-D coordinate, a full-rank data variable, …). Columns stay
/// un-broadcast: the reader of the batch broadcasts each one onto the full target
/// grid.
///
/// An optional [`selection`](Self::selection) restricts the batch to a subset of
/// the target cells (their row-major linear indices). It is how an
/// `NdFilterExec` records a predicate without
/// moving data: the columns are untouched, and materialization gathers only the
/// retained cells — the broadcast and the selection fuse into one gather per
/// column, so the filtered-out cross-product never exists.
#[derive(Debug, Clone)]
pub struct NdRecordBatch {
    schema: SchemaRef,
    columns: Vec<NdArrowArray>,
    target: Dimensions,
    /// Retained target-cell indices (row-major), or `None` for the full grid.
    selection: Option<UInt64Array>,
}

impl NdRecordBatch {
    pub fn try_new(
        schema: SchemaRef,
        columns: Vec<NdArrowArray>,
        target: Dimensions,
    ) -> Result<Self> {
        if schema.fields().len() != columns.len() {
            return plan_err!(
                "nd batch has {} columns but the schema declares {} fields",
                columns.len(),
                schema.fields().len()
            );
        }
        for (field, column) in schema.fields().iter().zip(columns.iter()) {
            if field.data_type() != column.data_type() {
                return plan_err!(
                    "nd column '{}' has type {} but the schema declares {}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                );
            }
            // Validate broadcast compatibility eagerly so materialization
            // cannot fail on shape errors.
            column.dims().check_broadcast_to(&target)?;
        }
        Ok(Self {
            schema,
            columns,
            target,
            selection: None,
        })
    }

    /// Attach (or clear) a selection restricting the batch to a subset of target
    /// cells, given as their row-major linear indices. Consumes and returns the
    /// batch. Indices are validated to fall within the target grid.
    pub fn with_selection(mut self, selection: Option<UInt64Array>) -> Result<Self> {
        if let Some(sel) = &selection {
            let n = self.target.num_elements() as u64;
            if sel.null_count() > 0 {
                return plan_err!("nd selection must not contain null indices");
            }
            if let Some(&max) = sel.values().iter().max()
                && max >= n
            {
                return plan_err!(
                    "nd selection index {max} is out of bounds for target grid of {n} cells"
                );
            }
        }
        self.selection = selection;
        Ok(self)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn columns(&self) -> &[NdArrowArray] {
        &self.columns
    }

    pub fn column(&self, index: usize) -> &NdArrowArray {
        &self.columns[index]
    }

    pub fn target(&self) -> &Dimensions {
        &self.target
    }

    /// The retained target-cell indices, or `None` when the full grid is kept.
    pub fn selection(&self) -> Option<&UInt64Array> {
        self.selection.as_ref()
    }

    /// Rows in the materialized batch: the selection size when filtered, else
    /// the full broadcast grid.
    pub fn num_rows(&self) -> usize {
        match &self.selection {
            Some(sel) => sel.len(),
            None => self.target.num_elements(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::dimensions::Dimension;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    fn test_batch() -> NdRecordBatch {
        // Grid (time=2, lat=3): time coord, lat coord, sst data.
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int32, true),
            Field::new("lat", DataType::Int32, true),
            Field::new("sst", DataType::Float64, true),
        ]));
        let time =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![7, 8])), dims(&[("time", 2)]))
                .unwrap();
        let lat = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            dims(&[("lat", 3)]),
        )
        .unwrap();
        let sst = NdArrowArray::try_new(
            Arc::new(Float64Array::from(vec![0.0, 0.1, 0.2, 1.0, 1.1, 1.2])),
            dims(&[("time", 2), ("lat", 3)]),
        )
        .unwrap();
        NdRecordBatch::try_new(
            schema,
            vec![time, lat, sst],
            dims(&[("time", 2), ("lat", 3)]),
        )
        .unwrap()
    }

    #[test]
    fn selection_sets_the_row_count() {
        // Keep target cells 1, 3, 5 of the (time=2, lat=3) grid.
        let batch = test_batch()
            .with_selection(Some(UInt64Array::from(vec![1u64, 3, 5])))
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(test_batch().num_rows(), 6);
    }

    #[test]
    fn out_of_bounds_selection_rejected() {
        // Grid has 6 cells; index 6 is out of range.
        let result = test_batch().with_selection(Some(UInt64Array::from(vec![0u64, 6])));
        assert!(result.is_err());
    }

    #[test]
    fn schema_mismatch_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Float64,
            true,
        )]));
        let time =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![7, 8])), dims(&[("time", 2)]))
                .unwrap();
        assert!(NdRecordBatch::try_new(schema, vec![time], dims(&[("time", 2)])).is_err());
    }

    #[test]
    fn column_count_mismatch_rejected() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int32, true),
            Field::new("lat", DataType::Int32, true),
        ]));
        let time =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![7, 8])), dims(&[("time", 2)]))
                .unwrap();
        // Two declared fields but only one column.
        assert!(NdRecordBatch::try_new(schema, vec![time], dims(&[("time", 2)])).is_err());
    }

    #[test]
    fn incompatible_column_dims_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "depth",
            DataType::Int32,
            true,
        )]));
        let depth = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![1, 2])),
            dims(&[("depth", 2)]),
        )
        .unwrap();
        assert!(NdRecordBatch::try_new(schema, vec![depth], dims(&[("time", 2)])).is_err());
    }
}
