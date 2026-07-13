//! Grid-shaped record batches: columns with heterogeneous dimension subsets
//! over a shared target grid.

use arrow::array::{Array, ArrayRef, RecordBatchOptions, UInt64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};

use super::array::NdArrowArray;
use super::dimensions::Dimensions;

/// A record batch whose columns are [`NdArrowArray`]s over a shared target
/// grid. Each column may live on a subset of the target's dimensions (a scalar
/// attribute, a 1-D coordinate, a full-rank data variable, …). Columns stay
/// un-broadcast until [`NdRecordBatch::materialize`], which broadcasts each one
/// onto the full target grid.
///
/// An optional [`selection`](Self::selection) restricts the batch to a subset of
/// the target cells (their row-major linear indices). It is how an
/// [`NdFilterExec`](crate::nd::exec::NdFilterExec) records a predicate without
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
            column.broadcast_map(&target)?;
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
            if let Some(&max) = sel.values().iter().max() {
                if max >= n {
                    return plan_err!(
                        "nd selection index {max} is out of bounds for target grid of {n} cells"
                    );
                }
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

    /// Materialize into a flat Arrow [`RecordBatch`] by broadcasting each
    /// column onto the target grid (a single gather per column, or a zero-copy
    /// pass-through for a column already at full rank).
    pub fn materialize(&self) -> Result<RecordBatch> {
        Ok(self.materialize_with_stats()?.0)
    }

    /// Like [`materialize`](Self::materialize), but also returns how many columns
    /// required an actual broadcast gather versus passed through zero-copy (an
    /// identity broadcast, i.e. already at full rank). Used to report implicit
    /// broadcasts as plan metrics.
    pub fn materialize_with_stats(&self) -> Result<(RecordBatch, usize, usize)> {
        let mut broadcasts = 0usize;
        let mut passthroughs = 0usize;
        let arrays: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|column| {
                let map = column.broadcast_map(&self.target)?;
                // The broadcast shape drives the metric even under a selection:
                // a full-rank column is a pass-through, a lower-rank one a
                // broadcast — the selection only narrows which cells are gathered.
                if map.is_identity() {
                    passthroughs += 1;
                } else {
                    broadcasts += 1;
                }
                match &self.selection {
                    // Broadcast and selection fuse into one gather: source
                    // offsets for exactly the retained target cells.
                    Some(sel) => column.take_indices(&map.gather_indices_at(sel)),
                    None => column.materialize_with_map(&map),
                }
            })
            .collect::<Result<_>>()?;

        let options = RecordBatchOptions::new().with_row_count(Some(self.num_rows()));
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok((batch, broadcasts, passthroughs))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float64Array, Int32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Schema};

    use super::*;
    use crate::nd::dimensions::Dimension;

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
    fn materialize_full_grid() {
        let batch = test_batch().materialize().unwrap();
        assert_eq!(batch.num_rows(), 6);
        // time repeats across lat; lat tiles across time; sst is full-rank.
        assert_eq!(
            batch.column(0).as_primitive::<Int32Type>().values(),
            &[7, 7, 7, 8, 8, 8]
        );
        assert_eq!(
            batch.column(1).as_primitive::<Int32Type>().values(),
            &[10, 20, 30, 10, 20, 30]
        );
        assert_eq!(
            batch.column(2).as_primitive::<Float64Type>().values(),
            &[0.0, 0.1, 0.2, 1.0, 1.1, 1.2]
        );
    }

    #[test]
    fn materialize_with_selection_gathers_retained_cells() {
        // Keep target cells 1, 3, 5 of the (time=2, lat=3) grid.
        let batch = test_batch()
            .with_selection(Some(UInt64Array::from(vec![1u64, 3, 5])))
            .unwrap();
        assert_eq!(batch.num_rows(), 3);

        let out = batch.materialize().unwrap();
        assert_eq!(out.num_rows(), 3);
        // Full grid: time=[7,7,7,8,8,8], lat=[10,20,30,10,20,30], sst=[..].
        // Cells 1,3,5 → time=[7,8,8], lat=[20,10,30], sst=[0.1,1.0,1.2].
        assert_eq!(
            out.column(0).as_primitive::<Int32Type>().values(),
            &[7, 8, 8]
        );
        assert_eq!(
            out.column(1).as_primitive::<Int32Type>().values(),
            &[20, 10, 30]
        );
        assert_eq!(
            out.column(2).as_primitive::<Float64Type>().values(),
            &[0.1, 1.0, 1.2]
        );
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
    fn incompatible_column_dims_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "depth",
            DataType::Int32,
            true,
        )]));
        let depth =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![1, 2])), dims(&[("depth", 2)]))
                .unwrap();
        assert!(NdRecordBatch::try_new(schema, vec![depth], dims(&[("time", 2)])).is_err());
    }
}
