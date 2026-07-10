//! Grid-shaped record batches: columns with heterogeneous dimension subsets
//! over a shared target grid.

use arrow::array::{ArrayRef, RecordBatchOptions};
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
#[derive(Debug, Clone)]
pub struct NdRecordBatch {
    schema: SchemaRef,
    columns: Vec<NdArrowArray>,
    target: Dimensions,
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
        })
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

    /// Rows in the materialized (fully broadcast) grid.
    pub fn num_rows(&self) -> usize {
        self.target.num_elements()
    }

    /// Materialize into a flat Arrow [`RecordBatch`] by broadcasting each
    /// column onto the target grid (a single gather per column, or a zero-copy
    /// pass-through for a column already at full rank).
    pub fn materialize(&self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|column| column.materialize(&self.target, None))
            .collect::<Result<_>>()?;

        let options = RecordBatchOptions::new().with_row_count(Some(self.num_rows()));
        RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float64Array, Int32Array};
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
