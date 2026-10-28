//! Broadcast of nd arrays and nd batches to flat Arrow data.
//!
//! The nd types live in `beacon-nd-encoding`, which only holds the data model and its
//! encoding. The broadcast is engine work, so it lives here, as extension traits.

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use datafusion::error::{DataFusionError, Result};

use super::broadcast::BroadcastMap;
use super::{Dimensions, NdArrowArray, NdRecordBatch};

/// Broadcast of one [`NdArrowArray`].
pub trait NdArrowArrayExt {
    /// Broadcast map from the dimensions of this array onto `target`.
    fn broadcast_map(&self, target: &Dimensions) -> Result<BroadcastMap>;

    /// This array broadcast onto `target`, as a single `take` (or a zero-copy
    /// clone when the broadcast is the identity).
    fn materialize(&self, target: &Dimensions) -> Result<ArrayRef>;

    /// Materialize with a precomputed [`BroadcastMap`]. Zero-copy when the map is
    /// the identity; otherwise a single gather.
    fn materialize_with_map(&self, map: &BroadcastMap) -> Result<ArrayRef>;

    /// Gather `indices` from the flat values with a single Arrow `take`.
    fn take_indices(&self, indices: &UInt64Array) -> Result<ArrayRef>;
}

impl NdArrowArrayExt for NdArrowArray {
    fn broadcast_map(&self, target: &Dimensions) -> Result<BroadcastMap> {
        BroadcastMap::try_new(self.dims(), target)
    }

    fn materialize(&self, target: &Dimensions) -> Result<ArrayRef> {
        let map = self.broadcast_map(target)?;
        self.materialize_with_map(&map)
    }

    fn materialize_with_map(&self, map: &BroadcastMap) -> Result<ArrayRef> {
        if map.is_identity() {
            return Ok(self.values().clone());
        }
        self.take_indices(&map.gather_indices())
    }

    fn take_indices(&self, indices: &UInt64Array) -> Result<ArrayRef> {
        arrow::compute::take(self.values().as_ref(), indices, None)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Broadcast of one [`NdRecordBatch`] to a flat Arrow batch.
pub trait NdRecordBatchExt {
    /// Broadcast each column onto the target grid (a single gather per column, or a
    /// zero-copy pass-through for a column already at full rank).
    fn materialize(&self) -> Result<RecordBatch>;

    /// Like [`materialize`](Self::materialize), and also how many columns needed a
    /// gather and how many passed through zero-copy.
    fn materialize_with_stats(&self) -> Result<(RecordBatch, usize, usize)>;
}

impl NdRecordBatchExt for NdRecordBatch {
    fn materialize(&self) -> Result<RecordBatch> {
        Ok(self.materialize_with_stats()?.0)
    }

    fn materialize_with_stats(&self) -> Result<(RecordBatch, usize, usize)> {
        let mut broadcasts = 0usize;
        let mut passthroughs = 0usize;
        let arrays: Vec<ArrayRef> = self
            .columns()
            .iter()
            .map(|column| {
                let map = column.broadcast_map(self.target())?;
                // The broadcast shape drives the metric even under a selection.
                if map.is_identity() {
                    passthroughs += 1;
                } else {
                    broadcasts += 1;
                }
                match self.selection() {
                    // Broadcast and selection fuse into one gather.
                    Some(sel) => column.take_indices(&map.gather_indices_at(sel)),
                    None => column.materialize_with_map(&map),
                }
            })
            .collect::<Result<_>>()?;

        let options = RecordBatchOptions::new().with_row_count(Some(self.num_rows()));
        let batch = RecordBatch::try_new_with_options(self.schema().clone(), arrays, &options)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok((batch, broadcasts, passthroughs))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, AsArray, Float64Array, Int32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Schema};

    use super::*;
    use crate::nd::Dimension;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    fn nd(values: Vec<i32>, spec: &[(&str, usize)]) -> NdArrowArray {
        NdArrowArray::try_new(Arc::new(Int32Array::from(values)), dims(spec)).unwrap()
    }

    fn ints(array: &ArrayRef) -> Vec<i32> {
        array.as_primitive::<Int32Type>().values().to_vec()
    }

    fn test_batch() -> NdRecordBatch {
        // Grid (time=2, lat=3): time coord, lat coord, sst data.
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int32, true),
            Field::new("lat", DataType::Int32, true),
            Field::new("sst", DataType::Float64, true),
        ]));
        let time = nd(vec![7, 8], &[("time", 2)]);
        let lat = nd(vec![10, 20, 30], &[("lat", 3)]);
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
    fn materialize_broadcast() {
        let lat = nd(vec![10, 20, 30], &[("lat", 3)]);
        let target = dims(&[("time", 2), ("lat", 3)]);
        let out = lat.materialize(&target).unwrap();
        assert_eq!(ints(&out), vec![10, 20, 30, 10, 20, 30]);
    }

    #[test]
    fn materialize_identity_zero_copy() {
        let a = nd((0..6).collect(), &[("time", 2), ("lon", 3)]);
        let target = dims(&[("time", 2), ("lon", 3)]);
        let out = a.materialize(&target).unwrap();
        assert_eq!(ints(&out), (0..6).collect::<Vec<_>>());
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
        let out = batch.materialize().unwrap();
        assert_eq!(out.num_rows(), 3);
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
    fn nulls_survive_the_broadcast_gather() {
        // A null coordinate value replicates as null across every cell it broadcasts to.
        let schema = Arc::new(Schema::new(vec![Field::new("time", DataType::Int32, true)]));
        let time = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![Some(7), None])),
            dims(&[("time", 2)]),
        )
        .unwrap();
        let batch =
            NdRecordBatch::try_new(schema, vec![time], dims(&[("time", 2), ("lat", 3)])).unwrap();
        let out = batch.materialize().unwrap();
        assert_eq!(out.num_rows(), 6);
        assert_eq!(out.column(0).null_count(), 3);
        assert!(out.column(0).is_null(3));
    }

    #[test]
    fn materialize_with_stats_separates_gathers_from_passthroughs() {
        // time and lat need a gather onto the grid; sst is already full-rank.
        let (_, broadcasts, passthroughs) = test_batch().materialize_with_stats().unwrap();
        assert_eq!(broadcasts, 2);
        assert_eq!(passthroughs, 1);
    }

    #[test]
    fn an_empty_axis_materializes_a_zero_row_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("time", DataType::Int32, true)]));
        let time = NdArrowArray::try_new(
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            dims(&[("time", 0)]),
        )
        .unwrap();
        let batch =
            NdRecordBatch::try_new(schema, vec![time], dims(&[("time", 0), ("lat", 3)])).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.materialize().unwrap().num_rows(), 0);
    }
}
