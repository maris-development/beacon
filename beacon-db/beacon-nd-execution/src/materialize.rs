//! Broadcast nd arrays and nd batches onto their target grid as flat Arrow data.
//!
//! The nd types live in `beacon-nd-encoding`, which stays free of compute
//! kernels. These extension traits add the materialization on top of them.

use arrow::array::{ArrayRef, RecordBatchOptions, UInt64Array};
use arrow::record_batch::RecordBatch;
use beacon_nd_encoding::{Dimensions, NdArrowArray, NdRecordBatch};
use datafusion::error::{DataFusionError, Result};

use crate::broadcast::BroadcastMap;

/// Broadcast an [`NdArrowArray`] onto a target dimension grid.
pub trait NdArrowArrayExt {
    /// The broadcast of this array onto `target`.
    fn broadcast_map(&self, target: &Dimensions) -> Result<BroadcastMap>;

    /// Materialize as a flat array over `target` (C-order).
    fn materialize(&self, target: &Dimensions) -> Result<ArrayRef>;

    /// Materialize using a precomputed [`BroadcastMap`] (from
    /// [`broadcast_map`](Self::broadcast_map)).
    fn materialize_with_map(&self, map: &BroadcastMap) -> Result<ArrayRef>;

    /// Gather the values at `indices` (source offsets, for example from
    /// [`BroadcastMap::gather_indices_at`]).
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

/// Broadcast every column of an [`NdRecordBatch`] onto its target grid.
pub trait NdRecordBatchExt {
    /// Materialize into a flat Arrow [`RecordBatch`] by broadcasting each
    /// column onto the target grid (a single gather per column, or a zero-copy
    /// pass-through for a column already at full rank).
    fn materialize(&self) -> Result<RecordBatch>;

    /// Like [`materialize`](Self::materialize), but also returns how many columns
    /// required an actual broadcast gather versus passed through zero-copy (an
    /// identity broadcast, i.e. already at full rank). Used to report implicit
    /// broadcasts as plan metrics.
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
                // The broadcast shape drives the metric even under a selection:
                // a full-rank column is a pass-through, a lower-rank one a
                // broadcast — the selection only narrows which cells are gathered.
                if map.is_identity() {
                    passthroughs += 1;
                } else {
                    broadcasts += 1;
                }
                match self.selection() {
                    // Broadcast and selection fuse into one gather: source
                    // offsets for exactly the retained target cells.
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

    use arrow::array::{Array, AsArray, Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Schema};
    use beacon_nd_encoding::Dimension;
    use beacon_nd_encoding::encoding::{
        decode_nd_array, decode_nd_record_batch, encode_flat_batch_as_nd, nd_encoded_type,
    };

    use super::*;

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
    fn nulls_survive_the_broadcast_gather() {
        // Validity lives in the Arrow null buffer, so a null coordinate value
        // must replicate as null across every cell it broadcasts to.
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
        // The null time step contributes 3 null cells, one per lat.
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

    #[test]
    fn null_scalar_broadcasts_to_an_all_null_column() {
        let null_struct = arrow::array::new_null_array(&nd_encoded_type(&DataType::Float64), 1);
        let decoded = decode_nd_array(&null_struct, 0).unwrap();
        let out = decoded.materialize(&dims(&[("time", 3)])).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.null_count(), 3);
    }

    #[test]
    fn zero_column_batch_materializes_its_row_count() {
        let empty = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(42)),
        )
        .unwrap();
        let flat = decode_nd_record_batch(&empty).unwrap().materialize().unwrap();
        assert_eq!(flat.num_columns(), 0);
        assert_eq!(flat.num_rows(), 42);
    }

    #[test]
    fn flat_batch_round_trips_through_nd() {
        // Ragged path: encode → decode → materialize returns the flat batch.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float64, true),
        ]));
        let flat = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            ],
        )
        .unwrap();
        let encoded = encode_flat_batch_as_nd(&flat).unwrap();
        let decoded = decode_nd_record_batch(&encoded).unwrap();
        assert_eq!(decoded.materialize().unwrap(), flat);
    }
}
