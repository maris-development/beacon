//! N-dimensional Arrow arrays and dimension-aware physical operators.
//!
//! Beacon's nd formats (zarr, netcdf) are logically grids of variables over
//! shared named dimensions. Flattening a grid to a table broadcasts every
//! variable onto the full dimension cross-product. This module keeps columns
//! *un-broadcast* through the physical plan until a terminal node materializes
//! them:
//!
//! - [`NdArrowArray`]: a flat Arrow array plus named C-order [`Dimensions`].
//! - [`BroadcastMap`]: a broadcast expressed as stride arithmetic; no data
//!   moves.
//! - [`NdRecordBatch`]: columns (each on a subset of the dimensions) over a
//!   shared target grid.
//! - [`exec::NdSourceExec`]: streams nd batches from an [`exec::NdBatchProvider`].
//! - [`exec::NdBroadcastExec`]: the terminal node that materializes nd batches
//!   into flat `RecordBatch`es.
//!
//! Filtering and projection operators are layered on top of this spine later.

pub mod array;
pub mod batch;
pub mod broadcast;
pub mod dimensions;
pub mod exec;

pub use array::{ArraySubset, NdArrowArray};
pub use batch::NdRecordBatch;
pub use broadcast::{BroadcastMap, RunStructure};
pub use dimensions::{Dimension, Dimensions};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float64Array, Int32Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Schema};
    use datafusion::error::Result;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::TryStreamExt;

    use super::exec::{MemoryNdBatchProvider, NdBroadcastExec, NdSourceExec};
    use super::*;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    /// A (time=4, lat=3, lon=2) grid with coordinate variables and one data
    /// variable, split into two nd batches along `time`.
    fn test_source() -> (Arc<Schema>, Arc<NdSourceExec>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int32, true),
            Field::new("lat", DataType::Int32, true),
            Field::new("lon", DataType::Int32, true),
            Field::new("sst", DataType::Float64, true),
        ]));

        let lat = || {
            NdArrowArray::try_new(
                Arc::new(Int32Array::from(vec![-30, 0, 30])),
                dims(&[("lat", 3)]),
            )
            .unwrap()
        };
        let lon = || {
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![5, 15])), dims(&[("lon", 2)]))
                .unwrap()
        };

        let mut batches = Vec::new();
        for chunk in 0..2u32 {
            let t0 = chunk * 2;
            let time = NdArrowArray::try_new(
                Arc::new(Int32Array::from(vec![100 + t0 as i32, 101 + t0 as i32])),
                dims(&[("time", 2)]),
            )
            .unwrap();
            let sst_values: Vec<f64> = (0..12).map(|i| (t0 * 6 + i) as f64).collect();
            let sst = NdArrowArray::try_new(
                Arc::new(Float64Array::from(sst_values)),
                dims(&[("time", 2), ("lat", 3), ("lon", 2)]),
            )
            .unwrap();
            let batch = NdRecordBatch::try_new(
                schema.clone(),
                vec![time, lat(), lon(), sst],
                dims(&[("time", 2), ("lat", 3), ("lon", 2)]),
            )
            .unwrap();
            batches.push(batch);
        }

        let provider = MemoryNdBatchProvider::new(schema.clone(), batches);
        (schema, Arc::new(NdSourceExec::new(Arc::new(provider))))
    }

    async fn run(plan: Arc<dyn ExecutionPlan>) -> Result<arrow::record_batch::RecordBatch> {
        let schema = plan.schema();
        let batches: Vec<_> = plan
            .execute(0, Arc::new(TaskContext::default()))?
            .try_collect()
            .await?;
        Ok(concat_batches(&schema, &batches)?)
    }

    #[tokio::test]
    async fn source_alone_materializes_full_grid() {
        // NdSourceExec's own `execute` materializes (no broadcast node needed).
        let (_, source) = test_source();
        let batch = run(source).await.unwrap();
        assert_eq!(batch.num_rows(), 24);
        assert_eq!(batch.column(0).as_primitive::<Int32Type>().value(0), 100);
        assert_eq!(batch.column(1).as_primitive::<Int32Type>().value(0), -30);
        assert_eq!(batch.column(2).as_primitive::<Int32Type>().value(0), 5);
    }

    #[tokio::test]
    async fn source_then_broadcast_materializes_full_grid() {
        let (_, source) = test_source();
        let plan = Arc::new(NdBroadcastExec::try_new(source).unwrap());
        let batch = run(plan).await.unwrap();

        // 4 time x 3 lat x 2 lon = 24 rows, C-order (time outer, lon inner).
        assert_eq!(batch.num_rows(), 24);

        // time repeats over the 6 (lat,lon) cells of each step.
        assert_eq!(
            batch.column(0).as_primitive::<Int32Type>().values(),
            &[
                100, 100, 100, 100, 100, 100, 101, 101, 101, 101, 101, 101, 102, 102, 102, 102,
                102, 102, 103, 103, 103, 103, 103, 103
            ]
        );
        // lat repeats over lon, tiles over time.
        assert_eq!(
            &batch.column(1).as_primitive::<Int32Type>().values()[..6],
            &[-30, -30, 0, 0, 30, 30]
        );
        // lon tiles over everything.
        assert_eq!(
            &batch.column(2).as_primitive::<Int32Type>().values()[..6],
            &[5, 15, 5, 15, 5, 15]
        );
        // sst is full-rank: passes through unbroadcast, values 0..24.
        assert_eq!(
            batch.column(3).as_primitive::<Float64Type>().values(),
            &(0..24).map(|v| v as f64).collect::<Vec<_>>()[..]
        );
    }

    #[tokio::test]
    async fn broadcast_requires_nd_input() {
        // A non-nd child is rejected at construction.
        let (_, source) = test_source();
        let broadcast = Arc::new(NdBroadcastExec::try_new(source).unwrap());
        // Wrapping a broadcast (which is not nd-aware) must fail.
        assert!(NdBroadcastExec::try_new(broadcast).is_err());
    }
}
