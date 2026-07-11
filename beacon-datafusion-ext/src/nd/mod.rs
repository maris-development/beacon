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
pub mod encoding;
pub mod exec;

pub use array::NdArrowArray;
pub use batch::NdRecordBatch;
pub use broadcast::BroadcastMap;
pub use dimensions::{Dimension, Dimensions};
pub use encoding::{
    decode_nd_record_batch, encode_flat_batch_as_nd, encode_nd_record_batch, encoded_schema,
    is_nd_encoded, logical_schema, nd_encoded_field, nd_encoded_type,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float64Array, Int32Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{Float64Type, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::error::Result;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::TryStreamExt;

    use super::encoding::encode_nd_record_batch;
    use super::exec::{NdBroadcastExec, NdSourceExec};
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
    /// variable, encoded into two nd-encoded `RecordBatch`es (split along
    /// `time`) and served from an in-memory `DataSourceExec` — the shape a file
    /// opener produces.
    fn test_source() -> Arc<NdSourceExec> {
        let logical = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("time", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("lat", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("lon", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("sst", arrow::datatypes::DataType::Float64, true),
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

        let mut encoded = Vec::new();
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
            let nd = NdRecordBatch::try_new(
                logical.clone(),
                vec![time, lat(), lon(), sst],
                dims(&[("time", 2), ("lat", 3), ("lon", 2)]),
            )
            .unwrap();
            encoded.push(encode_nd_record_batch(&nd).unwrap());
        }

        let encoded_schema = encoded[0].schema();
        // One partition, each nd chunk its own single-row encoded batch.
        let source = MemorySourceConfig::try_new_exec(
            &[encoded.into_iter().map(|b| vec![b]).flatten().collect()],
            encoded_schema,
            None,
        )
        .unwrap();
        Arc::new(NdSourceExec::try_new(source).unwrap())
    }

    async fn run(plan: Arc<dyn ExecutionPlan>) -> Result<RecordBatch> {
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
        let batch = run(test_source()).await.unwrap();
        assert_eq!(batch.num_rows(), 24);
        assert_eq!(batch.column(0).as_primitive::<Int32Type>().value(0), 100);
        assert_eq!(batch.column(1).as_primitive::<Int32Type>().value(0), -30);
        assert_eq!(batch.column(2).as_primitive::<Int32Type>().value(0), 5);
    }

    #[tokio::test]
    async fn source_then_broadcast_materializes_full_grid() {
        let plan = Arc::new(NdBroadcastExec::try_new(test_source()).unwrap());
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
    async fn nodes_report_metrics() {
        let source = test_source();
        let broadcast = Arc::new(NdBroadcastExec::try_new(source.clone()).unwrap());

        // Drain the plan so the streams run to completion and finalize metrics.
        let out = run(broadcast.clone() as Arc<dyn ExecutionPlan>).await.unwrap();
        assert_eq!(out.num_rows(), 24);

        // Broadcast reports the flattened output rows.
        let broadcast_metrics = broadcast.metrics().unwrap();
        assert_eq!(broadcast_metrics.output_rows(), Some(24));

        // Source reports the grid rows it decoded (12 + 12) and the number of
        // nd batches.
        let source_metrics = source.metrics().unwrap();
        assert_eq!(source_metrics.output_rows(), Some(24));
        assert_eq!(
            source_metrics
                .sum_by_name("nd_batches")
                .map(|v| v.as_usize()),
            Some(2)
        );
    }

    #[tokio::test]
    async fn broadcast_requires_nd_input() {
        // A non-nd child is rejected at construction.
        let broadcast = Arc::new(NdBroadcastExec::try_new(test_source()).unwrap());
        // Wrapping a broadcast (which is not nd-aware) must fail.
        assert!(NdBroadcastExec::try_new(broadcast).is_err());
    }
}
