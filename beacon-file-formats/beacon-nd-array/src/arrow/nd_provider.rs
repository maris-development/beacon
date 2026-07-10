//! Bridge from a [`Dataset`] to the nd execution-plan spine.
//!
//! [`DatasetNdProvider`] implements [`NdBatchProvider`] so a file format can
//! feed a dataset straight into an `NdSourceExec`: it chunks the dataset in
//! C-order and yields one [`NdRecordBatch`] per chunk with each variable left
//! *un-broadcast* on its own dimensions. The `NdBroadcastExec` above the source
//! is what later materializes the broadcast — this provider never expands the
//! cross-product.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::nd::exec::{
    NdBatchProvider, NdBroadcastExec, NdSourceExec, SendableNdBatchStream,
};
use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use crate::arrow::array::ndarray_to_arrow_array;
use crate::arrow::batch::{
    any_dataset_as_record_batch_stream, build_dataset_schema, c_order_chunk_shape,
    extract_dataset_layout, generate_array_subset_from_chunk, generate_chunk_subsets,
};
use crate::dataset::{AnyDataset, Dataset};

fn exec_err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

/// Broadcast a dataset to a flat `RecordBatch` stream through the nd
/// execution-plan spine.
///
/// A regular dataset is fed through [`DatasetNdProvider`] → [`NdSourceExec`] →
/// [`NdBroadcastExec`]: the source streams un-broadcast [`NdRecordBatch`]es and
/// the broadcast node materializes them. A ragged dataset has no rectangular
/// grid, so it stays on the v1 stream. This is the drop-in the file-format
/// openers use in place of the v1 broadcast engine.
///
/// Predicate pushdown (chunk pruning) is intentionally not applied here — the
/// nd spine has no filter node yet, and the scan reports filters as inexact so
/// DataFusion keeps a `FilterExec` above it. Filtering will return to the spine
/// later.
pub fn any_dataset_as_broadcast_stream(
    dataset: AnyDataset,
    batch_size: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    match dataset {
        AnyDataset::Regular(regular) => {
            let provider =
                Arc::new(DatasetNdProvider::new(regular, batch_size)) as Arc<dyn NdBatchProvider>;
            let source = Arc::new(NdSourceExec::new(provider)) as Arc<dyn ExecutionPlan>;
            // The nd nodes are pure data-flow here (no session state), so a
            // default context is sufficient.
            let result = NdBroadcastExec::try_new(source)
                .and_then(|broadcast| broadcast.execute(0, Arc::new(TaskContext::default())));
            match result {
                Ok(stream) => stream.boxed(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        }
        ragged => any_dataset_as_record_batch_stream(ragged, batch_size, None, None)
            .map_err(exec_err)
            .boxed(),
    }
}

/// An [`NdBatchProvider`] over a regular (non-ragged) [`Dataset`].
#[derive(Debug)]
pub struct DatasetNdProvider {
    dataset: Dataset,
    schema: SchemaRef,
    batch_size: usize,
}

impl DatasetNdProvider {
    /// `batch_size` is the target number of grid elements per chunk (the outer
    /// axis is cut to approach it), mirroring the v1 stream.
    pub fn new(dataset: Dataset, batch_size: usize) -> Self {
        let schema = build_dataset_schema(&dataset.arrays);
        Self {
            dataset,
            schema,
            batch_size,
        }
    }
}

impl NdBatchProvider for DatasetNdProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream> {
        let (max_dims, max_shape, chunk_shape) =
            extract_dataset_layout(&self.dataset).map_err(exec_err)?;

        // Honour a native chunk layout when present; otherwise cut the outer
        // axis to approach `batch_size`.
        let effective_chunk_shape = if chunk_shape != max_shape {
            chunk_shape
        } else {
            c_order_chunk_shape(&max_shape, self.batch_size)
        };

        let subsets = generate_chunk_subsets(&max_shape, &effective_chunk_shape);
        let arrays = Arc::new(self.dataset.arrays.clone());
        let max_dims = Arc::new(max_dims);
        let schema = self.schema.clone();

        let stream = futures::stream::iter(subsets).then(move |subset| {
            let arrays = arrays.clone();
            let max_dims = max_dims.clone();
            let schema = schema.clone();
            async move {
                // The chunk's target grid: the max dimensions cut to this
                // chunk's extent.
                let target = Dimensions::try_new(
                    max_dims
                        .iter()
                        .zip(subset.shape.iter())
                        .map(|(name, &size)| Dimension::new(name.as_str(), size))
                        .collect(),
                )?;

                let mut columns = Vec::with_capacity(arrays.len());
                for (name, array) in arrays.iter() {
                    // Slice each variable to the part of the chunk that lies on
                    // its own dimensions, then convert to a flat Arrow array.
                    let array_subset =
                        generate_array_subset_from_chunk(&subset, &max_dims, array.as_ref());
                    let sliced = array.subset(array_subset).await.map_err(exec_err)?;
                    let values = ndarray_to_arrow_array(sliced.as_ref())
                        .await
                        .map_err(exec_err)?;
                    let dims = Dimensions::try_new(
                        sliced
                            .dimensions()
                            .iter()
                            .zip(sliced.shape().iter())
                            .map(|(dim, &size)| Dimension::new(dim.as_str(), size))
                            .collect(),
                    )?;
                    columns.push(NdArrowArray::try_new(values, dims).map_err(|e| {
                        exec_err(format!("nd column '{name}': {e}"))
                    })?);
                }

                NdRecordBatch::try_new(schema, columns, target)
            }
        });

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::compute::concat_batches;
    use arrow::record_batch::RecordBatch;
    use beacon_datafusion_ext::nd::exec::{NdBroadcastExec, NdSourceExec};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::TryStreamExt;
    use indexmap::IndexMap;

    use super::*;
    use crate::arrow::batch::dataset_as_record_batch_stream;
    use crate::{NdArray, NdArrayD};

    async fn test_dataset() -> Dataset {
        let time = NdArray::<i64>::try_new_from_vec_in_mem(
            (0..4).map(|v| v * 100).collect(),
            vec![4],
            vec!["time".to_string()],
            None,
        )
        .unwrap();
        let lat = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![-30.0, 0.0, 30.0],
            vec![3],
            vec!["lat".to_string()],
            None,
        )
        .unwrap();
        let sst = NdArray::<f64>::try_new_from_vec_in_mem(
            (0..12).map(|v| v as f64).collect(),
            vec![4, 3],
            vec!["time".to_string(), "lat".to_string()],
            None,
        )
        .unwrap();

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("time".to_string(), Arc::new(time));
        arrays.insert("lat".to_string(), Arc::new(lat));
        arrays.insert("sst".to_string(), Arc::new(sst));
        Dataset::new("test".to_string(), arrays).await
    }

    /// The provider fed through NdSourceExec + NdBroadcastExec must produce the
    /// same flat table as the v1 broadcast stream.
    #[tokio::test]
    async fn provider_matches_v1_broadcast() {
        for batch_size in [usize::MAX, 6, 3] {
            let ds = test_dataset().await;
            let schema = build_dataset_schema(&ds.arrays);

            let v1: Vec<RecordBatch> =
                dataset_as_record_batch_stream(ds.clone(), batch_size, None, None)
                    .try_collect()
                    .await
                    .unwrap();
            let expected = concat_batches(&schema, &v1).unwrap();

            let provider = DatasetNdProvider::new(ds, batch_size);
            let source = Arc::new(NdSourceExec::new(Arc::new(provider)));
            let plan: Arc<dyn ExecutionPlan> =
                Arc::new(NdBroadcastExec::try_new(source).unwrap());
            let actual_batches: Vec<RecordBatch> = plan
                .execute(0, Arc::new(TaskContext::default()))
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let actual = concat_batches(&plan.schema(), &actual_batches).unwrap();

            assert_eq!(actual, expected, "batch_size={batch_size}");
            assert_eq!(actual.num_rows(), 12);
        }
    }
}
