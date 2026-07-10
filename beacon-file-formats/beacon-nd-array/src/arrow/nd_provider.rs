//! Bridge from a [`Dataset`] to the nd execution-plan spine.
//!
//! [`dataset_as_nd_stream`] chunks a regular dataset in C-order and yields one
//! un-broadcast [`NdRecordBatch`] per chunk. [`any_dataset_as_broadcast_stream`]
//! is the drop-in the file-format openers use: it broadcasts a regular dataset
//! through the nd spine ([`NdRecordBatch::materialize`], the operation
//! [`beacon_datafusion_ext::nd::exec::NdBroadcastExec`] performs) and falls back
//! to the v1 stream for ragged datasets (no rectangular grid).
//!
//! Predicate pushdown (chunk pruning) is intentionally not applied — the nd
//! spine has no filter node yet, and the scan reports filters as inexact so
//! DataFusion keeps a `FilterExec` above it. Filtering will return later.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
use datafusion::error::{DataFusionError, Result};
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

/// Broadcast a dataset to a flat `RecordBatch` stream through the nd spine.
/// Regular datasets stream as chunked [`NdRecordBatch`]es that are materialized
/// (broadcast); ragged datasets fall back to the v1 stream.
pub fn any_dataset_as_broadcast_stream(
    dataset: AnyDataset,
    batch_size: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    match dataset {
        AnyDataset::Regular(regular) => dataset_as_nd_stream(regular, batch_size)
            .map(|nd| nd.and_then(|batch| batch.materialize()))
            .boxed(),
        ragged => any_dataset_as_record_batch_stream(ragged, batch_size, None, None)
            .map_err(exec_err)
            .boxed(),
    }
}

/// Chunk a regular [`Dataset`] in C-order into a stream of un-broadcast
/// [`NdRecordBatch`]es — each variable kept on its own dimensions.
pub fn dataset_as_nd_stream(
    dataset: Dataset,
    batch_size: usize,
) -> BoxStream<'static, Result<NdRecordBatch>> {
    let (max_dims, max_shape, chunk_shape) = match extract_dataset_layout(&dataset) {
        Ok(layout) => layout,
        Err(e) => return futures::stream::once(async move { Err(exec_err(e)) }).boxed(),
    };

    // Honour a native chunk layout when present; otherwise cut the outer axis to
    // approach `batch_size`.
    let effective_chunk_shape = if chunk_shape != max_shape {
        chunk_shape
    } else {
        c_order_chunk_shape(&max_shape, batch_size)
    };

    let subsets = generate_chunk_subsets(&max_shape, &effective_chunk_shape);
    let arrays = Arc::new(dataset.arrays);
    let max_dims = Arc::new(max_dims);
    let schema = build_dataset_schema(&arrays);

    futures::stream::iter(subsets)
        .then(move |subset| {
            let arrays = arrays.clone();
            let max_dims = max_dims.clone();
            let schema = schema.clone();
            async move {
                let target = Dimensions::try_new(
                    max_dims
                        .iter()
                        .zip(subset.shape.iter())
                        .map(|(name, &size)| Dimension::new(name.as_str(), size))
                        .collect(),
                )?;

                let mut columns = Vec::with_capacity(arrays.len());
                for (name, array) in arrays.iter() {
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
                    columns.push(
                        NdArrowArray::try_new(values, dims)
                            .map_err(|e| exec_err(format!("nd column '{name}': {e}")))?,
                    );
                }

                NdRecordBatch::try_new(schema, columns, target)
            }
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::compute::concat_batches;
    use arrow::record_batch::RecordBatch;
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

    /// Broadcasting through the nd spine matches the v1 broadcast stream.
    #[tokio::test]
    async fn spine_matches_v1_broadcast() {
        for batch_size in [usize::MAX, 6, 3] {
            let ds = test_dataset().await;
            let schema = build_dataset_schema(&ds.arrays);

            let v1: Vec<RecordBatch> =
                dataset_as_record_batch_stream(ds.clone(), batch_size, None, None)
                    .try_collect()
                    .await
                    .unwrap();
            let expected = concat_batches(&schema, &v1).unwrap();

            let actual_batches: Vec<RecordBatch> =
                any_dataset_as_broadcast_stream(AnyDataset::Regular(ds), batch_size)
                    .try_collect()
                    .await
                    .unwrap();
            let actual = concat_batches(&schema, &actual_batches).unwrap();

            assert_eq!(actual, expected, "batch_size={batch_size}");
            assert_eq!(actual.num_rows(), 12);
        }
    }
}
