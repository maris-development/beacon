//! Streaming a regular (non-ragged) [`Dataset`] as Arrow [`RecordBatch`]es.
//!
//! The dataset is chunked along its outermost axis; each chunk is an
//! independent read unit (see [`read_regular_unit`]) so it can be driven
//! sequentially ([`dataset_as_record_batch_stream`]) or concurrently (by the
//! parallel producer in [`super::parallel`]).

use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, stream::BoxStream};
use indexmap::IndexMap;

use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    arrow::{
        pushdown::{compute_chunk_mask, is_pushdown_candidate, mask_is_all_false, mask_pushdown},
        pushdown_filter::PushdownFilter,
    },
    dataset::Dataset,
};

use super::{array::ndarray_to_arrow_array, metrics::DatasetReadMetrics};

/// Stream [`RecordBatch`]es from a regular dataset, one per chunk, in order.
pub fn dataset_as_record_batch_stream(
    dataset: Dataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
    metrics: Option<DatasetReadMetrics>,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    futures::stream::once(async move { plan_regular_stream(dataset, batch_size, predicate).await })
        .map(move |result| match result {
            Ok(plan) => {
                let RegularStreamPlan {
                    arrays,
                    subsets,
                    schema,
                    max_dims,
                    dim_masks,
                } = plan;
                let metrics = metrics.clone();
                futures::stream::iter(subsets)
                    .then(move |subset| {
                        let arrays = arrays.clone();
                        let schema = schema.clone();
                        let max_dims = max_dims.clone();
                        let dim_masks = dim_masks.clone();
                        let metrics = metrics.clone();
                        async move {
                            read_regular_unit(
                                &arrays, subset, schema, &max_dims, &dim_masks, &metrics,
                            )
                            .await
                        }
                    })
                    .filter_map(|opt| async move { opt })
                    .boxed()
            }
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        })
        .flatten()
        .boxed()
}

/// Everything needed to read the chunks of a regular dataset, computed once up
/// front (arrays, chunk subsets, schema, dimension order, and pushdown masks).
pub(crate) struct RegularStreamPlan {
    pub(crate) arrays: Arc<IndexMap<String, Arc<dyn NdArrayD>>>,
    pub(crate) subsets: Vec<ArraySubset>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) max_dims: Vec<String>,
    pub(crate) dim_masks: Arc<Vec<(String, Vec<bool>)>>,
}

/// Compute the read plan for a regular dataset: derive the layout/schema, pick
/// the effective chunk shape, enumerate chunk subsets, and resolve pushdown
/// masks.
pub(crate) async fn plan_regular_stream(
    dataset: Dataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
) -> anyhow::Result<RegularStreamPlan> {
    let (max_dims, max_shape, chunk_shape) = extract_dataset_layout(&dataset)?;
    let schema = build_dataset_schema(&dataset.arrays);

    let effective_chunk_shape = if chunk_shape != max_shape {
        chunk_shape
    } else {
        c_order_chunk_shape(&max_shape, batch_size)
    };

    let subsets = generate_chunk_subsets(&max_shape, &effective_chunk_shape);
    let arrays = Arc::new(dataset.arrays);
    let dim_masks = Arc::new(compute_predicate_masks(&arrays, predicate).await?);

    Ok(RegularStreamPlan {
        arrays,
        subsets,
        schema,
        max_dims,
        dim_masks,
    })
}

/// Read one chunk subset of a regular dataset, applying pruning and updating
/// `metrics`. Returns `None` when the chunk is pruned or empty. Each call is
/// independent, so this can be driven sequentially or concurrently.
pub(crate) async fn read_regular_unit(
    arrays: &IndexMap<String, Arc<dyn NdArrayD>>,
    subset: ArraySubset,
    schema: Arc<Schema>,
    max_dims: &[String],
    dim_masks: &[(String, Vec<bool>)],
    metrics: &Option<DatasetReadMetrics>,
) -> Option<anyhow::Result<RecordBatch>> {
    let chunk_rows: usize = subset.shape.iter().product();
    match read_chunk(arrays, subset, schema, max_dims, dim_masks).await {
        Ok(None) => {
            if let Some(m) = metrics {
                m.batches_pruned.add(1);
                m.rows_pruned.add(chunk_rows);
            }
            None
        }
        Ok(Some(batch)) if batch.num_rows() > 0 => {
            if let Some(m) = metrics {
                m.output_rows.add(batch.num_rows());
                m.output_batches.add(1);
            }
            Some(Ok(batch))
        }
        Ok(_) => None,
        Err(e) => Some(Err(e)),
    }
}

fn extract_dataset_layout(
    dataset: &Dataset,
) -> anyhow::Result<(Vec<String>, Vec<usize>, Vec<usize>)> {
    for (array_name, array) in &dataset.arrays {
        if !array
            .dimensions()
            .iter()
            .all(|d| dataset.dimensions.contains_key(d))
        {
            let array_dimensions = array.dimensions();
            let dataset_dimensions = dataset.dimensions.keys().cloned().collect::<Vec<String>>();
            anyhow::bail!(
                "Array '{}' has dimensions: {:?} that are not part of the dataset dimensions: {:?}",
                array_name,
                array_dimensions,
                dataset_dimensions
            );
        }
    }

    let (_, max_array) = dataset
        .arrays
        .iter()
        .max_by_key(|(_, array)| array.shape().len())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Dataset has no arrays, so we can not determine the shape and chunk shape to use for iterating over the dataset."
            )
        })?;

    Ok((
        max_array.dimensions(),
        max_array.shape(),
        max_array.chunk_shape(),
    ))
}

fn build_dataset_schema(arrays: &IndexMap<String, Arc<dyn NdArrayD>>) -> Arc<Schema> {
    Arc::new(Schema::new(
        arrays
            .iter()
            .map(|(name, array)| {
                let arrow_dt: DataType = array.datatype().into();
                Field::new(name.clone(), arrow_dt, true)
            })
            .collect::<Vec<_>>(),
    ))
}

async fn compute_predicate_masks(
    arrays: &IndexMap<String, Arc<dyn NdArrayD>>,
    predicate: Option<PushdownFilter>,
) -> anyhow::Result<Vec<(String, Vec<bool>)>> {
    let mut dim_masks: Vec<(String, Vec<bool>)> = Vec::new();
    if let Some(pred) = predicate {
        let ranges = pred.ranges();
        for (name, array) in arrays {
            if let Some(range) = ranges.get(name)
                && is_pushdown_candidate(array)
            {
                let dim = array.dimensions()[0].clone();
                let mask = mask_pushdown(array.clone(), range).await?;
                dim_masks.push((dim, mask));
            }
        }
    }
    Ok(dim_masks)
}

async fn read_chunk(
    arrays: &IndexMap<String, Arc<dyn NdArrayD>>,
    subset: ArraySubset,
    schema: Arc<Schema>,
    max_dims: &[String],
    dim_masks: &[(String, Vec<bool>)],
) -> anyhow::Result<Option<RecordBatch>> {
    if !dim_masks.is_empty() {
        let mask = compute_chunk_mask(dim_masks, max_dims, &subset.start, &subset.shape);
        if mask_is_all_false(&mask) {
            return Ok(None);
        }
    }

    let mut arrow_arrays: Vec<ArrayRef> = Vec::with_capacity(arrays.len());
    for (name, array) in arrays {
        let array_subset = generate_array_subset_from_chunk(&subset, max_dims, array.as_ref());
        let sliced = array
            .subset(array_subset)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to subset array '{}': {}", name, e))?;
        let broadcasted = sliced
            .broadcast(subset.shape.clone(), max_dims.to_vec())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to broadcast array '{}': {}", name, e))?;
        arrow_arrays.push(
            ndarray_to_arrow_array(broadcasted.as_ref())
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to convert array '{}' to Arrow: {}", name, e)
                })?,
        );
    }
    Ok(Some(RecordBatch::try_new(schema, arrow_arrays)?))
}

/// For a given chunk subset (expressed in max_dims space), compute the corresponding
/// subset for an individual array that may have fewer dimensions.
fn generate_array_subset_from_chunk(
    subset: &ArraySubset,
    chunk_dimensions: &[String],
    array: &dyn NdArrayD,
) -> ArraySubset {
    let array_dims = array.dimensions();
    let mut start = Vec::with_capacity(array_dims.len());
    let mut shape = Vec::with_capacity(array_dims.len());

    for dim in &array_dims {
        if let Some(idx) = chunk_dimensions.iter().position(|d| d == dim) {
            start.push(subset.start[idx]);
            shape.push(subset.shape[idx]);
        }
    }

    ArraySubset::new(start, shape)
}

/// Compute a C-memory-ordered chunk shape targeting approximately `batch_size` elements.
/// Inner dimensions are kept whole; only the outermost axis is cut.
fn c_order_chunk_shape(shape: &[usize], batch_size: usize) -> Vec<usize> {
    if shape.is_empty() {
        return vec![];
    }
    let inner: usize = shape[1..].iter().product::<usize>().max(1);
    let rows = (batch_size / inner).max(1).min(shape[0].max(1));
    let mut chunk = shape.to_vec();
    chunk[0] = rows;
    chunk
}

/// Generate all chunk subsets for iterating over `shape` in chunks of `chunk_shape`.
/// Boundary chunks are automatically shrunk to fit the remaining elements.
fn generate_chunk_subsets(shape: &[usize], chunk_shape: &[usize]) -> Vec<ArraySubset> {
    if shape.is_empty() {
        return vec![ArraySubset::new(vec![], vec![])];
    }

    let chunk_counts: Vec<usize> = shape
        .iter()
        .zip(chunk_shape.iter())
        .map(|(&axis_len, &axis_chunk)| {
            if axis_len == 0 {
                0
            } else {
                axis_len.div_ceil(axis_chunk.max(1))
            }
        })
        .collect();

    if chunk_counts.contains(&0) {
        return vec![];
    }

    let total_chunks: usize = chunk_counts.iter().product();
    let mut subsets = Vec::with_capacity(total_chunks);

    for linear_idx in 0..total_chunks {
        let mut rem = linear_idx;
        let mut chunk_index = vec![0usize; shape.len()];
        for axis in (0..shape.len()).rev() {
            chunk_index[axis] = rem % chunk_counts[axis];
            rem /= chunk_counts[axis];
        }

        let start: Vec<usize> = chunk_index
            .iter()
            .zip(chunk_shape.iter())
            .map(|(&ci, &cs)| ci * cs.max(1))
            .collect();

        let sub_shape: Vec<usize> = shape
            .iter()
            .zip(start.iter())
            .zip(chunk_shape.iter())
            .map(|((&axis_len, &axis_start), &axis_chunk)| {
                axis_chunk.max(1).min(axis_len - axis_start)
            })
            .collect();

        subsets.push(ArraySubset::new(start, sub_shape));
    }

    subsets
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NdArray;
    use arrow::array::{Array, Float64Array, Int32Array};
    use futures::TryStreamExt;

    async fn make_dataset(arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> Dataset {
        let map: IndexMap<String, Arc<dyn NdArrayD>> = arrays
            .into_iter()
            .map(|(name, arr)| (name.to_string(), arr))
            .collect();
        Dataset::new("test".to_string(), map).await
    }

    #[tokio::test]
    async fn test_single_array_no_chunk() {
        let nd = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![1, 2, 3, 4, 5, 6],
            vec![2, 3],
            vec!["x".to_string(), "y".to_string()],
            None,
        )
        .unwrap();
        let ds = make_dataset(vec![("values", Arc::new(nd))]).await;
        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds, usize::MAX, None, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 6);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values().as_ref(), &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_broadcast_different_dimensions() {
        // 2D array (x=2, y=3) and 1D array (y=3) that should broadcast
        let nd_2d = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            vec![2, 3],
            vec!["x".to_string(), "y".to_string()],
            None,
        )
        .unwrap();
        let nd_1d = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0, 30.0],
            vec![3],
            vec!["y".to_string()],
            None,
        )
        .unwrap();

        let ds = make_dataset(vec![("data", Arc::new(nd_2d)), ("scale", Arc::new(nd_1d))]).await;

        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds, usize::MAX, None, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 6);

        // The 1D scale array [10, 20, 30] should be broadcast to [10, 20, 30, 10, 20, 30]
        let scale_col = batch
            .column_by_name("scale")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            scale_col.values().as_ref(),
            &[10.0, 20.0, 30.0, 10.0, 20.0, 30.0]
        );
    }

    #[tokio::test]
    async fn test_dimension_mismatch_error() {
        let nd1 = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![1, 2, 3],
            vec![3],
            vec!["x".to_string()],
            None,
        )
        .unwrap();

        // Manually create a dataset and tamper with its dimensions
        let mut ds = make_dataset(vec![("a", Arc::new(nd1))]).await;
        ds.dimensions.clear();

        let results: Vec<anyhow::Result<RecordBatch>> =
            dataset_as_record_batch_stream(ds, usize::MAX, None, None)
                .collect()
                .await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[tokio::test]
    async fn test_empty_dataset_error() {
        let ds = make_dataset(vec![]).await;
        let results: Vec<anyhow::Result<RecordBatch>> =
            dataset_as_record_batch_stream(ds, usize::MAX, None, None)
                .collect()
                .await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[tokio::test]
    async fn test_fill_value_produces_nulls() {
        let nd = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![1, -9999, 3],
            vec![3],
            vec!["x".to_string()],
            Some(-9999),
        )
        .unwrap();
        let ds = make_dataset(vec![("vals", Arc::new(nd))]).await;
        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds, usize::MAX, None, None)
            .try_collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(col.is_valid(0));
        assert!(col.is_null(1));
        assert!(col.is_valid(2));
    }

    #[test]
    fn test_generate_chunk_subsets_basic() {
        let subsets = generate_chunk_subsets(&[10, 5], &[3, 5]);
        // ceil(10/3) = 4 chunks on first axis, 1 on second
        assert_eq!(subsets.len(), 4);
        assert_eq!(subsets[0].start, vec![0, 0]);
        assert_eq!(subsets[0].shape, vec![3, 5]);
        assert_eq!(subsets[3].start, vec![9, 0]);
        assert_eq!(subsets[3].shape, vec![1, 5]); // remainder
    }

    #[test]
    fn test_generate_chunk_subsets_empty() {
        let subsets = generate_chunk_subsets(&[], &[]);
        assert_eq!(subsets.len(), 1);
        assert!(subsets[0].start.is_empty());
    }

    #[test]
    fn test_generate_array_subset_from_chunk() {
        // Chunk subset in (x, y, z) space
        let chunk_subset = ArraySubset::new(vec![5, 0, 10], vec![3, 4, 5]);
        let chunk_dims = vec!["x".to_string(), "y".to_string(), "z".to_string()];

        // Array only has (y, z) dimensions
        let nd = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![0.0; 20],
            vec![4, 5],
            vec!["y".to_string(), "z".to_string()],
            None,
        )
        .unwrap();

        let result = generate_array_subset_from_chunk(&chunk_subset, &chunk_dims, &nd);
        assert_eq!(result.start, vec![0, 10]);
        assert_eq!(result.shape, vec![4, 5]);
    }

    #[test]
    fn test_c_order_chunk_shape_2d_cuts_outer_axis() {
        // shape [10, 5], batch_size 15 → inner=5, rows=3 → [3, 5]
        assert_eq!(c_order_chunk_shape(&[10, 5], 15), vec![3, 5]);
    }

    #[test]
    fn test_c_order_chunk_shape_batch_larger_than_array() {
        // batch_size bigger than total → rows capped at shape[0]
        assert_eq!(c_order_chunk_shape(&[4, 3], 1000), vec![4, 3]);
    }

    #[test]
    fn test_c_order_chunk_shape_usize_max() {
        // usize::MAX must not overflow and must cap at shape[0]
        assert_eq!(c_order_chunk_shape(&[6, 3], usize::MAX), vec![6, 3]);
    }

    #[test]
    fn test_c_order_chunk_shape_1d() {
        // 1-D: inner=1, rows=batch_size capped at shape[0]
        assert_eq!(c_order_chunk_shape(&[20], 7), vec![7]);
        assert_eq!(c_order_chunk_shape(&[20], 100), vec![20]);
    }

    #[test]
    fn test_c_order_chunk_shape_batch_smaller_than_row() {
        // batch_size < one inner row → rows=1 (minimum)
        assert_eq!(c_order_chunk_shape(&[10, 100], 50), vec![1, 100]);
    }

    #[test]
    fn test_c_order_chunk_shape_empty() {
        assert_eq!(c_order_chunk_shape(&[], 100), Vec::<usize>::new());
    }

    #[tokio::test]
    async fn test_chunked_stream_splits_into_multiple_batches() {
        // shape [6, 1]: inner=1, batch_size=2 → chunk=[2,1] → 3 batches
        let nd = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![1, 2, 3, 4, 5, 6],
            vec![6, 1],
            vec!["time".to_string(), "x".to_string()],
            None,
        )
        .unwrap();
        let ds = make_dataset(vec![("vals", Arc::new(nd))]).await;
        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds, 2, None, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
        assert_eq!(batches[2].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_chunked_stream_remainder_batch() {
        // shape [7, 1]: batch_size=3 → chunks [3,1],[3,1],[1,1] → 3 batches, last has 1 row
        let nd = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![10, 20, 30, 40, 50, 60, 70],
            vec![7, 1],
            vec!["t".to_string(), "x".to_string()],
            None,
        )
        .unwrap();
        let ds = make_dataset(vec![("v", Arc::new(nd))]).await;
        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds, 3, None, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[2].num_rows(), 1);
        let col = batches[2]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values().as_ref(), &[70]);
    }

    #[tokio::test]
    async fn test_chunked_stream_broadcast_per_chunk() {
        // 2D [4,3] and 1D [3] broadcast; batch_size=6 → chunk=[2,3] → 2 batches of 6 rows
        let nd_2d = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            vec![4, 3],
            vec!["time".to_string(), "x".to_string()],
            None,
        )
        .unwrap();
        let nd_1d = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![10, 20, 30],
            vec![3],
            vec!["x".to_string()],
            None,
        )
        .unwrap();
        let ds = make_dataset(vec![("data", Arc::new(nd_2d)), ("scale", Arc::new(nd_1d))]).await;
        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds, 6, None, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 6);
        assert_eq!(batches[1].num_rows(), 6);
        // First chunk: scale broadcast over time rows 0-1 → [10,20,30,10,20,30]
        let scale0 = batches[0]
            .column_by_name("scale")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(scale0.values().as_ref(), &[10, 20, 30, 10, 20, 30]);
        // Second chunk: same pattern for time rows 2-3
        let scale1 = batches[1]
            .column_by_name("scale")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(scale1.values().as_ref(), &[10, 20, 30, 10, 20, 30]);
    }
}
