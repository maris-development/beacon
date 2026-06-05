use std::{collections::HashMap, ops::Range, sync::Arc};

use indexmap::IndexMap;

use arrow::{
    array::{ArrayRef, RecordBatchOptions, new_null_array},
    compute::concat,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, stream::BoxStream};

use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    arrow::{
        pushdown::{
            compute_chunk_mask, is_pushdown_candidate, is_pushdown_candidate_ragged,
            mask_is_all_false, mask_pushdown,
        },
        pushdown_filter::PushdownFilter,
    },
    dataset::{
        Dataset,
        any::AnyDataset,
        ragged::{RaggedArray, RaggedDataset},
    },
};

use super::{array::ndarray_to_arrow_array, metrics::DatasetReadMetrics};

pub fn any_dataset_as_row_size(
    dataset: AnyDataset,
) -> anyhow::Result<BoxStream<'static, anyhow::Result<RecordBatch>>> {
    match dataset {
        AnyDataset::Regular(dataset) => {
            // Find max array and use its shape to determine the number of rows using the product.
            let max_array = dataset
                .arrays
                .values()
                .max_by_key(|array| array.shape().len())
                .ok_or_else(|| anyhow::anyhow!("Dataset contains no arrays"))?;

            let shape = max_array.shape();
            if shape.is_empty() {
                Ok(futures::stream::once(async {
                    let schema = Arc::new(Schema::empty());
                    let record_batch_options = RecordBatchOptions::new().with_row_count(Some(1));
                    RecordBatch::try_new_with_options(schema, vec![], &record_batch_options)
                        .map_err(|e| e.into())
                })
                .boxed())
            } else {
                Ok(futures::stream::once(async move {
                    let schema = Arc::new(Schema::empty());
                    let record_batch_options =
                        RecordBatchOptions::new().with_row_count(Some(shape.iter().product()));
                    RecordBatch::try_new_with_options(schema, vec![], &record_batch_options)
                        .map_err(|e| e.into())
                })
                .boxed())
            }
        }
        AnyDataset::Ragged { ragged, .. } => {
            // The ragged dataset row count is the size of the largest obs variable.
            let max_obs_rows = ragged
                .observation_dimensions()
                .filter_map(|dim| ragged.variables.get(dim))
                .filter_map(|var| var.array().shape().first().cloned())
                .max()
                .unwrap_or(0);

            Ok(futures::stream::once(async move {
                let schema = Arc::new(Schema::empty());
                let record_batch_options =
                    RecordBatchOptions::new().with_row_count(Some(max_obs_rows));
                RecordBatch::try_new_with_options(schema, vec![], &record_batch_options)
                    .map_err(|e| e.into())
            })
            .boxed())
        }
    }
}

pub fn any_dataset_as_record_batch_stream(
    dataset: AnyDataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
    metrics: Option<DatasetReadMetrics>,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    match dataset {
        AnyDataset::Regular(ds) => {
            dataset_as_record_batch_stream(ds, batch_size, predicate, metrics)
        }
        AnyDataset::Ragged { ragged, .. } => {
            ragged_dataset_as_record_batch_stream(ragged, batch_size, predicate, metrics)
        }
    }
}

/// Stream [`RecordBatch`]es from a ragged dataset, grouping multiple
/// casts into each batch until `batch_size` observation rows are
/// reached. Casts are never split — a single cast that exceeds
/// `batch_size` on its own is still emitted as one batch.
fn ragged_dataset_as_record_batch_stream(
    ragged: RaggedDataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
    metrics: Option<DatasetReadMetrics>,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    let schema = ragged_record_batch_schema(&ragged);
    let obs_dims: std::collections::HashSet<String> =
        ragged.observation_dimensions().map(String::from).collect();
    let instance_dim = ragged.instance_dimension().to_string();

    futures::stream::once(async move {
        let offsets = ragged.cumulative_offsets().await?.clone();
        let n = ragged.len();

        let mut mask = vec![true; n];
        for (name, array) in &ragged.variables {
            if let RaggedArray::InstanceVariable(array) = array
                && is_pushdown_candidate_ragged(array, &instance_dim)
                && let Some(value_range) = predicate.as_ref().and_then(|f| f.ranges().get(name))
            {
                let array_mask = mask_pushdown(array.clone(), value_range).await?;
                assert_eq!(
                    array_mask.len(),
                    n,
                    "Pushdown mask length must match number of casts in the ragged dataset"
                );
                for (i, keep) in array_mask.iter().enumerate() {
                    if !keep {
                        mask[i] = false;
                    }
                }
            }
        }

        // Determine which instance indices pass the instance mask.
        let passing_indices: Vec<usize> = mask
            .iter()
            .enumerate()
            .filter_map(|(i, &keep)| if keep { Some(i) } else { None })
            .collect();

        // Track pruned casts and their observation row count.
        if let Some(m) = &metrics {
            let excluded = n - passing_indices.len();
            if excluded > 0 {
                m.batches_pruned.add(excluded);
                // Sum max-obs-dim rows for each excluded cast.
                let pruned_rows: usize = offsets
                    .iter()
                    .filter(|(dim, _)| obs_dims.contains(dim.as_str()))
                    .map(|(_, cum)| {
                        mask.iter()
                            .enumerate()
                            .filter(|(_, keep)| !**keep)
                            .map(|(i, _)| cum[i + 1] - cum[i])
                            .sum::<usize>()
                    })
                    .max()
                    .unwrap_or(0);
                m.rows_pruned.add(pruned_rows);
            }
        }

        // Replan batches using only passing instances.
        // Build filtered offsets for batch planning.
        let filtered_offsets: HashMap<String, Vec<usize>> = offsets
            .iter()
            .filter(|(dim, _)| obs_dims.contains(dim.as_str()))
            .map(|(dim, cum)| {
                let mut new_cum = Vec::with_capacity(passing_indices.len() + 1);
                new_cum.push(0usize);
                let mut running = 0usize;
                for &idx in &passing_indices {
                    running += cum[idx + 1] - cum[idx];
                    new_cum.push(running);
                }
                (dim.clone(), new_cum)
            })
            .collect();

        let n_filtered = passing_indices.len();
        let ranges = plan_ragged_batches(&filtered_offsets, &obs_dims, n_filtered, batch_size);
        Ok((
            ragged,
            schema,
            obs_dims,
            offsets,
            passing_indices,
            ranges,
        ))
    })
    .map(|init| match init {
        Ok((ragged, schema, obs_dims, offsets, passing_indices, ranges)) => {
            futures::stream::iter(ranges)
                .then(move |range| {
                    let ragged = ragged.clone();
                    let schema = schema.clone();
                    let obs_dims = obs_dims.clone();
                    let offsets = offsets.clone();
                    let passing_indices = passing_indices.clone();
                    async move {
                        // Map filtered range back to original indices for reading.
                        let batch_indices: Vec<usize> =
                            passing_indices[range.start..range.end].to_vec();

                        // Read each cast and merge (can't use get_casts_range since indices may not be contiguous).
                        let cast_data = if let (Some(&first), Some(&last)) =
                            (batch_indices.first(), batch_indices.last())
                        {
                            // If indices are contiguous, use range read.
                            if last - first + 1 == batch_indices.len() {
                                ragged.get_casts_range(first, last + 1).await?
                            } else {
                                // Non-contiguous: read the bounding range (conservative).
                                ragged.get_casts_range(first, last + 1).await?
                            }
                        } else {
                            anyhow::bail!("Empty batch range");
                        };

                        // Build the record batch using original offsets for the bounding range.
                        let first = *batch_indices.first().unwrap();
                        let last = *batch_indices.last().unwrap();
                        let bounding_range = first..last + 1;
                        let batch = ragged_batch_to_record_batch(
                            &cast_data,
                            &schema,
                            &obs_dims,
                            &offsets,
                            &bounding_range,
                        )
                        .await?;

                        // Output rows/batches are tracked by DataFusion's FileStream.
                        Ok(batch)
                    }
                })
                .boxed()
        }
        Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
    })
    .flatten()
    .boxed()
}

/// Build a unified Arrow [`Schema`] for all variables that appear in
/// any cast of a ragged dataset. This includes instance variables,
/// observation variables (all obs-dim groups), variable attributes,
/// and global attributes. Row-size variables are excluded. Fields are
/// sorted alphabetically.
fn ragged_record_batch_schema(ragged: &RaggedDataset) -> Arc<Schema> {
    let mut fields: Vec<Field> = Vec::new();

    for (name, var) in &ragged.variables {
        let dt: DataType = var.array().datatype().into();
        fields.push(Field::new(name.clone(), dt, true));
    }

    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Arc::new(Schema::new(fields))
}

/// Greedily group consecutive casts into batch ranges such that no
/// batch exceeds `batch_size` observation rows. A single cast that
/// exceeds `batch_size` on its own is still emitted as one batch.
///
/// The effective row count for a group is the **max across obs_dims**
/// of the total observation rows contributed by the casts in the group.
fn plan_ragged_batches(
    offsets: &HashMap<String, Vec<usize>>,
    obs_dims: &std::collections::HashSet<String>,
    n_instances: usize,
    batch_size: usize,
) -> Vec<Range<usize>> {
    if n_instances == 0 {
        return vec![];
    }

    let mut ranges: Vec<Range<usize>> = Vec::new();
    let mut batch_start = 0usize;

    for i in 1..=n_instances {
        // Compute the max row count across all obs dims for [batch_start..i).
        let current_rows: usize = offsets
            .iter()
            .filter(|(dim, _)| obs_dims.contains(dim.as_str()))
            .map(|(_, cum)| cum[i] - cum[batch_start])
            .max()
            .unwrap_or(0);

        if current_rows > batch_size && i > batch_start + 1 {
            // Adding cast `i-1` would exceed the limit — close the previous batch.
            ranges.push(batch_start..i - 1);
            batch_start = i - 1;
        }
    }

    // Emit the final batch.
    if batch_start < n_instances {
        ranges.push(batch_start..n_instances);
    }

    ranges
}

/// Convert a multi-cast [`Dataset`] (from [`RaggedDataset::get_casts_range`])
/// into a [`RecordBatch`] aligned to `schema`.
///
/// Instance variables (shape `[N, ...]` where N = number of casts in the
/// range) are run-length expanded: element `i` is repeated for each
/// observation row belonging to cast `i`. Observation variables are
/// already contiguous across all casts. Attributes are repeated to
/// fill all rows.
async fn ragged_batch_to_record_batch(
    cast_data: &Dataset,
    schema: &Arc<Schema>,
    obs_dims: &std::collections::HashSet<String>,
    offsets: &HashMap<String, Vec<usize>>,
    range: &Range<usize>,
) -> anyhow::Result<RecordBatch> {
    // Target row count = max total obs rows across all obs dims for this range.
    let target_rows: usize = offsets
        .iter()
        .filter(|(dim, _)| obs_dims.contains(dim.as_str()))
        .map(|(_, cum)| cum[range.end] - cum[range.start])
        .max()
        .unwrap_or(0);

    // Per-cast row sizes for the "primary" obs dim (the one with the most rows).
    // Used for run-length expansion of instance variables.
    let primary_obs_dim = offsets
        .iter()
        .filter(|(dim, _)| obs_dims.contains(dim.as_str()))
        .max_by_key(|(_, cum)| cum[range.end] - cum[range.start])
        .map(|(dim, _)| dim.as_str());

    let cast_row_sizes: Vec<usize> = if let Some(dim) = primary_obs_dim {
        let cum = &offsets[dim];
        (range.start..range.end)
            .map(|i| cum[i + 1] - cum[i])
            .collect()
    } else {
        vec![0; range.end - range.start]
    };

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col = match cast_data.get_array(field.name()) {
            Some(array) => {
                let arrow_arr = ndarray_to_arrow_array(array.as_ref()).await?;
                let arr_len = arrow_arr.len();
                let is_obs = array
                    .dimensions()
                    .first()
                    .is_some_and(|d| obs_dims.contains(d));

                if arr_len == target_rows {
                    arrow_arr
                } else if target_rows == 0 {
                    new_null_array(field.data_type(), 0)
                } else if is_obs {
                    // Observation variable on a shorter obs dimension → null-pad.
                    let pad_len = target_rows - arr_len;
                    let null_tail = new_null_array(field.data_type(), pad_len);
                    concat(&[arrow_arr.as_ref(), null_tail.as_ref()])?
                } else if arr_len == range.end - range.start {
                    // Instance variable — run-length expand per cast row sizes.
                    run_length_expand(&arrow_arr, &cast_row_sizes, target_rows)?
                } else {
                    // Attribute (dimensionless / scalar) — repeat to target row count.
                    let refs: Vec<&dyn arrow::array::Array> =
                        std::iter::repeat_n(arrow_arr.as_ref(), target_rows).collect();
                    concat(&refs)?
                }
            }
            None => new_null_array(field.data_type(), target_rows),
        };
        columns.push(col);
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Expand an array of N elements by repeating element `i` for
/// `row_sizes[i]` times, producing an array of `target_rows` length.
fn run_length_expand(
    array: &ArrayRef,
    row_sizes: &[usize],
    target_rows: usize,
) -> Result<ArrayRef, arrow::error::ArrowError> {
    // Build a take-indices array: [0,0,..,0, 1,1,..,1, 2, ...]
    let indices: Vec<u32> = row_sizes
        .iter()
        .enumerate()
        .flat_map(|(i, &count)| std::iter::repeat_n(i as u32, count))
        .collect();

    debug_assert_eq!(indices.len(), target_rows);

    let indices_array = arrow::array::UInt32Array::from(indices);
    arrow::compute::take(array.as_ref(), &indices_array, None)
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
            if let Some(range) = ranges.get(name) {
                if is_pushdown_candidate(array) {
                    let dim = array.dimensions()[0].clone();
                    let mask = mask_pushdown(array.clone(), range).await?;
                    dim_masks.push((dim, mask));
                }
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

pub fn dataset_as_record_batch_stream(
    dataset: Dataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
    metrics: Option<DatasetReadMetrics>,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    let (max_dims, max_shape, chunk_shape) = match extract_dataset_layout(&dataset) {
        Ok(layout) => layout,
        Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
    };

    let schema = build_dataset_schema(&dataset.arrays);

    let effective_chunk_shape = if chunk_shape != max_shape {
        chunk_shape
    } else {
        c_order_chunk_shape(&max_shape, batch_size)
    };

    let subsets = generate_chunk_subsets(&max_shape, &effective_chunk_shape);
    let arrays = Arc::new(dataset.arrays);

    futures::stream::once(async move {
        let dim_masks = Arc::new(compute_predicate_masks(&arrays, predicate).await?);
        Ok((arrays, subsets, schema, max_dims, dim_masks))
    })
    .map(move |result| {
        // Create locals from the captured `metrics` field so they can be moved
        // into the inner closures (you cannot move a captured FnMut field directly).
        let metrics_for_then = metrics.clone();
        match result {
            Ok((arrays, subsets, schema, max_dims, dim_masks)) => futures::stream::iter(subsets)
                .then(move |subset| {
                    let arrays = arrays.clone();
                    let schema = schema.clone();
                    let max_dims = max_dims.clone();
                    let dim_masks = dim_masks.clone();
                    let metrics = metrics_for_then.clone();
                    let chunk_rows: usize = subset.shape.iter().product();
                    async move {
                        let result =
                            read_chunk(&arrays, subset, schema, &max_dims, &dim_masks).await;
                        if let Ok(None) = &result {
                            if let Some(m) = &metrics {
                                m.batches_pruned.add(1);
                                m.rows_pruned.add(chunk_rows);
                            }
                        }
                        result
                    }
                })
                .filter_map(move |result| async move {
                    // Output rows/batches are tracked by DataFusion's FileStream.
                    match result {
                        Ok(Some(batch)) if batch.num_rows() > 0 => Some(Ok(batch)),
                        Ok(_) => None,
                        Err(e) => Some(Err(e)),
                    }
                })
                .boxed(),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        }
    })
    .flatten()
    .boxed()
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
    use indexmap::IndexMap;

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

    // ── any_dataset_as_record_batch_stream ───────────────────────────

    use crate::dataset::AnyDataset;
    use arrow::array::StringArray;

    /// Build a minimal CF contiguous-ragged-array dataset with 3 casts.
    ///
    /// Layout (same as dataset::tests::make_ragged_dataset):
    ///   - instance dim `"casts"` (size 3)
    ///   - obs dim `"z_obs"` (size 6, row sizes = [2, 1, 3])
    ///   - `station_id`  : instance var, f64  [100, 200, 300]
    ///   - `z_row_size`  : row-size var, i32  [2, 1, 3]
    ///   - `z_row_size.sample_dimension` : attr "z_obs"
    ///   - `depth`       : obs var, f64       [10, 20, 30, 40, 50, 60]
    ///   - `temperature` : obs var, f64       [1, 2, 3, 4, 5, 6]
    ///   - `depth.units` : var attr, String   ["m"]
    ///   - `temperature.units` : var attr     ["°C"]
    ///   - `Conventions` : global attr        ["CF-1.6"]
    async fn make_ragged() -> Dataset {
        let station_id = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![100.0, 200.0, 300.0],
            vec![3],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let z_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![2, 1, 3],
            vec![3],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let sample_dim = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["z_obs".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let depth = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            vec![6],
            vec!["z_obs".into()],
            None,
        )
        .unwrap();
        let temperature = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            vec![6],
            vec!["z_obs".into()],
            None,
        )
        .unwrap();
        let depth_units = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["m".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let temp_units = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["°C".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let conventions = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["CF-1.6".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        make_dataset(vec![
            ("Conventions", Arc::new(conventions)),
            ("station_id", Arc::new(station_id)),
            ("z_row_size", Arc::new(z_row_size)),
            ("z_row_size.sample_dimension", Arc::new(sample_dim)),
            ("depth", Arc::new(depth)),
            ("depth.units", Arc::new(depth_units)),
            ("temperature", Arc::new(temperature)),
            ("temperature.units", Arc::new(temp_units)),
        ])
        .await
    }

    #[tokio::test]
    async fn test_any_dataset_regular_delegates() {
        let nd =
            NdArray::<i32>::try_new_from_vec_in_mem(vec![1, 2, 3], vec![3], vec!["x".into()], None)
                .unwrap();
        let ds = make_dataset(vec![("vals", Arc::new(nd))]).await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let batches: Vec<RecordBatch> =
            any_dataset_as_record_batch_stream(any, usize::MAX, None, None)
                .try_collect()
                .await
                .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values().as_ref(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn test_any_dataset_ragged_single_obs_dim() {
        let ds = make_ragged().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any, 1, None, None)
            .try_collect()
            .await
            .unwrap();

        // One batch per cast.
        assert_eq!(batches.len(), 3);
        // Row counts match row sizes [2, 1, 3].
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        assert_eq!(batches[2].num_rows(), 3);

        // Obs variable `depth` present in first cast.
        let depth_col = batches[0]
            .column_by_name("depth")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(depth_col.values().as_ref(), &[10.0, 20.0]);
    }

    #[tokio::test]
    async fn test_any_dataset_ragged_schema_excludes_row_size() {
        let ds = make_ragged().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any, 1, None, None)
            .try_collect()
            .await
            .unwrap();

        let schema = batches[0].schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(!field_names.contains(&"z_row_size"));
        assert!(!field_names.contains(&"z_row_size.sample_dimension"));
    }

    #[tokio::test]
    async fn test_any_dataset_ragged_instance_vars_repeated() {
        let ds = make_ragged().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any, 1, None, None)
            .try_collect()
            .await
            .unwrap();

        // Cast 0: station_id=100, obs_len=2 → [100, 100]
        let sid = batches[0]
            .column_by_name("station_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sid.values().as_ref(), &[100.0, 100.0]);

        // Cast 2: station_id=300, obs_len=3 → [300, 300, 300]
        let sid2 = batches[2]
            .column_by_name("station_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sid2.values().as_ref(), &[300.0, 300.0, 300.0]);
    }

    #[tokio::test]
    async fn test_any_dataset_ragged_attrs_repeated() {
        let ds = make_ragged().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any, 1, None, None)
            .try_collect()
            .await
            .unwrap();

        // Cast 0 has obs_len=2. Global attr `Conventions` should be repeated.
        let conv = batches[0]
            .column_by_name("Conventions")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(conv.len(), 2);
        assert_eq!(conv.value(0), "CF-1.6");
        assert_eq!(conv.value(1), "CF-1.6");

        // Variable attr `depth.units` should be repeated.
        let du = batches[0]
            .column_by_name("depth.units")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(du.len(), 2);
        assert_eq!(du.value(0), "m");
        assert_eq!(du.value(1), "m");
    }

    #[tokio::test]
    async fn test_any_dataset_ragged_multi_obs_dim_null_pad() {
        // Two obs dims: z_obs (row sizes [2,1]) and t_obs (row sizes [3,2]).
        // Cast 0: z_obs=2, t_obs=3 → target=3, z_obs vars null-padded.
        // Cast 1: z_obs=1, t_obs=2 → target=2, z_obs vars null-padded.
        let station_id = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![100.0, 200.0],
            vec![2],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let z_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![2, 1],
            vec![2],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let z_sample_dim = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["z_obs".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let t_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![3, 2],
            vec![2],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let t_sample_dim = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["t_obs".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let depth = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0, 30.0],
            vec![3],
            vec!["z_obs".into()],
            None,
        )
        .unwrap();
        let time = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0, 4.0, 5.0],
            vec![5],
            vec!["t_obs".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset(vec![
            ("station_id", Arc::new(station_id)),
            ("z_row_size", Arc::new(z_row_size)),
            ("z_row_size.sample_dimension", Arc::new(z_sample_dim)),
            ("t_row_size", Arc::new(t_row_size)),
            ("t_row_size.sample_dimension", Arc::new(t_sample_dim)),
            ("depth", Arc::new(depth)),
            ("time", Arc::new(time)),
        ])
        .await;

        let any = AnyDataset::try_from_dataset(ds).await.unwrap();
        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any, 1, None, None)
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 2);

        // Cast 0: z_obs=2, t_obs=3 → target=3.
        assert_eq!(batches[0].num_rows(), 3);
        let depth_col = batches[0]
            .column_by_name("depth")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // depth has 2 real values, then 1 null.
        assert_eq!(depth_col.value(0), 10.0);
        assert_eq!(depth_col.value(1), 20.0);
        assert!(depth_col.is_null(2));

        let time_col = batches[0]
            .column_by_name("time")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(time_col.values().as_ref(), &[1.0, 2.0, 3.0]);

        // Cast 1: z_obs=1, t_obs=2 → target=2.
        assert_eq!(batches[1].num_rows(), 2);
        let depth_col1 = batches[1]
            .column_by_name("depth")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(depth_col1.value(0), 30.0);
        assert!(depth_col1.is_null(1));
    }

    #[tokio::test]
    async fn test_any_dataset_ragged_empty_cast() {
        // 2 casts, first has 0 observations, second has 2.
        let station_id = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![100.0, 200.0],
            vec![2],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let z_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![0, 2],
            vec![2],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let sample_dim = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["z_obs".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let depth = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0],
            vec![2],
            vec!["z_obs".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset(vec![
            ("station_id", Arc::new(station_id)),
            ("z_row_size", Arc::new(z_row_size)),
            ("z_row_size.sample_dimension", Arc::new(sample_dim)),
            ("depth", Arc::new(depth)),
        ])
        .await;

        let any = AnyDataset::try_from_dataset(ds).await.unwrap();
        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any, 1, None, None)
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 2);
        // First cast: 0 observations → 0-row batch.
        assert_eq!(batches[0].num_rows(), 0);
        // Second cast: 2 observations.
        assert_eq!(batches[1].num_rows(), 2);
    }
}
