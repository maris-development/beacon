use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, stream::BoxStream};

use crate::{NdArrayD, array::subset::ArraySubset, dataset::Dataset};

use super::array::ndarray_to_arrow_array;

pub fn dataset_as_record_batch_stream(
    dataset: Dataset,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    let dimensions = dataset.dimensions;

    // Verify that all arrays have all their dimensions being part of the dataset dimensions.
    for (array_name, array) in &dataset.arrays {
        if !array
            .dimensions()
            .iter()
            .all(|d| dimensions.contains_key(d))
        {
            let array_dimensions = array.dimensions();
            let dataset_dimensions = dimensions.keys().cloned().collect::<Vec<String>>();
            let array_name = array_name.clone();
            return futures::stream::once(async move {
                Err(anyhow::anyhow!(
                    "Array '{}' has dimensions: {:?} that are not part of the dataset dimensions: {:?}",
                    array_name,
                    array_dimensions,
                    dataset_dimensions
                ))
            })
            .boxed();
        }
    }

    // Find the array with the largest number of dimensions to determine the broadcast target.
    let Some((_, max_array)) = dataset
        .arrays
        .iter()
        .max_by_key(|(_, array)| array.shape().len())
    else {
        return futures::stream::once(async move {
            Err(anyhow::anyhow!(
                "Dataset has no arrays, so we can not determine the shape and chunk shape to use for iterating over the dataset."
            ))
        })
        .boxed();
    };

    let max_dims = max_array.dimensions();
    let max_shape = max_array.shape();
    let chunk_shape = max_array.chunk_shape();

    // Build schema from dataset arrays.
    let schema = Arc::new(Schema::new(
        dataset
            .arrays
            .iter()
            .map(|(name, array)| {
                let arrow_dt: DataType = array.datatype().into();
                Field::new(name.clone(), arrow_dt, true)
            })
            .collect::<Vec<_>>(),
    ));

    if chunk_shape == max_shape {
        // No chunking needed: broadcast all arrays and return a single RecordBatch.
        let arrays = dataset.arrays;
        let schema = schema.clone();
        return futures::stream::once(async move {
            let mut arrow_arrays: Vec<ArrayRef> = Vec::with_capacity(arrays.len());
            for (_, array) in &arrays {
                let broadcasted = array.broadcast(max_shape.clone(), max_dims.clone()).await?;
                arrow_arrays.push(ndarray_to_arrow_array(broadcasted.as_ref()).await?);
            }
            Ok(RecordBatch::try_new(schema, arrow_arrays)?)
        })
        .boxed();
    }

    // Chunked path: generate chunk subsets from the max shape and chunk shape,
    // then for each chunk, subset each array (before broadcasting to avoid materializing full data).
    let subsets = generate_chunk_subsets(&max_shape, &chunk_shape);
    let arrays = dataset.arrays;

    futures::stream::iter(subsets)
        .then(move |subset| {
            let max_dims = max_dims.clone();
            let arrays = arrays.clone();
            let schema = schema.clone();
            async move {
                let mut arrow_arrays: Vec<ArrayRef> = Vec::with_capacity(arrays.len());
                for (name, array) in &arrays {
                    // Compute the subset for this array's dimensions (which may be fewer than max_dims).
                    let array_subset =
                        generate_array_subset_from_chunk(&subset, &max_dims, array.as_ref());
                    let sliced = array
                        .subset(array_subset)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to subset array '{}': {}", name, e))?;
                    let broadcasted = sliced
                        .broadcast(subset.shape.clone(), max_dims.clone())
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to broadcast array '{}': {}", name, e)
                        })?;
                    arrow_arrays.push(ndarray_to_arrow_array(broadcasted.as_ref()).await.map_err(
                        |e| anyhow::anyhow!("Failed to convert array '{}' to Arrow: {}", name, e),
                    )?);
                }
                Ok(RecordBatch::try_new(schema, arrow_arrays)?)
            }
        })
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
        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds)
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

        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds)
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
            dataset_as_record_batch_stream(ds).collect().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[tokio::test]
    async fn test_empty_dataset_error() {
        let ds = make_dataset(vec![]).await;
        let results: Vec<anyhow::Result<RecordBatch>> =
            dataset_as_record_batch_stream(ds).collect().await;
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
        let batches: Vec<RecordBatch> = dataset_as_record_batch_stream(ds)
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
}
