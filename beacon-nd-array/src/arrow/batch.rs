use std::sync::Arc;

use arrow::{
    array::{ArrayRef, new_null_array},
    compute::concat,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, stream::BoxStream};

use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    dataset::{Dataset, any::AnyDataset, ragged::RaggedDataset},
};

use super::array::ndarray_to_arrow_array;

pub fn any_dataset_as_record_batch_stream(
    dataset: AnyDataset,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    match dataset {
        AnyDataset::Regular(ds) => dataset_as_record_batch_stream(ds),
        AnyDataset::Ragged { ragged, .. } => {
            let schema = ragged_record_batch_schema(&ragged);
            let obs_dims: std::collections::HashSet<String> =
                ragged.observation_dimensions().map(String::from).collect();
            let n = ragged.len();
            futures::stream::iter(0..n)
                .then(move |index| {
                    let ragged = ragged.clone();
                    let schema = schema.clone();
                    let obs_dims = obs_dims.clone();
                    async move {
                        let cast = ragged.get_cast(index).await?;
                        ragged_cast_to_record_batch(&cast, &schema, &obs_dims).await
                    }
                })
                .boxed()
        }
    }
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

/// Convert a single cast [`Dataset`] (as returned by
/// [`RaggedDataset::get_cast`]) into a [`RecordBatch`] aligned to
/// `schema`.
///
/// - Arrays whose first-axis length equals the target row count are
///   converted directly.
/// - Shorter arrays (including dimensionless scalars and instance vars
///   with shape \[1\]) are repeated to the target row count.
/// - Arrays missing from the cast are filled with nulls.
async fn ragged_cast_to_record_batch(
    cast: &Dataset,
    schema: &Arc<Schema>,
    obs_dims: &std::collections::HashSet<String>,
) -> anyhow::Result<RecordBatch> {
    // Target row count = max first-axis size across observation-dimension
    // arrays. Instance variables (shape [1]) and dimensionless attributes
    // are excluded from this calculation.
    let target_rows = cast
        .arrays
        .values()
        .filter(|a| a.dimensions().first().is_some_and(|d| obs_dims.contains(d)))
        .map(|a| a.shape()[0])
        .max()
        .unwrap_or(0);

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col = match cast.get_array(field.name()) {
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
                    // Empty cast — no observation rows.
                    new_null_array(field.data_type(), 0)
                } else if is_obs {
                    // Observation variable on a shorter obs dimension → null-pad.
                    let pad_len = target_rows - arr_len;
                    let null_tail = new_null_array(field.data_type(), pad_len);
                    concat(&[arrow_arr.as_ref(), null_tail.as_ref()])?
                } else {
                    // Instance variable or attribute → repeat to target row count.
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

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any)
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

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any)
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

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any)
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

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any)
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

        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any)
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
        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any)
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
        let batches: Vec<RecordBatch> = any_dataset_as_record_batch_stream(any)
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
