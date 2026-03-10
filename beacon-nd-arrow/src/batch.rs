use std::{collections::HashMap, sync::Arc};

use arrow::array::ArrayRef;
use futures::{StreamExt, stream::BoxStream};

use crate::array::{NdArrowArray, subset::ArraySubset};

pub struct NdRecordBatch {
    pub schema: Arc<arrow_schema::Schema>,
    pub arrays: Vec<Arc<dyn NdArrowArray>>,
    pub dimensions: Vec<String>,
    pub shape: Vec<usize>,
}

impl NdRecordBatch {
    pub fn new(
        schema: Arc<arrow_schema::Schema>,
        arrays: Vec<Arc<dyn NdArrowArray>>,
    ) -> anyhow::Result<Self> {
        // Validate that the number of arrays matches the number of fields in the schema
        if schema.fields().len() != arrays.len() {
            return Err(anyhow::anyhow!(
                "Number of arrays ({}) does not match number of fields in schema ({})",
                arrays.len(),
                schema.fields().len()
            ));
        }

        // Validate that each array's data type matches the corresponding field's data type
        for (field, array) in schema.fields().iter().zip(arrays.iter()) {
            if *field.data_type() != array.data_type() {
                return Err(anyhow::anyhow!(
                    "Data type mismatch for field '{}': expected {:?}, got {:?}",
                    field.name(),
                    field.data_type(),
                    array.data_type()
                ))?;
            }
        }

        let mut unique_dims: HashMap<_, _> = HashMap::new();

        for array in &arrays {
            for (dim, size) in array.dimensions().iter().zip(array.shape().iter()) {
                if let Some(existing_size) = unique_dims.get(dim) {
                    if *existing_size != *size {
                        return Err(anyhow::anyhow!(
                            "Dimension '{}' has conflicting sizes: {} and {}",
                            dim,
                            existing_size,
                            size
                        ))?;
                    }
                } else {
                    unique_dims.insert(dim.clone(), *size);
                }
            }
        }

        Ok(Self {
            schema,
            arrays,
            dimensions: unique_dims.keys().cloned().collect(),
            shape: unique_dims.values().cloned().collect(),
        })
    }

    pub fn schema(&self) -> &arrow_schema::Schema {
        &self.schema
    }

    pub async fn try_as_arrow_stream(
        &self,
        preferred_chunk_size: usize,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<arrow::record_batch::RecordBatch>>> {
        // find the dimensions and shape to broadcast to.
        // Get the biggest dimensions across all arrays to determine broadcasting.
        let max_dims_opt = self
            .arrays
            .iter()
            .max_by_key(|a| a.dimensions().len())
            .map(|a| (a.dimensions(), a.shape()));

        match max_dims_opt {
            Some((max_dims, max_shape)) => {
                let chunk_shape =
                    NdRecordBatch::generate_chunk_shape(&max_shape, preferred_chunk_size);

                if chunk_shape == max_shape {
                    // No chunking needed, just create a single RecordBatch.
                    let mut broadcasted_arrays: Vec<ArrayRef> = vec![];
                    for array in &self.arrays {
                        let view = array.broadcast(&max_shape, &max_dims).await?;
                        let arrow_array = view.as_arrow_array_ref().await?;
                        broadcasted_arrays.push(arrow_array);
                    }

                    let record_batch = arrow::record_batch::RecordBatch::try_new(
                        self.schema.clone(),
                        broadcasted_arrays,
                    )?;

                    return Ok(futures::stream::once(async { Ok(record_batch) }).boxed());
                }

                let subsets =
                    NdRecordBatch::generate_chunk_subsets(max_shape.clone(), chunk_shape.clone());
                let arrays = self.arrays.clone();
                let schema = self.schema.clone();

                let stream = futures::stream::iter(subsets).then(move |subset| {
                    let max_dims = max_dims.clone();
                    let arrays = arrays.clone();
                    let schema = schema.clone();

                    async move {
                        let mut broadcasted_arrays: Vec<ArrayRef> = vec![];
                        for array in &arrays {
                            let array_subset = NdRecordBatch::generate_array_subset_from_chunk(
                                &subset, &max_dims, array,
                            );
                            let array = array.subset(array_subset).await?;
                            let broadcasted = array.broadcast(&subset.shape, &max_dims).await?;
                            let arrow_array = broadcasted.as_arrow_array_ref().await?;
                            broadcasted_arrays.push(arrow_array);
                        }

                        let record_batch = arrow::record_batch::RecordBatch::try_new(
                            schema.clone(),
                            broadcasted_arrays,
                        )?;

                        Ok::<_, anyhow::Error>(record_batch)
                    }
                });

                Ok(stream.boxed())
            }
            None => {
                // No arrays, return an empty stream.
                Ok(futures::stream::empty().boxed())
            }
        }
    }

    fn generate_array_subset_from_chunk(
        subset: &ArraySubset,
        chunk_dimensions: &[String],
        array: &Arc<dyn NdArrowArray>,
    ) -> ArraySubset {
        let array_dims = array.dimensions();

        // Keep only the dimensions that are present in the array, and find their corresponding indices in the chunk dimensions.
        let mut array_subset_start = vec![];
        let mut array_subset_shape = vec![];

        for dim in &array_dims {
            if let Some(chunk_dim_index) = chunk_dimensions.iter().position(|d| d == dim) {
                array_subset_start.push(subset.start[chunk_dim_index]);
                array_subset_shape.push(subset.shape[chunk_dim_index]);
            }
        }

        ArraySubset {
            start: array_subset_start,
            shape: array_subset_shape,
        }
    }

    fn generate_chunk_subsets(
        shape: Vec<usize>,
        chunk_shape: Vec<usize>,
    ) -> impl Iterator<Item = ArraySubset> {
        if shape.len() != chunk_shape.len() {
            return Vec::new().into_iter();
        }

        if shape.is_empty() {
            return vec![ArraySubset {
                start: vec![],
                shape: vec![],
            }]
            .into_iter();
        }

        let chunk_counts: Vec<usize> = shape
            .iter()
            .zip(chunk_shape.iter())
            .map(|(axis_len, axis_chunk)| {
                if *axis_len == 0 {
                    0
                } else {
                    axis_len.div_ceil((*axis_chunk).max(1))
                }
            })
            .collect();

        if chunk_counts.contains(&0) {
            return Vec::new().into_iter();
        }

        let total_chunks = chunk_counts.iter().product::<usize>();
        let mut subsets = Vec::with_capacity(total_chunks);

        for linear_chunk_idx in 0..total_chunks {
            let mut rem = linear_chunk_idx;
            let mut chunk_index = vec![0usize; shape.len()];

            for axis in (0..shape.len()).rev() {
                chunk_index[axis] = rem % chunk_counts[axis];
                rem /= chunk_counts[axis];
            }

            let start: Vec<usize> = chunk_index
                .iter()
                .zip(chunk_shape.iter())
                .map(|(chunk_idx, axis_chunk)| chunk_idx.saturating_mul((*axis_chunk).max(1)))
                .collect();

            let subset_shape: Vec<usize> = shape
                .iter()
                .zip(start.iter())
                .zip(chunk_shape.iter())
                .map(|((axis_len, axis_start), axis_chunk)| {
                    (*axis_chunk).max(1).min(axis_len - axis_start)
                })
                .collect();

            subsets.push(ArraySubset {
                start,
                shape: subset_shape,
            });
        }

        subsets.into_iter()
    }

    fn generate_chunk_shape(shape: &[usize], preferred_chunk_size: usize) -> Vec<usize> {
        // This function generates a chunk shape for a given array shape and preferred chunk size. It tries to create chunks that are as close as possible to the preferred chunk size while respecting the original shape.
        // The chunk shape should always be c-ordered so all the dimensions and the chunks will be read in c-ordered memory layout.
        if shape.is_empty() {
            return vec![];
        }

        if preferred_chunk_size == 0 {
            return vec![1; shape.len()];
        }

        fn ceil_div(a: usize, b: usize) -> usize {
            a.div_ceil(b)
        }

        fn smallest_divisor_at_least(n: usize, min_value: usize) -> usize {
            if n == 0 {
                return 0;
            }

            let min_value = min_value.max(1);
            if min_value >= n {
                return n;
            }

            let mut divisor = min_value;
            while divisor <= n {
                if n % divisor == 0 {
                    return divisor;
                }
                divisor += 1;
            }

            n
        }

        let mut chunk_shape = vec![1usize; shape.len()];
        let mut tail_product = 1usize;

        for axis in (0..shape.len()).rev() {
            let axis_len = shape[axis];

            if axis_len == 0 {
                chunk_shape[axis] = 0;
                continue;
            }

            let required_on_axis = ceil_div(preferred_chunk_size, tail_product.max(1));
            let axis_chunk = smallest_divisor_at_least(axis_len, required_on_axis);

            chunk_shape[axis] = axis_chunk;
            tail_product = tail_product.saturating_mul(axis_chunk.max(1));
        }

        chunk_shape
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{NdArrowArrayDispatch, backend::mem::InMemoryArrayBackend};
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn int32_values(column: &ArrayRef) -> Vec<i32> {
        column
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array")
            .values()
            .to_vec()
    }

    #[test]
    fn generate_chunk_shape_tests() {
        let shape = vec![1000, 500, 200];
        let preferred_chunk_size = 500;
        assert_eq!(
            NdRecordBatch::generate_chunk_shape(&shape, preferred_chunk_size),
            vec![1, 4, 200]
        );

        let shape = vec![1000, 500, 200];
        let preferred_chunk_size = 1000;
        assert_eq!(
            NdRecordBatch::generate_chunk_shape(&shape, preferred_chunk_size),
            vec![1, 5, 200]
        );

        let shape = vec![20, 50, 20];
        let preferred_chunk_size = 128;
        assert_eq!(
            NdRecordBatch::generate_chunk_shape(&shape, preferred_chunk_size),
            vec![1, 10, 20]
        );

        let shape = vec![20, 50, 20];
        let preferred_chunk_size = 5000;
        assert_eq!(
            NdRecordBatch::generate_chunk_shape(&shape, preferred_chunk_size),
            vec![5, 50, 20]
        );

        let shape = vec![20, 50, 20];
        let preferred_chunk_size = 3000;
        assert_eq!(
            NdRecordBatch::generate_chunk_shape(&shape, preferred_chunk_size),
            vec![4, 50, 20]
        );

        let shape = vec![20, 50, 20];
        let preferred_chunk_size = 128000;
        assert_eq!(
            NdRecordBatch::generate_chunk_shape(&shape, preferred_chunk_size),
            vec![20, 50, 20]
        );
    }

    #[test]
    fn generate_chunk_subsets_c_order_and_remainder() {
        let shape = [2, 4];
        let chunk_shape = NdRecordBatch::generate_chunk_shape(&shape, 2);
        let subsets: Vec<ArraySubset> =
            NdRecordBatch::generate_chunk_subsets(shape.to_vec(), chunk_shape).collect();

        assert_eq!(subsets.len(), 4);
        assert_eq!(subsets[0].start, vec![0, 0]);
        assert_eq!(subsets[0].shape, vec![1, 2]);
        assert_eq!(subsets[1].start, vec![0, 2]);
        assert_eq!(subsets[1].shape, vec![1, 2]);
        assert_eq!(subsets[2].start, vec![1, 0]);
        assert_eq!(subsets[2].shape, vec![1, 2]);
        assert_eq!(subsets[3].start, vec![1, 2]);
        assert_eq!(subsets[3].shape, vec![1, 2]);
    }

    #[test]
    fn generate_chunk_subsets_empty_rank() {
        let shape: [usize; 0] = [];
        let chunk_shape = NdRecordBatch::generate_chunk_shape(&shape, 10);
        let subsets: Vec<ArraySubset> =
            NdRecordBatch::generate_chunk_subsets(shape.to_vec(), chunk_shape).collect();
        assert_eq!(subsets.len(), 1);
        assert_eq!(subsets[0].start, Vec::<usize>::new());
        assert_eq!(subsets[0].shape, Vec::<usize>::new());
    }

    #[test]
    fn generate_chunk_subsets_large_dimensions() {
        let shape = [48, 24, 12, 6];
        let chunk_shape = NdRecordBatch::generate_chunk_shape(&shape, 256);
        assert_eq!(chunk_shape, vec![1, 4, 12, 6]);

        let subsets: Vec<ArraySubset> =
            NdRecordBatch::generate_chunk_subsets(shape.to_vec(), chunk_shape).collect();

        assert_eq!(subsets.len(), 288);
        assert_eq!(subsets[0].start, vec![0, 0, 0, 0]);
        assert_eq!(subsets[0].shape, vec![1, 4, 12, 6]);
        assert_eq!(subsets[1].start, vec![0, 4, 0, 0]);
        assert_eq!(subsets[5].start, vec![0, 20, 0, 0]);
        assert_eq!(subsets[6].start, vec![1, 0, 0, 0]);
        assert_eq!(subsets[287].start, vec![47, 20, 0, 0]);
        assert_eq!(subsets[287].shape, vec![1, 4, 12, 6]);
    }

    #[tokio::test]
    async fn generate_array_subset_from_chunk_same_order() {
        let array = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2, 3, 4], (0..24).collect()).unwrap(),
                vec![2, 3, 4],
                vec!["time".to_string(), "lat".to_string(), "lon".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let chunk_subset = ArraySubset {
            start: vec![1, 0, 2],
            shape: vec![1, 3, 2],
        };

        let mapped = NdRecordBatch::generate_array_subset_from_chunk(
            &chunk_subset,
            &["time".to_string(), "lat".to_string(), "lon".to_string()],
            &array,
        );

        assert_eq!(mapped.start, vec![1, 0, 2]);
        assert_eq!(mapped.shape, vec![1, 3, 2]);
    }

    #[tokio::test]
    async fn generate_array_subset_from_chunk_reordered_dims() {
        let array = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![3, 2], (0..6).collect()).unwrap(),
                vec![3, 2],
                vec!["lon".to_string(), "time".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let chunk_subset = ArraySubset {
            start: vec![1, 4, 7],
            shape: vec![2, 5, 3],
        };

        let mapped = NdRecordBatch::generate_array_subset_from_chunk(
            &chunk_subset,
            &["time".to_string(), "lat".to_string(), "lon".to_string()],
            &array,
        );

        assert_eq!(mapped.start, vec![7, 1]);
        assert_eq!(mapped.shape, vec![3, 2]);
    }

    #[tokio::test]
    async fn generate_array_subset_from_chunk_missing_dim_is_skipped() {
        let array = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2, 5], (0..10).collect()).unwrap(),
                vec![2, 5],
                vec!["time".to_string(), "depth".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let chunk_subset = ArraySubset {
            start: vec![1, 4, 7],
            shape: vec![2, 5, 3],
        };

        let mapped = NdRecordBatch::generate_array_subset_from_chunk(
            &chunk_subset,
            &["time".to_string(), "lat".to_string(), "lon".to_string()],
            &array,
        );

        assert_eq!(mapped.start, vec![1]);
        assert_eq!(mapped.shape, vec![2]);
    }

    #[tokio::test]
    async fn try_as_arrow_stream_multiple_arrays_with_broadcast_no_chunking() {
        let array_main = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2, 3], vec![1, 2, 3, 4, 5, 6]).unwrap(),
                vec![2, 3],
                vec!["time".to_string(), "lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let array_lat = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![3], vec![10, 20, 30]).unwrap(),
                vec![3],
                vec!["lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let schema = Arc::new(Schema::new(vec![
            Field::new("main", DataType::Int32, false),
            Field::new("lat_only", DataType::Int32, false),
        ]));

        let record_batch = NdRecordBatch::new(schema, vec![array_main, array_lat]).unwrap();
        let batches = record_batch
            .try_as_arrow_stream(10_000)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 6);
        assert_eq!(int32_values(batches[0].column(0)), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(
            int32_values(batches[0].column(1)),
            vec![10, 20, 30, 10, 20, 30]
        );
    }

    #[tokio::test]
    async fn try_as_arrow_stream_multiple_arrays_with_broadcast_chunked() {
        let array_main = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2, 4], vec![0, 1, 2, 3, 4, 5, 6, 7]).unwrap(),
                vec![2, 4],
                vec!["time".to_string(), "lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let array_lat = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![4], vec![100, 200, 300, 400]).unwrap(),
                vec![4],
                vec!["lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let array_time = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2], vec![7, 8]).unwrap(),
                vec![2],
                vec!["time".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let schema = Arc::new(Schema::new(vec![
            Field::new("main", DataType::Int32, false),
            Field::new("lat_only", DataType::Int32, false),
            Field::new("time_only", DataType::Int32, false),
        ]));

        let record_batch =
            NdRecordBatch::new(schema, vec![array_main, array_lat, array_time]).unwrap();
        let batches = record_batch
            .try_as_arrow_stream(200)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 8);
        assert_eq!(
            int32_values(batches[0].column(0)),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
        assert_eq!(
            int32_values(batches[0].column(1)),
            vec![100, 200, 300, 400, 100, 200, 300, 400]
        );
        assert_eq!(
            int32_values(batches[0].column(2)),
            vec![7, 7, 7, 7, 8, 8, 8, 8]
        );
    }

    #[tokio::test]
    async fn try_as_arrow_stream_empty_batch_returns_empty_stream() {
        let schema = Arc::new(Schema::new(Vec::<Field>::new()));
        let record_batch = NdRecordBatch::new(schema, vec![]).unwrap();

        let batches = record_batch
            .try_as_arrow_stream(128)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn try_as_arrow_stream_three_arrays_mixed_broadcast_chunked() {
        let array_main = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2, 4], vec![0, 1, 2, 3, 4, 5, 6, 7]).unwrap(),
                vec![2, 4],
                vec!["time".to_string(), "lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let array_lat = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![4], vec![10, 20, 30, 40]).unwrap(),
                vec![4],
                vec!["lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let array_time = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2], vec![7, 8]).unwrap(),
                vec![2],
                vec!["time".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let schema = Arc::new(Schema::new(vec![
            Field::new("main", DataType::Int32, false),
            Field::new("lat_only", DataType::Int32, false),
            Field::new("time_only", DataType::Int32, false),
        ]));

        let record_batch =
            NdRecordBatch::new(schema, vec![array_main, array_lat, array_time]).unwrap();
        let batches = record_batch
            .try_as_arrow_stream(2)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 4);

        assert_eq!(int32_values(batches[0].column(0)), vec![0, 1]);
        assert_eq!(int32_values(batches[1].column(0)), vec![2, 3]);
        assert_eq!(int32_values(batches[2].column(0)), vec![4, 5]);
        assert_eq!(int32_values(batches[3].column(0)), vec![6, 7]);

        assert_eq!(int32_values(batches[0].column(1)), vec![10, 20]);
        assert_eq!(int32_values(batches[1].column(1)), vec![30, 40]);
        assert_eq!(int32_values(batches[2].column(1)), vec![10, 20]);
        assert_eq!(int32_values(batches[3].column(1)), vec![30, 40]);

        assert_eq!(int32_values(batches[0].column(2)), vec![7, 7]);
        assert_eq!(int32_values(batches[1].column(2)), vec![7, 7]);
        assert_eq!(int32_values(batches[2].column(2)), vec![8, 8]);
        assert_eq!(int32_values(batches[3].column(2)), vec![8, 8]);
    }

    #[tokio::test]
    async fn try_as_arrow_stream_preferred_chunk_size_zero_uses_unit_chunks() {
        let array_main = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2, 2], vec![1, 2, 3, 4]).unwrap(),
                vec![2, 2],
                vec!["time".to_string(), "lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "main",
            DataType::Int32,
            false,
        )]));
        let record_batch = NdRecordBatch::new(schema, vec![array_main]).unwrap();

        let batches = record_batch
            .try_as_arrow_stream(0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 4);
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![1, 1, 1, 1]
        );
        assert_eq!(int32_values(batches[0].column(0)), vec![1]);
        assert_eq!(int32_values(batches[1].column(0)), vec![2]);
        assert_eq!(int32_values(batches[2].column(0)), vec![3]);
        assert_eq!(int32_values(batches[3].column(0)), vec![4]);
    }

    #[tokio::test]
    async fn try_as_arrow_stream_no_broadcasting_required_chunked() {
        let array_a = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2, 4], vec![1, 2, 3, 4, 5, 6, 7, 8]).unwrap(),
                vec![2, 4],
                vec!["time".to_string(), "lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let array_b = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(vec![2, 4], vec![11, 12, 13, 14, 15, 16, 17, 18])
                    .unwrap(),
                vec![2, 4],
                vec!["time".to_string(), "lat".to_string()],
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let record_batch = NdRecordBatch::new(schema, vec![array_a, array_b]).unwrap();
        let batches = record_batch
            .try_as_arrow_stream(2)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 4);
        assert_eq!(int32_values(batches[0].column(0)), vec![1, 2]);
        assert_eq!(int32_values(batches[1].column(0)), vec![3, 4]);
        assert_eq!(int32_values(batches[2].column(0)), vec![5, 6]);
        assert_eq!(int32_values(batches[3].column(0)), vec![7, 8]);

        assert_eq!(int32_values(batches[0].column(1)), vec![11, 12]);
        assert_eq!(int32_values(batches[1].column(1)), vec![13, 14]);
        assert_eq!(int32_values(batches[2].column(1)), vec![15, 16]);
        assert_eq!(int32_values(batches[3].column(1)), vec![17, 18]);
    }

    #[tokio::test]
    async fn try_as_arrow_stream_three_arrays_time_lon_lat() {
        let dims = vec!["time".to_string(), "lon".to_string(), "lat".to_string()];

        let time_array = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(
                    vec![2, 3, 2],
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                )
                .unwrap(),
                vec![2, 3, 2],
                dims.clone(),
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let lon_array = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(
                    vec![2, 3, 2],
                    vec![101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112],
                )
                .unwrap(),
                vec![2, 3, 2],
                dims.clone(),
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let lat_array = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(
                    vec![2, 3, 2],
                    vec![201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212],
                )
                .unwrap(),
                vec![2, 3, 2],
                dims,
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int32, false),
            Field::new("lon", DataType::Int32, false),
            Field::new("lat", DataType::Int32, false),
        ]));

        let record_batch =
            NdRecordBatch::new(schema, vec![time_array, lon_array, lat_array]).unwrap();
        let batches = record_batch
            .try_as_arrow_stream(4)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(int32_values(batches[0].column(0)), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(
            int32_values(batches[1].column(0)),
            vec![7, 8, 9, 10, 11, 12]
        );

        assert_eq!(
            int32_values(batches[0].column(1)),
            vec![101, 102, 103, 104, 105, 106]
        );
        assert_eq!(
            int32_values(batches[1].column(1)),
            vec![107, 108, 109, 110, 111, 112]
        );

        assert_eq!(
            int32_values(batches[0].column(2)),
            vec![201, 202, 203, 204, 205, 206]
        );
        assert_eq!(
            int32_values(batches[1].column(2)),
            vec![207, 208, 209, 210, 211, 212]
        );
    }
}
