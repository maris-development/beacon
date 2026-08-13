//! Reading one chunk of a dataset into an un-broadcast [`NdRecordBatch`].
//!
//! [`read_nd_chunk`] is what a shared read does with a chunk it pops off the
//! queue (see [`crate::arrow::share`]). Each variable is sliced on its own axes,
//! so a coordinate is read once per chunk rather than once per row of it, and
//! the broadcast to flat Arrow happens above the scan in
//! [`beacon_datafusion_ext::nd::exec::NdBroadcastExec`].

use std::sync::Arc;

use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
use datafusion::error::{DataFusionError, Result};
use indexmap::IndexMap;

use crate::arrow::array::ndarray_to_arrow_array;
use crate::arrow::batch::generate_array_subset_from_chunk;

fn exec_err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}
/// Read one chunk of a regular dataset into an un-broadcast [`NdRecordBatch`].
///
/// Each variable is sliced on its own axes: a variable of lower rank than the
/// chunk reads only the axes it has, so a coordinate is read once per chunk
/// rather than once per row of it.
pub(crate) async fn read_nd_chunk(
    arrays: &IndexMap<String, Arc<dyn crate::NdArrayD>>,
    max_dims: &[String],
    schema: Arc<arrow::datatypes::Schema>,
    subset: crate::array::subset::ArraySubset,
) -> Result<NdRecordBatch> {
    let target = Dimensions::try_new(
        max_dims
            .iter()
            .zip(subset.shape.iter())
            .map(|(name, &size)| Dimension::new(name.as_str(), size))
            .collect(),
    )?;

    let mut columns = Vec::with_capacity(arrays.len());
    for (name, array) in arrays.iter() {
        let array_subset = generate_array_subset_from_chunk(&subset, max_dims, array.as_ref());
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
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::compute::concat_batches;
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use indexmap::IndexMap;

    use beacon_datafusion_ext::nd::decode_nd_record_batch;

    use crate::arrow::batch::build_dataset_schema;
    use crate::arrow::share::{SharedRead, flat_stream};
    use crate::dataset::{AnyDataset, Dataset};
    use crate::{NdArray, NdArrayD};

    /// Read `dataset` as the scan would: encoded, then broadcast back.
    async fn read_encoded(dataset: Dataset, batch_size: usize) -> Vec<RecordBatch> {
        let encoded: Vec<RecordBatch> =
            SharedRead::build(AnyDataset::Regular(dataset), batch_size, None, true, None)
                .await
                .unwrap()
                .stream(None)
                .try_collect()
                .await
                .unwrap();

        encoded
            .iter()
            .map(|batch| {
                decode_nd_record_batch(batch)
                    .unwrap()
                    .materialize()
                    .unwrap()
            })
            .collect()
    }

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

    /// An encoded read, decoded and broadcast, is the flat read.
    ///
    /// The two modes take the same chunks off the same queue and differ only in
    /// what they do with each one, so `encode → decode → materialize` has to be
    /// the identity on the rows.
    #[tokio::test]
    async fn an_encoded_read_matches_a_flat_one() {
        for batch_size in [usize::MAX, 6, 3] {
            let ds = test_dataset().await;
            let schema = build_dataset_schema(&ds.arrays);

            let flat: Vec<RecordBatch> =
                flat_stream(AnyDataset::Regular(ds.clone()), batch_size, None)
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
            let expected = concat_batches(&schema, &flat).unwrap();

            let materialized = read_encoded(ds, batch_size).await;
            let actual = concat_batches(&schema, &materialized).unwrap();

            assert_eq!(actual, expected, "batch_size={batch_size}");
            assert_eq!(actual.num_rows(), 12);
        }
    }

    /// A gridded dataset carrying rank-0 metadata attributes — a variable
    /// attribute (`sst.units`) and a global attribute (`.title`) — surfaced as
    /// scalar arrays. Each rides the `beacon.nd` encoding as a rank-0 column and
    /// broadcasts (replicates) its single value across every row of the grid.
    async fn test_dataset_with_attrs() -> Dataset {
        let base = test_dataset().await;

        // A NetCDF attribute is surfaced as a scalar (rank-0) array: no
        // dimensions, one element — exactly what `AttributeBackend` produces.
        let units = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["celsius".to_string()],
            vec![],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let title = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["demo".to_string()],
            vec![],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let mut arrays = base.arrays.clone();
        arrays.insert("sst.units".to_string(), Arc::new(units));
        arrays.insert(".title".to_string(), Arc::new(title));
        Dataset::new("with-attrs".to_string(), arrays).await
    }

    /// Rank-0 attributes decode and broadcast to a constant column spanning the
    /// full grid — one `"celsius"` / `"demo"` per row, across every chunk size.
    #[tokio::test]
    async fn scalar_attributes_broadcast_across_grid() {
        use arrow::array::StringArray;

        for batch_size in [usize::MAX, 6, 3] {
            let ds = test_dataset_with_attrs().await;
            let schema = build_dataset_schema(&ds.arrays);

            let materialized = read_encoded(ds, batch_size).await;
            let actual = concat_batches(&schema, &materialized).unwrap();

            assert_eq!(actual.num_rows(), 12, "batch_size={batch_size}");

            let units = actual
                .column_by_name("sst.units")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let title = actual
                .column_by_name(".title")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            assert!(
                (0..12).all(|i| units.value(i) == "celsius"),
                "batch_size={batch_size}: sst.units not replicated"
            );
            assert!(
                (0..12).all(|i| title.value(i) == "demo"),
                "batch_size={batch_size}: .title not replicated"
            );
        }
    }
}
