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
use indexmap::IndexMap;
use beacon_datafusion_ext::nd::{
    Dimension, Dimensions, NdArrowArray, NdRecordBatch, encode_flat_batch_as_nd,
    encode_nd_record_batch,
};
use datafusion::error::{DataFusionError, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use crate::arrow::array::ndarray_to_arrow_array;
use crate::arrow::batch::{
    ChunkGrid, any_dataset_as_record_batch_stream_split, build_dataset_schema, chunk_grid,
    generate_array_subset_from_chunk,
};
use crate::arrow::split::{ChunkSplit, split_ordinals};
use crate::dataset::{AnyDataset, Dataset};

fn exec_err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

/// Stream a dataset as `beacon.nd`-encoded `RecordBatch`es — the form a file
/// opener returns so the data can ride a `DataSourceExec` up to an
/// `NdSourceExec`, which decodes it, and an `NdBroadcastExec`, which broadcasts.
///
/// A regular dataset is chunked into un-broadcast [`NdRecordBatch`]es and
/// encoded. A ragged dataset is run-length expanded by the v1 stream, then each
/// flat batch is wrapped on a synthetic `row` dimension and encoded (so the
/// later broadcast is the identity).
pub fn any_dataset_as_encoded_stream(
    dataset: AnyDataset,
    batch_size: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    any_dataset_as_encoded_stream_split(dataset, batch_size, None)
}

/// The same, over one [`ChunkSplit`] of the dataset.
///
/// A `None` split reads the whole dataset, so this is a drop-in for
/// [`any_dataset_as_encoded_stream`]. A `Some` split reads one share of it. The
/// shares of a file tile it, so a scan that opens one file in several partitions
/// returns each row exactly once. See [`crate::arrow::split`].
pub fn any_dataset_as_encoded_stream_split(
    dataset: AnyDataset,
    batch_size: usize,
    split: Option<ChunkSplit>,
) -> BoxStream<'static, Result<RecordBatch>> {
    match dataset {
        AnyDataset::Regular(regular) => dataset_as_nd_stream_split(regular, batch_size, split)
            .map(|nd| nd.and_then(|batch| encode_nd_record_batch(&batch)))
            .boxed(),
        ragged => any_dataset_as_record_batch_stream_split(ragged, batch_size, None, None, split)
            .map_err(exec_err)
            .and_then(|flat| async move { encode_flat_batch_as_nd(&flat) })
            .boxed(),
    }
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

/// Stream a dataset as encoded batches, reading `in_flight` chunks at once.
///
/// [`any_dataset_as_encoded_stream`] reads one chunk at a time, which suits a
/// caller that is one of several partitions each reading its own share. A shared
/// stream has one reader for every partition, so the concurrency has to come
/// from inside it instead.
///
/// A ragged dataset falls back to the sequential stream. Its batch plan is built
/// from cumulative offsets that the plan itself depends on, so the reads are not
/// independent the way chunk reads are.
pub fn any_dataset_as_encoded_stream_concurrent(
    dataset: AnyDataset,
    batch_size: usize,
    in_flight: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    let AnyDataset::Regular(regular) = dataset else {
        return any_dataset_as_encoded_stream(dataset, batch_size);
    };

    let ChunkGrid {
        dims: max_dims,
        chunks,
    } = match chunk_grid(&regular, batch_size) {
        Ok(grid) => grid,
        Err(e) => return futures::stream::once(async move { Err(exec_err(e)) }).boxed(),
    };

    let arrays = Arc::new(regular.arrays);
    let max_dims = Arc::new(max_dims);
    let schema = build_dataset_schema(&arrays);

    futures::stream::iter(chunks)
        .map(move |subset| {
            let arrays = arrays.clone();
            let max_dims = max_dims.clone();
            let schema = schema.clone();
            async move {
                let nd = read_nd_chunk(&arrays, &max_dims, schema, subset).await?;
                encode_nd_record_batch(&nd)
            }
        })
        .buffered(in_flight.max(1))
        .boxed()
}

/// Chunk a regular [`Dataset`] in C-order into a stream of un-broadcast
/// [`NdRecordBatch`]es — each variable kept on its own dimensions.
pub fn dataset_as_nd_stream(
    dataset: Dataset,
    batch_size: usize,
) -> BoxStream<'static, Result<NdRecordBatch>> {
    dataset_as_nd_stream_split(dataset, batch_size, None)
}

/// The same, over one [`ChunkSplit`] of the chunk list.
pub fn dataset_as_nd_stream_split(
    dataset: Dataset,
    batch_size: usize,
    split: Option<ChunkSplit>,
) -> BoxStream<'static, Result<NdRecordBatch>> {
    let ChunkGrid {
        dims: max_dims,
        chunks,
    } = match chunk_grid(&dataset, batch_size) {
        Ok(grid) => grid,
        Err(e) => return futures::stream::once(async move { Err(exec_err(e)) }).boxed(),
    };

    // This partition's share of the list.
    let owned = split_ordinals(split, chunks.len());
    let subsets: Vec<_> = chunks
        .into_iter()
        .skip(owned.start)
        .take(owned.len())
        .collect();
    let arrays = Arc::new(dataset.arrays);
    let max_dims = Arc::new(max_dims);
    let schema = build_dataset_schema(&arrays);

    futures::stream::iter(subsets)
        .then(move |subset| {
            let arrays = arrays.clone();
            let max_dims = max_dims.clone();
            let schema = schema.clone();
            async move { read_nd_chunk(&arrays, &max_dims, schema, subset).await }
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

    use beacon_datafusion_ext::nd::decode_nd_record_batch;

    use crate::arrow::split::byte_tiling_splits;

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

    /// The encoded stream, decoded and broadcast, matches the v1 broadcast
    /// stream (i.e. encode → decode → materialize is faithful).
    #[tokio::test]
    async fn encoded_stream_matches_v1_broadcast() {
        for batch_size in [usize::MAX, 6, 3] {
            let ds = test_dataset().await;
            let schema = build_dataset_schema(&ds.arrays);

            let v1: Vec<RecordBatch> =
                dataset_as_record_batch_stream(ds.clone(), batch_size, None, None)
                    .try_collect()
                    .await
                    .unwrap();
            let expected = concat_batches(&schema, &v1).unwrap();

            let encoded: Vec<RecordBatch> =
                any_dataset_as_encoded_stream(AnyDataset::Regular(ds), batch_size)
                    .try_collect()
                    .await
                    .unwrap();
            let materialized: Vec<RecordBatch> = encoded
                .iter()
                .map(|b| decode_nd_record_batch(b).unwrap().materialize().unwrap())
                .collect();
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

    /// Read one split of a dataset, decoded and broadcast.
    async fn read_split(
        dataset: &Dataset,
        batch_size: usize,
        split: Option<ChunkSplit>,
    ) -> Vec<RecordBatch> {
        let encoded: Vec<RecordBatch> = any_dataset_as_encoded_stream_split(
            AnyDataset::Regular(dataset.clone()),
            batch_size,
            split,
        )
        .try_collect()
        .await
        .unwrap();

        encoded
            .iter()
            .map(|b| decode_nd_record_batch(b).unwrap().materialize().unwrap())
            .collect()
    }

    /// The splits of a dataset, read in order and concatenated, equal the
    /// unsplit read.
    ///
    /// This is the property a parallel scan of one file rests on. A gap loses
    /// rows. An overlap returns them twice. Neither shows up as an error, so it
    /// has to be tested here.
    #[tokio::test]
    async fn the_splits_of_a_dataset_rebuild_the_whole_read() {
        for batch_size in [usize::MAX, 6, 3, 1] {
            let ds = test_dataset().await;
            let schema = build_dataset_schema(&ds.arrays);

            let whole = read_split(&ds, batch_size, None).await;
            let expected = concat_batches(&schema, &whole).unwrap();
            assert_eq!(expected.num_rows(), 12);

            // Five parts over a 4x3 grid also covers the case of more parts than
            // chunks, where some parts read nothing.
            for parts in 1..=5 {
                let mut batches = Vec::new();
                for split in byte_tiling_splits(parts) {
                    batches.extend(read_split(&ds, batch_size, split).await);
                }
                let actual = concat_batches(&schema, &batches).unwrap();
                assert_eq!(actual, expected, "batch_size={batch_size} parts={parts}");
            }
        }
    }

    /// Rank-0 attributes decode and broadcast to a constant column spanning the
    /// full grid — one `"celsius"` / `"demo"` per row, across every chunk size.
    #[tokio::test]
    async fn scalar_attributes_broadcast_across_grid() {
        use arrow::array::StringArray;

        for batch_size in [usize::MAX, 6, 3] {
            let ds = test_dataset_with_attrs().await;
            let schema = build_dataset_schema(&ds.arrays);

            let encoded: Vec<RecordBatch> =
                any_dataset_as_encoded_stream(AnyDataset::Regular(ds), batch_size)
                    .try_collect()
                    .await
                    .unwrap();
            let materialized: Vec<RecordBatch> = encoded
                .iter()
                .map(|b| decode_nd_record_batch(b).unwrap().materialize().unwrap())
                .collect();
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
