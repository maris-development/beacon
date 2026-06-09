//! Public entry points for streaming an [`AnyDataset`] as Arrow
//! [`RecordBatch`]es.
//!
//! This module only dispatches by dataset kind; the actual read logic lives in
//! [`super::regular`] (regular datasets) and [`super::ragged`] (CF
//! contiguous-ragged datasets), and the parallel/shareable producer lives in
//! [`super::parallel`].

use std::sync::Arc;

use arrow::{array::RecordBatchOptions, datatypes::Schema, record_batch::RecordBatch};
use futures::{StreamExt, stream::BoxStream};

use crate::{arrow::pushdown_filter::PushdownFilter, dataset::any::AnyDataset};

use super::metrics::DatasetReadMetrics;
use super::ragged::ragged_dataset_as_record_batch_stream;

// Re-exported so the public API paths `arrow::batch::dataset_as_record_batch_stream`
// and `arrow::batch::ParallelDatasetStream` remain stable after the split.
pub use super::parallel::ParallelDatasetStream;
pub use super::regular::dataset_as_record_batch_stream;

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
        AnyDataset::Regular(ds) => dataset_as_record_batch_stream(ds, batch_size, predicate, metrics),
        AnyDataset::Ragged { ragged, .. } => {
            ragged_dataset_as_record_batch_stream(ragged, batch_size, predicate, metrics)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::Dataset;
    use crate::dataset::any::AnyDataset;
    use crate::{NdArray, NdArrayD};
    use arrow::array::{Array, Int32Array};
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
}
