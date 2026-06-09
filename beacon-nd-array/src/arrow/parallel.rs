//! Parallel, shareable producer of [`RecordBatch`]es from an [`AnyDataset`].
//!
//! Reuses the per-unit read planning of [`super::regular`] and [`super::ragged`]
//! but drives the independent read units concurrently across worker tasks,
//! publishing into a cloneable MPMC channel so multiple consumers can
//! work-share a single underlying read.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::Semaphore;

use crate::{
    arrow::{
        metrics::DatasetReadMetrics,
        pushdown_filter::PushdownFilter,
        ragged::{RaggedStreamPlan, plan_ragged_stream, read_ragged_unit},
        regular::{RegularStreamPlan, plan_regular_stream, read_regular_unit},
    },
    dataset::{Dataset, any::AnyDataset, ragged::RaggedDataset},
};

/// A parallel, shareable producer of [`RecordBatch`]es from an [`AnyDataset`].
///
/// Unlike [`any_dataset_as_record_batch_stream`] — which reads chunks/casts
/// sequentially and can only be polled by a single consumer — this drives the
/// independent read units concurrently across `parallelism` worker tasks and
/// publishes the results into a cloneable MPMC channel.
///
/// The handle is [`Clone`]: cloning it and calling [`into_stream`] on each
/// clone yields multiple consumers that **work-share** a single underlying
/// producer — every batch is delivered to exactly one consumer. This lets, for
/// example, multiple DataFusion partitions opening the same file consume one
/// shared parallel read.
///
/// [`into_stream`]: ParallelDatasetStream::into_stream
/// [`any_dataset_as_record_batch_stream`]: super::batch::any_dataset_as_record_batch_stream
///
/// Note: because reads run concurrently and the channel is MPMC, batches are
/// emitted in **non-deterministic order** (unlike the sequential stream). This
/// matches how DataFusion treats partition output as an unordered union.
#[derive(Clone, Debug)]
pub struct ParallelDatasetStream {
    receiver: flume::Receiver<anyhow::Result<RecordBatch>>,
}

impl ParallelDatasetStream {
    /// Spawn the coordinator and worker tasks that read `dataset` in parallel.
    ///
    /// `parallelism` bounds both the number of concurrently-reading tasks and
    /// the channel capacity (providing backpressure). It is clamped to at least
    /// 1. Must be called from within a Tokio runtime.
    pub fn spawn(
        dataset: AnyDataset,
        batch_size: usize,
        predicate: Option<PushdownFilter>,
        metrics: Option<DatasetReadMetrics>,
        parallelism: usize,
    ) -> Self {
        let parallelism = parallelism.max(1);
        let (sender, receiver) = flume::bounded::<anyhow::Result<RecordBatch>>(parallelism);

        tokio::spawn(async move {
            match dataset {
                AnyDataset::Regular(ds) => {
                    run_regular_parallel(ds, batch_size, predicate, metrics, parallelism, sender)
                        .await;
                }
                AnyDataset::Ragged { ragged, .. } => {
                    run_ragged_parallel(ragged, batch_size, predicate, metrics, parallelism, sender)
                        .await;
                }
            }
        });

        Self { receiver }
    }

    /// A per-consumer stream view over the shared producer. Clone the handle
    /// first, then call this on each clone to obtain multiple work-sharing
    /// consumers.
    pub fn into_stream(&self) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        self.receiver.clone().into_stream().boxed()
    }
}

/// Drive the read units of `units` concurrently, bounded by `parallelism`,
/// sending each result into `sender`. Mirrors the semaphore-bounded fan-out
/// used by `beacon_binary_format`'s `AsyncStreamProducer`.
async fn fan_out<F>(
    units: Vec<F>,
    parallelism: usize,
    sender: flume::Sender<anyhow::Result<RecordBatch>>,
) where
    F: std::future::Future<Output = Option<anyhow::Result<RecordBatch>>> + Send + 'static,
{
    let sem = Arc::new(Semaphore::new(parallelism));
    let mut handles = Vec::with_capacity(units.len());
    for unit in units {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let sender = sender.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit; // held until this read finishes
            if let Some(res) = unit.await {
                let _ = sender.send_async(res).await;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    drop(sender);
}

async fn run_regular_parallel(
    dataset: Dataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
    metrics: Option<DatasetReadMetrics>,
    parallelism: usize,
    sender: flume::Sender<anyhow::Result<RecordBatch>>,
) {
    let plan = match plan_regular_stream(dataset, batch_size, predicate).await {
        Ok(plan) => plan,
        Err(e) => {
            let _ = sender.send_async(Err(e)).await;
            return;
        }
    };
    let RegularStreamPlan {
        arrays,
        subsets,
        schema,
        max_dims,
        dim_masks,
    } = plan;
    let max_dims = Arc::new(max_dims);

    let units: Vec<_> = subsets
        .into_iter()
        .map(|subset| {
            let arrays = arrays.clone();
            let schema = schema.clone();
            let max_dims = max_dims.clone();
            let dim_masks = dim_masks.clone();
            let metrics = metrics.clone();
            async move {
                read_regular_unit(&arrays, subset, schema, &max_dims, &dim_masks, &metrics).await
            }
        })
        .collect();

    fan_out(units, parallelism, sender).await;
}

async fn run_ragged_parallel(
    ragged: RaggedDataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
    metrics: Option<DatasetReadMetrics>,
    parallelism: usize,
    sender: flume::Sender<anyhow::Result<RecordBatch>>,
) {
    let plan = match plan_ragged_stream(ragged, batch_size, predicate, &metrics).await {
        Ok(plan) => plan,
        Err(e) => {
            let _ = sender.send_async(Err(e)).await;
            return;
        }
    };
    let RaggedStreamPlan {
        ragged,
        schema,
        obs_dims,
        offsets,
        passing_indices,
        ranges,
    } = plan;
    let shared = Arc::new((ragged, schema, obs_dims, offsets, passing_indices));

    let units: Vec<_> = ranges
        .into_iter()
        .map(|range| {
            let shared = shared.clone();
            let metrics = metrics.clone();
            async move {
                let (ragged, schema, obs_dims, offsets, passing_indices) = &*shared;
                Some(
                    read_ragged_unit(
                        ragged,
                        schema,
                        obs_dims,
                        offsets,
                        passing_indices,
                        range,
                        &metrics,
                    )
                    .await,
                )
            }
        })
        .collect();

    fan_out(units, parallelism, sender).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::batch::any_dataset_as_record_batch_stream;
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

    /// The chunked `[6, 1]` i32 dataset (`vals = [1..=6]`).
    async fn make_chunked_i32() -> AnyDataset {
        let nd = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![1, 2, 3, 4, 5, 6],
            vec![6, 1],
            vec!["time".to_string(), "x".to_string()],
            None,
        )
        .unwrap();
        let ds = make_dataset(vec![("vals", Arc::new(nd))]).await;
        AnyDataset::try_from_dataset(ds).await.unwrap()
    }

    /// A minimal CF contiguous-ragged-array dataset with 3 casts.
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
        make_dataset(vec![
            ("station_id", Arc::new(station_id)),
            ("z_row_size", Arc::new(z_row_size)),
            ("z_row_size.sample_dimension", Arc::new(sample_dim)),
            ("depth", Arc::new(depth)),
            ("temperature", Arc::new(temperature)),
        ])
        .await
    }

    /// Collect every i32 value from column 0 across all batches, sorted.
    /// Order-insensitive — suitable for the parallel stream's non-deterministic
    /// batch order.
    fn sorted_i32_values(batches: &[RecordBatch]) -> Vec<i32> {
        let mut values: Vec<i32> = Vec::new();
        for batch in batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            values.extend(col.values().iter().copied());
        }
        values.sort_unstable();
        values
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_matches_sequential_regular() {
        // Sequential reference.
        let seq: Vec<RecordBatch> =
            any_dataset_as_record_batch_stream(make_chunked_i32().await, 2, None, None)
                .try_collect()
                .await
                .unwrap();
        let expected = sorted_i32_values(&seq);
        assert_eq!(expected, vec![1, 2, 3, 4, 5, 6]);

        // Parallel: same multiset of values, regardless of batch order.
        let par = ParallelDatasetStream::spawn(make_chunked_i32().await, 2, None, None, 4);
        let par_batches: Vec<RecordBatch> = par.into_stream().try_collect().await.unwrap();
        assert_eq!(sorted_i32_values(&par_batches), expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_work_sharing_two_consumers() {
        let seq_total: usize =
            any_dataset_as_record_batch_stream(make_chunked_i32().await, 2, None, None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();

        // One shared producer, two work-sharing consumers polled concurrently.
        let shared = ParallelDatasetStream::spawn(make_chunked_i32().await, 2, None, None, 4);
        let a = shared.clone().into_stream().try_collect::<Vec<RecordBatch>>();
        let b = shared.into_stream().try_collect::<Vec<RecordBatch>>();
        let (a, b) = futures::future::join(a, b).await;
        let a = a.unwrap();
        let b = b.unwrap();

        // Each batch is delivered to exactly one consumer: combined rows equal
        // the sequential total (no loss, no duplication), and the union of
        // values is the full dataset.
        let combined_rows: usize = a.iter().chain(b.iter()).map(|batch| batch.num_rows()).sum();
        assert_eq!(combined_rows, seq_total);

        let mut combined: Vec<i32> = Vec::new();
        combined.extend(sorted_i32_values(&a));
        combined.extend(sorted_i32_values(&b));
        combined.sort_unstable();
        assert_eq!(combined, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_parallel_parallelism_one() {
        let par = ParallelDatasetStream::spawn(make_chunked_i32().await, 2, None, None, 1);
        let batches: Vec<RecordBatch> = par.into_stream().try_collect().await.unwrap();
        assert_eq!(sorted_i32_values(&batches), vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_matches_sequential_ragged() {
        let make = || async {
            let ds = make_ragged().await;
            AnyDataset::try_from_dataset(ds).await.unwrap()
        };

        let seq: Vec<RecordBatch> = any_dataset_as_record_batch_stream(make().await, 1, None, None)
            .try_collect()
            .await
            .unwrap();
        let seq_rows: usize = seq.iter().map(|b| b.num_rows()).sum();

        let par = ParallelDatasetStream::spawn(make().await, 1, None, None, 4);
        let par_batches: Vec<RecordBatch> = par.into_stream().try_collect().await.unwrap();
        let par_rows: usize = par_batches.iter().map(|b| b.num_rows()).sum();

        // Same number of batches and total rows as the sequential stream.
        assert_eq!(par_batches.len(), seq.len());
        assert_eq!(par_rows, seq_rows);
    }

    #[tokio::test]
    async fn test_parallel_propagates_init_error() {
        let ds = make_dataset(vec![]).await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();
        let par = ParallelDatasetStream::spawn(any, usize::MAX, None, None, 4);
        let results: Vec<anyhow::Result<RecordBatch>> = par.into_stream().collect().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }
}
