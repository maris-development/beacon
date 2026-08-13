use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use crossbeam::queue::ArrayQueue;
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;
use futures::stream::BoxStream;
use indexmap::IndexMap;

use crate::NdArrayD;
use std::ops::Range;

use crate::array::subset::ArraySubset;
use crate::arrow::batch::{
    ChunkGrid, RaggedPlan, build_dataset_schema, chunk_grid, chunk_is_pruned,
    compute_predicate_masks, plan_ragged_read, read_chunk, read_ragged_range,
};
use crate::arrow::nd_provider::read_nd_chunk;
use crate::arrow::pushdown_filter::PushdownFilter;
use crate::dataset::AnyDataset;

/// Which batches a shared read produces.
///
/// A scan is one or the other throughout, so the mode is fixed when the queue is
/// filled. The first partition to arrive chooses it, and every partition of a
/// scan would choose the same, so whichever arrives first is the right one.
///
/// Both modes carry the predicate, and both prune on it: the queue is filled
/// with every chunk of the file either way, and a chunk no row of which can meet
/// the predicate is dropped when it is popped, before it is read.
#[derive(Debug, Clone)]
pub enum ReadMode {
    /// `beacon.nd`-encoded batches, which an `NdSourceExec` decodes above the
    /// scan. This is what a column read produces.
    Encoded(Option<PushdownFilter>),
    /// Flat, broadcast batches. This is what the `COUNT(*)` path reads.
    Flat(Option<PushdownFilter>),
}

impl ReadMode {
    /// The predicate this mode prunes on, if it has one.
    fn predicate(&self) -> Option<PushdownFilter> {
        match self {
            ReadMode::Encoded(predicate) | ReadMode::Flat(predicate) => predicate.clone(),
        }
    }
}

/// One unit of work: what a partition reads for one pop.
///
/// The two dataset shapes divide differently, so the queue carries whichever
/// unit its file is made of. Both are read once and by one partition, which is
/// all the queue needs of them.
#[derive(Debug)]
enum Work {
    /// One hyperslab of a regular dataset's chunk grid.
    Grid(ArraySubset),
    /// One batch of a ragged dataset's plan, as a range of passing casts.
    ///
    /// A ragged dataset has no chunk grid. Its batches are cut where the cast
    /// boundaries fall, which takes the cumulative offsets and the predicate
    /// masks to work out, so the plan is built once by whichever partition
    /// arrives first. After that a range reads on its own, exactly as a chunk
    /// does.
    Ragged(Range<usize>),
}

/// A file opened once, and the subsets left to read from it.
#[derive(Debug)]
pub struct SharedRead {
    queue: ArrayQueue<Work>,
    read: ReadKind,
    mode: ReadMode,
    /// Per-dimension keep masks, computed once for the file rather than once per
    /// partition. Empty unless the mode is [`ReadMode::Flat`] with a predicate.
    dim_masks: Arc<Vec<(String, Vec<bool>)>>,
}

#[derive(Debug)]
enum ReadKind {
    Grid {
        arrays: Arc<IndexMap<String, Arc<dyn NdArrayD>>>,
        dims: Arc<Vec<String>>,
        schema: Arc<Schema>,
    },
    Ragged {
        plan: Arc<RaggedPlan>,
    },
}

impl SharedRead {
    /// Open `dataset` for sharing, and fill the queue with its subsets.
    ///
    /// A regular dataset is cut on its chunk grid, which is the same grid an
    /// unshared read walks. A ragged one is cut on its batch plan, which is the
    /// same plan an unshared read builds.
    pub async fn build(
        dataset: AnyDataset,
        batch_size: usize,
        mode: ReadMode,
    ) -> Result<Arc<Self>> {
        let predicate = mode.predicate();

        let regular = match dataset {
            AnyDataset::Regular(regular) => regular,
            AnyDataset::Ragged { ragged, .. } => {
                let plan = plan_ragged_read(ragged, batch_size, predicate)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                let queue = ArrayQueue::new(plan.ranges.len().max(1));
                for range in plan.ranges.clone() {
                    // The capacity is the range count, so this cannot fail.
                    let _ = queue.push(Work::Ragged(range));
                }
                return Ok(Arc::new(Self {
                    queue,
                    read: ReadKind::Ragged {
                        plan: Arc::new(plan),
                    },
                    mode,
                    dim_masks: Arc::new(Vec::new()),
                }));
            }
        };

        let ChunkGrid { dims, chunks } = chunk_grid(&regular, batch_size)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // A dataset with no chunks still needs a queue, and `ArrayQueue` will
        // not take a capacity of zero.
        let queue = ArrayQueue::new(chunks.len().max(1));
        for subset in chunks {
            // The capacity is the chunk count, so this cannot fail.
            let _ = queue.push(Work::Grid(subset));
        }

        let arrays = Arc::new(regular.arrays);
        let schema = build_dataset_schema(&arrays);

        // Computed once for the file. An unshared read computes these per
        // partition, which reads the coordinate arrays once per partition.
        let dim_masks = compute_predicate_masks(&arrays, predicate)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        Ok(Arc::new(Self {
            queue,
            read: ReadKind::Grid {
                arrays,
                dims: Arc::new(dims),
                schema,
            },
            mode,
            dim_masks: Arc::new(dim_masks),
        }))
    }

    /// How many subsets are left. For tests and diagnostics.
    pub fn remaining(&self) -> usize {
        self.queue.len()
    }

    /// One partition's stream: pop, read, yield, until the queue is empty.
    ///
    /// Every partition of the file calls this and they all pull from the same
    /// queue, so the batches divide between them as fast as each can take them.
    /// A partition that drops its stream stops popping, and whatever it had not
    /// taken is left for the others.
    pub fn stream(self: Arc<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::unfold(self, |shared| async move {
            let work = shared.queue.pop()?;
            let batches = shared.read(work);
            Some((batches, shared))
        })
        .flatten()
        .boxed()
    }

    /// The batches one unit of work produces.
    fn read(&self, work: Work) -> BoxStream<'static, Result<RecordBatch>> {
        match (work, &self.read) {
            (
                Work::Grid(subset),
                ReadKind::Grid {
                    arrays,
                    dims,
                    schema,
                },
            ) => {
                let arrays = arrays.clone();
                let dims = dims.clone();
                let schema = schema.clone();
                let masks = self.dim_masks.clone();
                let flat = matches!(self.mode, ReadMode::Flat(_));
                futures::stream::once(async move {
                    if flat {
                        // `read_chunk` applies the masks itself, and returns
                        // `None` for a chunk they exclude.
                        return read_chunk(&arrays, subset, schema, &dims, &masks)
                            .await
                            .map_err(|e| DataFusionError::Execution(e.to_string()));
                    }
                    // The nd path prunes on the same masks, and has to do it
                    // here: a chunk no row of which can meet the predicate is
                    // dropped before it is fetched, which is the whole saving.
                    // The predicate is applied again above the scan, so this
                    // only ever skips rows that would have been dropped there.
                    if chunk_is_pruned(&masks, &dims, &subset) {
                        return Ok(None);
                    }
                    let nd = read_nd_chunk(&arrays, &dims, schema, subset).await?;
                    beacon_datafusion_ext::nd::encode_nd_record_batch(&nd).map(Some)
                })
                .filter_map(|batch| futures::future::ready(batch.transpose()))
                .boxed()
            }
            (Work::Ragged(range), ReadKind::Ragged { plan }) => {
                let plan = plan.clone();
                // A ragged read is flat already, and its plan applied the
                // predicate when it chose which casts survive, so there is
                // nothing left to prune here.
                let encode = matches!(self.mode, ReadMode::Encoded(_));
                futures::stream::once(async move {
                    let flat = read_ragged_range(&plan, range)
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    if encode {
                        beacon_datafusion_ext::nd::encode_flat_batch_as_nd(&flat)
                    } else {
                        Ok(flat)
                    }
                })
                .boxed()
            }
            // `build` pairs the unit with the kind, so a mismatch would be a bug
            // in this file rather than bad input.
            _ => futures::stream::once(async {
                Err(DataFusionError::Internal(
                    "nd share: work unit does not match the dataset it came from".to_string(),
                ))
            })
            .boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::TryStreamExt;

    use super::*;
    use crate::NdArray;
    use crate::dataset::Dataset;

    /// A dataset of `rows` values on one dimension.
    async fn dataset(rows: usize) -> AnyDataset {
        let values = NdArray::<i64>::try_new_from_vec_in_mem(
            (0..rows as i64).collect(),
            vec![rows],
            vec!["row".to_string()],
            None,
        )
        .unwrap();

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("value".to_string(), Arc::new(values));
        AnyDataset::Regular(Dataset::new("shared".to_string(), arrays).await)
    }

    /// A CF contiguous ragged-array dataset of `casts` casts, each holding one
    /// more observation than the last.
    ///
    /// Uneven on purpose: a plan over equal casts would divide the same way
    /// whatever the batch size, and hide a mistake in the mapping back to the
    /// dataset's own indices.
    async fn ragged_dataset(casts: usize) -> (AnyDataset, usize) {
        let sizes: Vec<i32> = (1..=casts as i32).collect();
        let observations: usize = sizes.iter().map(|size| *size as usize).sum();

        let row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            sizes,
            vec![casts],
            vec!["casts".to_string()],
            None,
        )
        .unwrap();
        // The attribute that marks the dataset as ragged and names the
        // observation dimension `row_size` counts into.
        let sample_dimension = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["obs".to_string()],
            vec![],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let station = NdArray::<f64>::try_new_from_vec_in_mem(
            (0..casts).map(|cast| cast as f64).collect(),
            vec![casts],
            vec!["casts".to_string()],
            None,
        )
        .unwrap();
        let temperature = NdArray::<f64>::try_new_from_vec_in_mem(
            (0..observations).map(|obs| obs as f64 * 0.5).collect(),
            vec![observations],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("row_size".to_string(), Arc::new(row_size));
        arrays.insert(
            "row_size.sample_dimension".to_string(),
            Arc::new(sample_dimension),
        );
        arrays.insert("station".to_string(), Arc::new(station));
        arrays.insert("temperature".to_string(), Arc::new(temperature));

        let dataset = Dataset::new("ragged".to_string(), arrays).await;
        let any = AnyDataset::try_from_dataset(dataset).await.unwrap();
        assert!(
            matches!(any, AnyDataset::Ragged { .. }),
            "fixture is ragged"
        );
        (any, observations)
    }

    /// Drain one partition's encoded stream into the rows it read.
    async fn drain(stream: BoxStream<'static, Result<RecordBatch>>) -> usize {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches
            .iter()
            .map(|batch| {
                beacon_datafusion_ext::nd::decode_nd_record_batch(batch)
                    .unwrap()
                    .num_rows()
            })
            .sum()
    }

    /// Drain a flat stream into the rows it read.
    ///
    /// A flat batch is already broadcast, so it needs no decoding. This is what
    /// the `COUNT(*)` path counts.
    async fn drain_flat(stream: BoxStream<'static, Result<RecordBatch>>) -> usize {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Run `partitions` partitions over one read, and return the rows each one
    /// took. This is the shape the opener runs: every partition of a file holds
    /// the same `Arc` and streams from it.
    async fn read_in_partitions(
        shared: Arc<SharedRead>,
        partitions: usize,
        encoded: bool,
    ) -> Vec<usize> {
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..partitions {
            let shared = shared.clone();
            set.spawn(async move {
                if encoded {
                    drain(shared.stream()).await
                } else {
                    drain_flat(shared.stream()).await
                }
            });
        }

        let mut rows = Vec::new();
        while let Some(read) = set.join_next().await {
            rows.push(read.expect("a partition finishes"));
        }
        rows
    }

    // ── pruning on the predicate ───────────────────────────────────────

    /// `value > threshold`, as the scan would push it down.
    fn greater_than(column: &str, threshold: i64) -> PushdownFilter {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;

        PushdownFilter::new(Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column, 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(threshold)))),
        )) as Arc<dyn PhysicalExpr>)
    }

    /// Every value an encoded read returned, decoded and broadcast.
    async fn values_read(stream: BoxStream<'static, Result<RecordBatch>>) -> Vec<i64> {
        use arrow::array::Int64Array;

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut values = Vec::new();
        for batch in &batches {
            let flat = beacon_datafusion_ext::nd::decode_nd_record_batch(batch)
                .unwrap()
                .materialize()
                .unwrap();
            let column = flat
                .column_by_name("value")
                .expect("the fixture has one column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an i64 column")
                .clone();
            values.extend(column.iter().flatten());
        }
        values
    }

    /// The nd read skips chunks the predicate excludes, and keeps every row it
    /// wants.
    ///
    /// Both halves matter and neither implies the other. Reading everything is
    /// correct but pointless; reading less is pointless if it drops a row the
    /// query asked for, and nothing about that raises an error — the chunk is
    /// simply never fetched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_nd_read_skips_the_chunks_the_predicate_excludes() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const THRESHOLD: i64 = 8_000;

        let whole = SharedRead::build(dataset(ROWS).await, BATCH, ReadMode::Encoded(None))
            .await
            .expect("the read builds");
        let chunks = whole.remaining();
        let all = values_read(whole.stream()).await;
        assert_eq!(all.len(), ROWS, "the unfiltered read returns the file");

        let pruned = SharedRead::build(
            dataset(ROWS).await,
            BATCH,
            ReadMode::Encoded(Some(greater_than("value", THRESHOLD))),
        )
        .await
        .expect("the read builds");
        assert_eq!(
            pruned.remaining(),
            chunks,
            "the queue still holds every chunk; a chunk is dropped when it is popped"
        );
        let kept = values_read(pruned.stream()).await;

        // The fixture counts up, so a chunk below the threshold holds nothing
        // the query wants. Fifteen of the twenty chunks are entirely below it.
        assert!(
            kept.len() < ROWS,
            "the read must skip the chunks under the threshold, it returned all {ROWS} rows"
        );

        // Nothing the predicate keeps may go missing. The read is allowed to
        // return more than that — it skips whole chunks, not rows — and the
        // scan applies the predicate again above.
        let wanted: Vec<i64> = all.iter().copied().filter(|v| *v > THRESHOLD).collect();
        let returned: std::collections::HashSet<i64> = kept.iter().copied().collect();
        assert!(
            wanted.iter().all(|value| returned.contains(value)),
            "the read dropped a row the predicate keeps"
        );
    }

    /// The flat read and the nd read skip the same chunks.
    ///
    /// `COUNT(*)` goes one way and a column read the other. They must agree
    /// about which chunks hold nothing, or a count stops matching its own rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn both_modes_skip_the_same_chunks() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const THRESHOLD: i64 = 8_000;

        let nd = SharedRead::build(
            dataset(ROWS).await,
            BATCH,
            ReadMode::Encoded(Some(greater_than("value", THRESHOLD))),
        )
        .await
        .expect("the read builds");
        let nd_rows = drain(nd.stream()).await;

        let flat = SharedRead::build(
            dataset(ROWS).await,
            BATCH,
            ReadMode::Flat(Some(greater_than("value", THRESHOLD))),
        )
        .await
        .expect("the read builds");
        let flat_rows = drain_flat(flat.stream()).await;

        assert_eq!(nd_rows, flat_rows, "the two modes read the same chunks");
        assert!(
            nd_rows > 0 && nd_rows < ROWS,
            "and they pruned some of them"
        );
    }

    /// A predicate on a column the file does not bound prunes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unrelated_predicate_reads_the_whole_file() {
        const ROWS: usize = 4_000;

        let shared = SharedRead::build(
            dataset(ROWS).await,
            512,
            ReadMode::Encoded(Some(greater_than("no_such_column", 10))),
        )
        .await
        .expect("the read builds");

        assert_eq!(drain(shared.stream()).await, ROWS);
    }

    /// The partitions of one file read every row once between them.
    ///
    /// This is the property the whole design rests on. A subset popped by two
    /// partitions is a row returned twice, and one popped by none is a row lost,
    /// and neither raises an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_share_read_every_row_once() {
        const ROWS: usize = 10_000;

        for partitions in [1_usize, 2, 3, 8] {
            let shared = SharedRead::build(dataset(ROWS).await, 512, ReadMode::Encoded(None))
                .await
                .expect("the read builds");
            let rows = read_in_partitions(shared, partitions, true).await;

            assert_eq!(
                rows.iter().sum::<usize>(),
                ROWS,
                "partitions={partitions}: every row is read exactly once"
            );
        }
    }

    /// The partitions of a file count its rows once between them.
    ///
    /// `COUNT(*)` reads through the flat mode, which never decodes an nd array
    /// and so cannot inherit the division from the decode. A file every
    /// partition holds would be counted once per partition if this mode did not
    /// take its work from the same queue: the answer would grow with
    /// `target_partitions`, silently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_share_count_its_rows_once() {
        const ROWS: usize = 10_000;

        for partitions in [1_usize, 2, 4, 8] {
            let shared = SharedRead::build(dataset(ROWS).await, 512, ReadMode::Flat(None))
                .await
                .expect("the read builds");
            let counted = read_in_partitions(shared, partitions, false).await;

            assert_eq!(
                counted.iter().sum::<usize>(),
                ROWS,
                "partitions={partitions}: every row counted exactly once"
            );
        }
    }

    /// Several partitions draw from one file's queue, and each gets its own
    /// work.
    ///
    /// Every partition takes a batch before any of them drains, so what is
    /// asserted is the division and not the scheduling. Letting them race and
    /// counting afterwards says nothing: a queue this small is often emptied by
    /// whichever partition is polled first, and that is correct behaviour.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_shared_file_divides_across_partitions() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const PARTITIONS: usize = 4;

        let shared = SharedRead::build(dataset(ROWS).await, BATCH, ReadMode::Encoded(None))
            .await
            .expect("the read builds");
        let chunks = shared.remaining();
        assert!(
            chunks >= PARTITIONS,
            "the fixture must hold at least one chunk per partition, it holds {chunks}"
        );

        let mut streams: Vec<_> = (0..PARTITIONS).map(|_| shared.clone().stream()).collect();
        let mut taken = 0;
        for (partition, stream) in streams.iter_mut().enumerate() {
            let batch = stream
                .next()
                .await
                .unwrap_or_else(|| panic!("partition {partition} gets a chunk of its own"))
                .expect("it reads");
            taken += beacon_datafusion_ext::nd::decode_nd_record_batch(&batch)
                .unwrap()
                .num_rows();
        }
        assert_eq!(
            shared.remaining(),
            chunks - PARTITIONS,
            "each partition took one chunk, and no chunk went to two of them"
        );

        // The rest divides between them, and between them they read the file.
        let mut rest = 0;
        for stream in streams {
            rest += drain(stream).await;
        }
        assert_eq!(taken + rest, ROWS, "every row is read exactly once");
    }

    /// The partitions of a ragged file read every observation once between them.
    ///
    /// A ragged dataset has no chunk grid, so its queue holds ranges of its
    /// batch plan instead. The property is the same and so is the risk: a range
    /// popped twice is an observation returned twice, and one popped by nobody
    /// is an observation lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_ragged_share_read_every_row_once() {
        const CASTS: usize = 60;

        // A batch size well under the file, so the plan holds many ranges to
        // divide, and one over it, so it holds one.
        for batch_size in [8_usize, 64, usize::MAX] {
            for partitions in [1_usize, 2, 3, 8] {
                let (source, observations) = ragged_dataset(CASTS).await;
                let shared = SharedRead::build(source, batch_size, ReadMode::Encoded(None))
                    .await
                    .expect("the plan builds");
                let rows = read_in_partitions(shared, partitions, true).await;

                assert_eq!(
                    rows.iter().sum::<usize>(),
                    observations,
                    "batch_size={batch_size} partitions={partitions}: every row once"
                );
            }
        }
    }

    /// A ragged file counts its rows once too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_ragged_share_count_its_rows_once() {
        const CASTS: usize = 60;
        const PARTITIONS: usize = 4;

        let (source, observations) = ragged_dataset(CASTS).await;
        let shared = SharedRead::build(source, 8, ReadMode::Flat(None))
            .await
            .expect("the plan builds");
        let counted = read_in_partitions(shared, PARTITIONS, false).await;

        assert_eq!(
            counted.iter().sum::<usize>(),
            observations,
            "every observation counted once"
        );
    }

    /// A ragged file divides across partitions rather than falling to one.
    ///
    /// The queue used to hold a ragged file as a single unit, so one partition
    /// read it and the rest found the queue empty. This is the assertion that
    /// says it no longer does — and, as above, it takes a batch per partition
    /// up front so that it asserts the division rather than the scheduling.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_ragged_file_divides_across_partitions() {
        const CASTS: usize = 60;
        const PARTITIONS: usize = 4;

        let (source, observations) = ragged_dataset(CASTS).await;
        let shared = SharedRead::build(source, 8, ReadMode::Encoded(None))
            .await
            .expect("the plan builds");
        let ranges = shared.remaining();
        assert!(
            ranges >= PARTITIONS,
            "the fixture must hold at least one range per partition, got {ranges}"
        );

        let mut streams: Vec<_> = (0..PARTITIONS).map(|_| shared.clone().stream()).collect();
        let mut taken = 0;
        for (partition, stream) in streams.iter_mut().enumerate() {
            let batch = stream
                .next()
                .await
                .unwrap_or_else(|| panic!("partition {partition} gets a range of its own"))
                .expect("it reads");
            taken += beacon_datafusion_ext::nd::decode_nd_record_batch(&batch)
                .unwrap()
                .num_rows();
        }
        assert_eq!(
            shared.remaining(),
            ranges - PARTITIONS,
            "each partition took one range, and no range went to two of them"
        );

        let mut rest = 0;
        for stream in streams {
            rest += drain(stream).await;
        }
        assert_eq!(taken + rest, observations, "every row is read exactly once");
    }

    /// More partitions than subsets is not an error. The surplus find the queue
    /// empty and finish at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partition_with_nothing_left_to_pop_just_finishes() {
        const ROWS: usize = 100;

        // One chunk, eight partitions.
        let shared = SharedRead::build(dataset(ROWS).await, usize::MAX, ReadMode::Encoded(None))
            .await
            .expect("the read builds");
        let rows = read_in_partitions(shared, 8, true).await;

        assert_eq!(rows.iter().sum::<usize>(), ROWS);
        assert_eq!(
            rows.iter().filter(|read| **read > 0).count(),
            1,
            "one chunk is read by one partition, and the rest read nothing"
        );
    }

    /// A partition that leaves early leaves its work for the others.
    ///
    /// Nothing is reserved up front, so a dropped stream costs only the subsets
    /// it had already popped. A `LIMIT` that stops one partition mid-file must
    /// not take rows away from the rest.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn work_a_partition_does_not_take_is_left_for_the_others() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 500;

        let shared = SharedRead::build(dataset(ROWS).await, BATCH, ReadMode::Encoded(None))
            .await
            .expect("the read builds");

        // One partition takes a single batch and leaves.
        let mut early = shared.clone().stream();
        let first = early.next().await.expect("one batch").expect("it reads");
        let taken = beacon_datafusion_ext::nd::decode_nd_record_batch(&first)
            .unwrap()
            .num_rows();
        drop(early);

        // The other reads what is left, and between them that is the file.
        let rest = drain(shared.stream()).await;
        assert_eq!(
            taken + rest,
            ROWS,
            "the subsets the leaver never popped are still there"
        );
    }
}
