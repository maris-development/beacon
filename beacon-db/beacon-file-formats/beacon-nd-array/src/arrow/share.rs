use std::sync::Arc;

use arrow::array::RecordBatchOptions;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use crossbeam::queue::ArrayQueue;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{BatchAdapter, BatchAdapterFactory};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use indexmap::IndexMap;
use tokio::sync::OnceCell;

use crate::NdArrayD;
use crate::projection::DatasetProjection;
use std::ops::Range;

use crate::array::subset::ArraySubset;
use crate::arrow::batch::{
    ChunkGrid, RaggedPlan, build_dataset_schema, chunk_grid, chunk_is_pruned,
    compute_predicate_masks, plan_ragged_read, read_chunk, read_ragged_range,
};
use crate::arrow::metrics::SharedReadMetrics;
use crate::arrow::nd_provider::read_nd_chunk;
use crate::arrow::pushdown_filter::PushdownFilter;
use crate::dataset::AnyDataset;

/// What the scan wants out of a file, and so what a batch off the queue becomes.
///
/// A scan is one or the other throughout: the first partition to arrive decides,
/// and every partition of a scan would decide the same. The decision reaches
/// down to the read itself, because the two want different batches — a column
/// read wants `beacon.nd`-encoded chunks for the `NdSourceExec` above it to
/// decode, and `COUNT(*)` wants flat ones it can take a row count off.
#[derive(Debug)]
enum Output {
    /// Columns: nd-encoded batches, reordered and null-filled onto the
    /// projected schema.
    Columns(Arc<BatchAdapter>),
    /// `COUNT(*)`: flat batches, of which only the row count leaves, under the
    /// (empty) projected schema. See [`count_projection`].
    Rows(SchemaRef),
}

impl Output {
    /// Whether the read encodes its chunks, rather than broadcasting them flat.
    fn encoded(&self) -> bool {
        matches!(self, Output::Columns(_))
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
///
/// The queue holds only what the query needs: [`SharedRead::build`] applies the
/// predicate as it fills it, so every unit in here is a read that will produce
/// rows.
#[derive(Debug)]
pub struct SharedRead {
    queue: ArrayQueue<Work>,
    read: ReadKind,
    /// Whether a chunk leaves nd-encoded. See [`Output`].
    encoded: bool,
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
    /// Open `dataset` for sharing, and fill the queue with the subsets that are
    /// worth reading.
    ///
    /// A regular dataset is cut on its chunk grid, which is the same grid an
    /// unshared read walks. A ragged one is cut on its batch plan, which is the
    /// same plan an unshared read builds.
    ///
    /// # The predicate is applied here, not later
    ///
    /// The coordinate arrays are read once, before the queue is filled, and a
    /// chunk no row of which can meet the predicate never enters it. So the
    /// queue holds the work the query actually needs, and the partitions divide
    /// *that*.
    ///
    /// Filtering as each chunk is popped would divide the file evenly and the
    /// work unevenly: one partition can draw a run of chunks that are all
    /// excluded and finish having read nothing, while another reads everything
    /// the query wanted. It would also leave `remaining` counting work that does
    /// not exist.
    ///
    /// A ragged dataset already works this way: `plan_ragged_read` applies the
    /// predicate when it chooses which casts survive, so its plan holds only the
    /// batches that have something in them.
    async fn build(
        dataset: AnyDataset,
        batch_size: usize,
        predicate: Option<PushdownFilter>,
        encoded: bool,
        metrics: Option<&SharedReadMetrics>,
    ) -> Result<Arc<Self>> {
        let regular = match dataset {
            AnyDataset::Regular(regular) => regular,
            AnyDataset::Ragged { ragged, .. } => {
                let plan = plan_ragged_read(ragged, batch_size, predicate)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                // Casts the predicate excluded before the plan existed. No batch
                // can account for them, so they are counted here.
                if let (Some(metrics), Some((casts, rows))) = (metrics, plan.pruned) {
                    metrics.chunks_pruned.add(casts);
                    metrics.rows_pruned.add(rows);
                }

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
                    encoded,
                }));
            }
        };

        let ChunkGrid { dims, chunks } = chunk_grid(&regular, batch_size)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let arrays = Arc::new(regular.arrays);
        let schema = build_dataset_schema(&arrays);

        // Read once for the file, before anything is queued. An unshared read
        // computes these per partition, which reads the coordinate arrays once
        // per partition.
        let dim_masks = compute_predicate_masks(&arrays, predicate)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // Only the chunks that can hold a row the query wants. The predicate is
        // applied again above the scan, so dropping one here only drops rows
        // that would have been dropped there.
        let (wanted, pruned): (Vec<ArraySubset>, Vec<ArraySubset>) = chunks
            .into_iter()
            .partition(|subset| !chunk_is_pruned(&dim_masks, &dims, subset));

        if let Some(metrics) = metrics {
            metrics.chunks_pruned.add(pruned.len());
            metrics
                .rows_pruned
                .add(pruned.iter().map(|subset| subset.rows()).sum::<usize>());
        }

        // A dataset with no chunks left still needs a queue, and `ArrayQueue`
        // will not take a capacity of zero.
        let queue = ArrayQueue::new(wanted.len().max(1));
        for subset in wanted {
            // The capacity is the chunk count, so this cannot fail.
            let _ = queue.push(Work::Grid(subset));
        }

        Ok(Arc::new(Self {
            queue,
            read: ReadKind::Grid {
                arrays,
                dims: Arc::new(dims),
                schema,
            },
            encoded,
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
    fn stream(
        self: Arc<Self>,
        metrics: Option<SharedReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::unfold((self, metrics), |(shared, metrics)| async move {
            let work = shared.queue.pop()?;
            let batches = shared.read(work, metrics.clone());
            Some((batches, (shared, metrics)))
        })
        .flatten()
        .boxed()
    }

    /// The batches one unit of work produces.
    fn read(
        &self,
        work: Work,
        metrics: Option<SharedReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
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
                let flat = !self.encoded;
                // The rows this chunk holds, as the scan will broadcast them.
                // An encoded batch carries the lot in one row, so counting the
                // batch would say nothing.
                if let Some(metrics) = &metrics {
                    metrics.chunks_read.add(1);
                    metrics.rows_read.add(subset.rows());
                }
                futures::stream::once(async move {
                    // No masks here: `build` applied them when it filled the
                    // queue, so this chunk is one the query wants. Passing them
                    // again would rebuild the same chunk mask per read and
                    // always come to the same answer.
                    if flat {
                        return read_chunk(&arrays, subset, schema, &dims, &[])
                            .await
                            .map_err(|e| DataFusionError::Execution(e.to_string()));
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
                let encode = self.encoded;
                futures::stream::once(async move {
                    let flat = read_ragged_range(&plan, range)
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    if let Some(metrics) = &metrics {
                        metrics.chunks_read.add(1);
                        metrics.rows_read.add(flat.num_rows());
                    }
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

/// The shares of one scan, keyed by object path.
///
/// A file in here is in *every* partition's file group, so it is read through
/// its share and no other way: reading it whole would return every row once per
/// partition. [`share_files`] builds the map and the groups together, so the two
/// cannot disagree.
pub type FileShares =
    Arc<std::collections::HashMap<object_store::path::Path, Arc<OnceCell<Arc<SharedDataset>>>>>;

/// How a scan's files are dealt to its partitions, and which of them are shared.
pub struct SharedScan {
    /// One group per partition, in partition order.
    pub file_groups: Vec<FileGroup>,
    /// The share of each file that landed in every group.
    pub shares: FileShares,
}

/// Give every partition the files worth sharing, and one each of the rest.
///
/// A file worth sharing goes into *every* partition's group and gets a share.
/// Nothing about it is divided here: the partitions divide it as they read it,
/// by taking chunks from the one queue behind that share. Balance follows
/// completion rather than a guess made at plan time, which matters most under a
/// predicate — an nd chunk list is C-ordered, so `WHERE time > …` prunes a
/// prefix of it, and a deal made at plan time would leave the early partitions
/// idle.
///
/// `min_share_size` says which files those are. `Some(size)` leaves a file at or
/// under it whole, dealt to one partition: every partition opening a small file
/// to take a chunk or two would cost more than it returns, and the listing has
/// already spread these across the scan. The test is on one file, not on the
/// scan total — a share pays to open a file, so a file is what has to earn it,
/// and a thousand small files are not one large one.
///
/// `None` shares every file. That is for a format whose object is not its data:
/// a zarr group's object is a `zarr.json` of a few KB that can front terabytes
/// of chunks, so any size test on it measures the wrong thing and would decline
/// every store, however large.
///
/// Returns `None` when no file is worth sharing, which leaves the scan's
/// grouping alone.
pub fn share_files(
    file_groups: &[FileGroup],
    target_partitions: usize,
    min_share_size: Option<u64>,
) -> Option<SharedScan> {
    if target_partitions <= 1 {
        return None;
    }

    let (shared, whole): (Vec<_>, Vec<_>) = file_groups
        .iter()
        .flat_map(FileGroup::iter)
        .cloned()
        .partition(|file| match min_share_size {
            None => true,
            Some(min) => {
                let (start, end) = file.range();
                end.saturating_sub(start) > min
            }
        });
    if shared.is_empty() {
        return None;
    }

    let shares = shared
        .iter()
        .map(|file| (file.object_meta.location.clone(), Arc::new(OnceCell::new())))
        .collect();

    // Every partition starts with every shared file, and opens it first: a
    // partition that reaches the share sooner starts drawing from its queue
    // sooner. The rest are spread one per partition, as the listing would have.
    let mut groups = vec![shared; target_partitions];
    for (index, file) in whole.into_iter().enumerate() {
        groups[index % target_partitions].push(file);
    }

    Some(SharedScan {
        file_groups: groups.into_iter().map(FileGroup::new).collect(),
        shares: Arc::new(shares),
    })
}

/// One file, opened and planned once: the queue its partitions draw from, and
/// what a batch off that queue becomes.
///
/// This is what a file-format opener puts behind its share. A shared file builds
/// one of these for all of its partitions; an unshared file builds one for the
/// single partition that holds it, which is the same work without the sharing.
///
/// Every format that reads through the nd pipeline plans a file the same way, so
/// the planning lives here rather than three times over. What differs between
/// them — how a file is opened, which dimensions it reads on — happens before
/// this and is handed in as an [`AnyDataset`].
#[derive(Debug)]
pub struct SharedDataset {
    read: Arc<SharedRead>,
    output: Output,
}

impl SharedDataset {
    /// Plan `dataset` for a scan that wants `projected_schema`.
    ///
    /// Resolves the projection, fills the queue, and decides what a batch off it
    /// becomes. `predicate` is a hint: it prunes chunks that cannot hold a row
    /// the query wants, and the scan is expected to apply it again above.
    ///
    /// `metrics` belong to the partition that plans. They take the counts made
    /// here rather than per chunk: a chunk the predicate excluded is dropped
    /// before the queue exists, so no reader of the queue can account for it.
    pub async fn plan(
        dataset: AnyDataset,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: Option<&SharedReadMetrics>,
    ) -> Result<Arc<Self>> {
        let dataset_schema: SchemaRef = Arc::new(
            crate::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to derive an Arrow schema from the dataset: {e}"
                ))
            })?,
        );

        // The columns of this file the query needs, in file order.
        let projection: Vec<usize> = dataset_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| projected_schema.index_of(field.name()).is_ok())
            .map(|(index, _)| index)
            .collect();

        let pushdown = predicate.clone().map(PushdownFilter::new);

        // No column is wanted, so this is `COUNT(*)`: the read is driven by
        // columns of its own and only the row counts leave.
        let (output, projection) = if projection.is_empty() {
            let counted = count_projection(&dataset, &dataset_schema, &predicate);
            (Output::Rows(projected_schema), counted)
        } else {
            // The scan carries nd columns, so adaptation happens in the encoded
            // (struct) domain: reorder and null-fill onto the projected schema.
            let source_schema: SchemaRef = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
                &dataset_schema.project(&projection)?,
            ));
            let adapter =
                BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;
            (Output::Columns(Arc::new(adapter)), projection)
        };

        let dataset = project(dataset, &dataset_schema, projection)?;
        let read =
            SharedRead::build(dataset, batch_size, pushdown, output.encoded(), metrics).await?;

        Ok(Arc::new(Self { read, output }))
    }

    /// How much of the file is left to read. For tests and diagnostics.
    pub fn remaining(&self) -> usize {
        self.read.remaining()
    }

    /// One partition's stream over the file.
    ///
    /// Every partition of a shared file calls this on the same `SharedDataset`,
    /// and they draw from the one queue behind it. `metrics` are this
    /// partition's, so what each one read is what it reports.
    pub fn stream(
        &self,
        metrics: Option<SharedReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let batches = self.read.clone().stream(metrics);
        match &self.output {
            Output::Columns(adapter) => {
                let adapter = adapter.clone();
                batches
                    .and_then(move |batch| {
                        let adapted = adapter.adapt_batch(&batch).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to adapt the batch onto the scan's schema: {e}"
                            ))
                        });
                        futures::future::ready(adapted)
                    })
                    .boxed()
            }
            Output::Rows(schema) => {
                let schema = schema.clone();
                batches
                    .and_then(move |batch| {
                        let counted = RecordBatch::try_new_with_options(
                            schema.clone(),
                            vec![],
                            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                        )
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to build a count batch: {e}"
                            ))
                        });
                        futures::future::ready(counted)
                    })
                    .boxed()
            }
        }
    }
}

/// The columns a `COUNT(*)` reads, out of a file the query wants no column of.
///
/// Reading no column at all would give an empty stream and a count of zero. The
/// read is driven by the widest variable instead, so the row count is the full
/// broadcast row count — a scalar attribute like `.Conventions` would give one
/// row — plus any column the predicate names, so a pushed-down filter still
/// applies ([`PushdownFilter`] matches by name).
fn count_projection(
    dataset: &AnyDataset,
    dataset_schema: &SchemaRef,
    predicate: &Option<Arc<dyn PhysicalExpr>>,
) -> Vec<usize> {
    let driver = dataset
        .fields()
        .keys()
        .max_by_key(|name| {
            dataset
                .get_array(name)
                .map(|array| array.shape().iter().product::<usize>())
                .unwrap_or(0)
        })
        .and_then(|name| dataset_schema.index_of(name).ok())
        .unwrap_or(0);

    let mut projection = vec![driver];
    if let Some(predicate) = predicate {
        for column in datafusion::physical_expr::utils::collect_columns(predicate) {
            if let Ok(index) = dataset_schema.index_of(column.name()) {
                projection.push(index);
            }
        }
    }
    projection.sort_unstable();
    projection.dedup();
    projection
}

/// Keep only `projection` of `dataset`, or all of it when that is everything.
fn project(
    dataset: AnyDataset,
    dataset_schema: &SchemaRef,
    projection: Vec<usize>,
) -> Result<AnyDataset> {
    if projection.len() == dataset_schema.fields().len() {
        return Ok(dataset);
    }
    dataset
        .project(&DatasetProjection {
            dimension_projection: None,
            index_projection: Some(projection),
        })
        .map_err(|e| DataFusionError::Execution(format!("Failed to project the dataset: {e}")))
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
                    drain(shared.stream(None)).await
                } else {
                    drain_flat(shared.stream(None)).await
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

    /// The queue holds only the chunks the predicate keeps, and keeps every row
    /// the query wants.
    ///
    /// Both halves matter and neither implies the other. Reading everything is
    /// correct but pointless; reading less is pointless if it drops a row the
    /// query asked for, and nothing about that raises an error — the chunk is
    /// simply never fetched.
    ///
    /// The queue length is the first assertion because it is where the saving
    /// is. A queue still holding the excluded chunks would give a partition a
    /// run of work that turns out to be nothing, while another partition reads
    /// everything the query wanted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_queue_holds_only_the_chunks_the_predicate_keeps() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const THRESHOLD: i64 = 8_000;

        let whole = SharedRead::build(dataset(ROWS).await, BATCH, None, true, None)
            .await
            .expect("the read builds");
        let chunks = whole.remaining();
        let all = values_read(whole.stream(None)).await;
        assert_eq!(all.len(), ROWS, "the unfiltered read returns the file");

        let pruned = SharedRead::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            true,
            None,
        )
        .await
        .expect("the read builds");

        // The fixture counts up, so a chunk below the threshold holds nothing
        // the query wants, and it is left out before any partition can draw it.
        let queued = pruned.remaining();
        assert!(
            queued > 0 && queued < chunks,
            "the queue should hold some of the {chunks} chunks, it holds {queued}"
        );

        // The rows read come from the queued chunks and no others. The last
        // chunk of the file is short, so this is a bound rather than a product.
        let kept = values_read(pruned.stream(None)).await;
        assert!(
            kept.len() <= queued * BATCH && kept.len() > (queued - 1) * BATCH,
            "{} rows off {queued} chunks of at most {BATCH}",
            kept.len()
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

    /// A predicate no row can meet leaves an empty queue.
    ///
    /// The partitions then find nothing to do, which is the point: the file is
    /// never opened for reading at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_predicate_nothing_meets_queues_no_work() {
        const ROWS: usize = 10_000;

        let shared = SharedRead::build(
            dataset(ROWS).await,
            512,
            Some(greater_than("value", ROWS as i64 * 10)),
            true,
            None,
        )
        .await
        .expect("the read builds");

        assert_eq!(shared.remaining(), 0, "no chunk can hold a matching row");
        assert_eq!(drain(shared.stream(None)).await, 0);
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
            Some(greater_than("value", THRESHOLD)),
            true,
            None,
        )
        .await
        .expect("the read builds");
        let nd_rows = drain(nd.stream(None)).await;

        let flat = SharedRead::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            false,
            None,
        )
        .await
        .expect("the read builds");
        let flat_rows = drain_flat(flat.stream(None)).await;

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
            Some(greater_than("no_such_column", 10)),
            true,
            None,
        )
        .await
        .expect("the read builds");

        assert_eq!(drain(shared.stream(None)).await, ROWS);
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
            let shared = SharedRead::build(dataset(ROWS).await, 512, None, true, None)
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
            let shared = SharedRead::build(dataset(ROWS).await, 512, None, false, None)
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

        let shared = SharedRead::build(dataset(ROWS).await, BATCH, None, true, None)
            .await
            .expect("the read builds");
        let chunks = shared.remaining();
        assert!(
            chunks >= PARTITIONS,
            "the fixture must hold at least one chunk per partition, it holds {chunks}"
        );

        let mut streams: Vec<_> = (0..PARTITIONS)
            .map(|_| shared.clone().stream(None))
            .collect();
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
                let shared = SharedRead::build(source, batch_size, None, true, None)
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
        let shared = SharedRead::build(source, 8, None, false, None)
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
        let shared = SharedRead::build(source, 8, None, true, None)
            .await
            .expect("the plan builds");
        let ranges = shared.remaining();
        assert!(
            ranges >= PARTITIONS,
            "the fixture must hold at least one range per partition, got {ranges}"
        );

        let mut streams: Vec<_> = (0..PARTITIONS)
            .map(|_| shared.clone().stream(None))
            .collect();
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
        let shared = SharedRead::build(dataset(ROWS).await, usize::MAX, None, true, None)
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

        let shared = SharedRead::build(dataset(ROWS).await, BATCH, None, true, None)
            .await
            .expect("the read builds");

        // One partition takes a single batch and leaves.
        let mut early = shared.clone().stream(None);
        let first = early.next().await.expect("one batch").expect("it reads");
        let taken = beacon_datafusion_ext::nd::decode_nd_record_batch(&first)
            .unwrap()
            .num_rows();
        drop(early);

        // The other reads what is left, and between them that is the file.
        let rest = drain(shared.stream(None)).await;
        assert_eq!(
            taken + rest,
            ROWS,
            "the subsets the leaver never popped are still there"
        );
    }
}

#[cfg(test)]
mod deal_tests {
    use datafusion::datasource::listing::PartitionedFile;
    use object_store::path::Path;

    use super::*;

    const MIN: u64 = 8 * 1024 * 1024;

    /// A file worth sharing lands in every partition, and gets a share.
    #[test]
    fn a_shared_file_lands_in_every_partition() {
        const PARTITIONS: usize = 4;

        let groups = vec![FileGroup::new(vec![PartitionedFile::new(
            "large.nc",
            64 * 1024 * 1024,
        )])];

        let scan = share_files(&groups, PARTITIONS, Some(MIN)).expect("a large file is shared");

        assert_eq!(scan.file_groups.len(), PARTITIONS);
        for group in &scan.file_groups {
            assert_eq!(group.len(), 1, "each partition holds the file once");
            assert!(
                group.iter().next().unwrap().range.is_none(),
                "a shared file is not divided"
            );
        }
        assert!(
            scan.shares.contains_key(&Path::from("large.nc")),
            "the file the partitions share must have a share"
        );
        assert_eq!(scan.shares.len(), 1);
    }

    /// Small files are dealt one per partition, undivided and unshared.
    #[test]
    fn small_files_are_dealt_whole_and_unshared() {
        let mut files = vec![PartitionedFile::new("large.nc", 64 * 1024 * 1024)];
        files.extend((0..4).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024)));

        let scan =
            share_files(&[FileGroup::new(files)], 4, Some(MIN)).expect("the large file is shared");

        let small: Vec<_> = scan
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .filter(|file| file.object_meta.location.as_ref().starts_with("small-"))
            .collect();

        assert_eq!(small.len(), 4, "each small file appears once in the scan");
        for file in small {
            assert!(file.range.is_none(), "a whole file is not divided");
        }
        assert_eq!(scan.shares.len(), 1, "only the large file is shared");
    }

    /// Every partition opens the shared file before its own small files.
    #[test]
    fn the_shared_file_comes_first_in_every_group() {
        let mut files = vec![PartitionedFile::new("large.nc", 64 * 1024 * 1024)];
        files.extend((0..8).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024)));

        let scan =
            share_files(&[FileGroup::new(files)], 4, Some(MIN)).expect("the large file is shared");

        for group in &scan.file_groups {
            let first = group.iter().next().expect("a group holds the shared file");
            assert_eq!(
                first.object_meta.location,
                Path::from("large.nc"),
                "the shared file is opened first"
            );
        }
    }

    /// A scan with nothing worth sharing is left alone.
    ///
    /// This is where the rule departs from DataFusion's, which tests the scan
    /// total: 200 MB of 1 MB files clears any total-based minimum, and every one
    /// of them would be opened by every partition for a fraction of its rows. A
    /// share pays to open one file, so one file is what has to earn it.
    #[test]
    fn a_scan_with_no_large_file_is_left_alone() {
        let files: Vec<_> = (0..200)
            .map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024 * 1024))
            .collect();

        assert!(
            share_files(&[FileGroup::new(files)], 4, Some(MIN)).is_none(),
            "200 MB of 1 MB files is still no file worth sharing"
        );
    }

    /// A single-partition scan shares nothing: there is nobody to share with.
    #[test]
    fn one_partition_is_left_alone() {
        let groups = vec![FileGroup::new(vec![PartitionedFile::new(
            "large.nc",
            64 * 1024 * 1024,
        )])];

        assert!(share_files(&groups, 1, Some(MIN)).is_none());
    }

    /// Several large files are each shared by every partition.
    #[test]
    fn every_large_file_is_shared_by_every_partition() {
        const PARTITIONS: usize = 3;

        let files: Vec<_> = (0..2)
            .map(|i| PartitionedFile::new(format!("large-{i}.nc"), 64 * 1024 * 1024))
            .collect();

        let scan = share_files(&[FileGroup::new(files)], PARTITIONS, Some(MIN))
            .expect("both files are shared");

        assert_eq!(scan.shares.len(), 2);
        for group in &scan.file_groups {
            assert_eq!(group.len(), 2, "every partition holds both files");
        }
    }
}
