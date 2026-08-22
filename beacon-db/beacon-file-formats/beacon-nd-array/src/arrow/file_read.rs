use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatchOptions};
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use crossbeam::queue::ArrayQueue;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{BatchAdapter, BatchAdapterFactory};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use indexmap::IndexMap;

use crate::NdArrayD;
use crate::projection::DatasetProjection;
use std::ops::Range;

use crate::array::subset::ArraySubset;
use crate::arrow::batch::{
    ChunkGrid, RaggedPlan, build_dataset_schema, chunk_grid, chunk_is_pruned,
    compute_predicate_masks, plan_ragged_read, read_chunk, read_ragged_range,
};
use crate::arrow::metrics::ReadMetrics;
use crate::arrow::nd_provider::read_nd_chunk;
use crate::arrow::partition::FilePartitions;
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
    Columns {
        adapter: Arc<BatchAdapter>,
        /// What the file's `PARTITIONED BY` columns are called, when the scan
        /// projects any.
        partition_fields: Vec<FieldRef>,
        /// Those columns, nd-encoded as rank-0 arrays. One value each, constant
        /// for the whole file, so they are built once here and appended to
        /// every batch of it. See [`FilePartitions`].
        partition_columns: Vec<ArrayRef>,
    },
    /// `COUNT(*)`: flat batches, of which only the row count leaves, under the
    /// (empty) projected schema. See [`count_projection`].
    ///
    /// A scan of nothing but partition columns comes here too. It wants no
    /// column of the file either, and the row count is what says how many times
    /// each partition value repeats.
    Rows {
        schema: SchemaRef,
        partitions: FilePartitions,
    },
    /// The file holds none of the columns the query projects, so it has nothing
    /// to contribute and is not read at all.
    Nothing,
}

impl Output {
    /// Whether the read encodes its chunks, rather than broadcasting them flat.
    fn encoded(&self) -> bool {
        matches!(self, Output::Columns { .. })
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
/// The queue holds only what the query needs: [`WorkQueue::build`] applies the
/// predicate as it fills it, so every unit in here is a read that will produce
/// rows.
#[derive(Debug)]
pub struct WorkQueue {
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

impl WorkQueue {
    /// Open `dataset` and fill the queue with the subsets worth reading.
    ///
    /// A regular dataset is cut on its chunk grid; a ragged one on its batch
    /// plan. Whichever partitions reach this file draw from the queue, and each
    /// subset in it is popped once, so no two of them read the same data.
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
    pub(crate) async fn build(
        dataset: AnyDataset,
        batch_size: usize,
        predicate: Option<PushdownFilter>,
        encoded: bool,
        metrics: Option<&ReadMetrics>,
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

        // Read once for the file, before anything is queued. Computing them per
        // work unit instead would read the coordinate arrays once per chunk.
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
    pub(crate) fn stream(
        self: Arc<Self>,
        metrics: Option<ReadMetrics>,
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
        metrics: Option<ReadMetrics>,
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
                    "nd read: work unit does not match the dataset it came from".to_string(),
                ))
            })
            .boxed(),
        }
    }
}

/// Read a whole dataset as flat, broadcast batches, in file order.
///
/// This is the [`WorkQueue`] a `COUNT(*)` builds, minus the counting: one
/// consumer, the whole queue, and the predicate pruning chunks it cannot use.
/// It is the only way to get flat batches out of a dataset, and it exists for
/// the readers' own tests — a scan goes through [`FileRead::plan`], which
/// resolves a projection and encodes.
pub async fn flat_stream(
    dataset: AnyDataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    Ok(
        WorkQueue::build(dataset, batch_size, predicate, false, None)
            .await?
            .stream(None),
    )
}

/// One file, opened and planned once: the [`WorkQueue`] of what is left to read
/// from it, and what a batch off that queue becomes.
///
/// This is what a format's [`OpenFile`](crate::arrow::morsel::OpenFile) returns,
/// and what a [`MorselSource`](crate::arrow::morsel::MorselSource) hands to its
/// workers. One is built per file, by whichever worker opens it, and every
/// worker that reaches that file afterwards draws from the same one — which is
/// what lets several of them finish a file together.
///
/// Every format that reads through the nd pipeline plans a file the same way, so
/// the planning lives here rather than four times over. What differs between
/// them — how a file is opened, which dimensions it reads on — happens before
/// this and is handed in as an [`AnyDataset`].
#[derive(Debug)]
pub struct FileRead {
    /// `None` when the file holds none of the projected columns. There is
    /// nothing to queue, so nothing is opened for reading either.
    queue: Option<Arc<WorkQueue>>,
    output: Output,
}

impl FileRead {
    /// Plan `dataset` for a scan that wants `projected_schema`.
    ///
    /// Resolves the projection, fills the queue, and decides what a batch off it
    /// becomes. `predicate` is a hint: it prunes chunks that cannot hold a row
    /// the query wants, and the scan is expected to apply it again above.
    ///
    /// `metrics` belong to the partition that plans. They take the counts made
    /// here rather than per chunk: a chunk the predicate excluded is dropped
    /// before the queue exists, so no reader of the queue can account for it.
    ///
    /// `partitions` are the table's `PARTITIONED BY` columns and this file's
    /// values for them. They are in the file's path rather than in the file, so
    /// they are appended to its batches here — see [`FilePartitions`]. Pass
    /// [`FilePartitions::none`] for an unpartitioned table.
    pub async fn plan(
        dataset: AnyDataset,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        partitions: FilePartitions,
        metrics: Option<&ReadMetrics>,
    ) -> Result<Arc<Self>> {
        let dataset_schema: SchemaRef = Arc::new(
            crate::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to derive an Arrow schema from the dataset: {e}"
                ))
            })?,
        );

        // The `PARTITIONED BY` columns this scan projects, in the order it wants
        // them. They are added to every batch below rather than read: the value
        // is in the file's path, not in the file.
        let partition_fields = partitions.projected_fields(&projected_schema);

        // The columns of this file the query needs, in file order. A partition
        // column shadows a variable of the same name — the path wins, as it does
        // for every other format — so it never counts as one of these.
        let projection: Vec<usize> = dataset_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                !partitions.holds(field.name()) && projected_schema.index_of(field.name()).is_ok()
            })
            .map(|(index, _)| index)
            .collect();

        let pushdown = predicate.clone().map(PushdownFilter::new);

        // How much of the file itself the query wants, partition columns aside.
        let wanted_file_columns = projected_schema.fields().len() - partition_fields.len();

        // Nothing of this file was projected. That is two different situations,
        // and they must not be confused: the query wanted no column at all, or
        // it wanted columns this file does not have.
        let (output, projection) = if projection.is_empty() {
            if wanted_file_columns > 0 {
                // The query named columns and this file has none of them. A
                // collection is not obliged to be uniform — of one CORA year, 2%
                // of the files carry no `TEMP` and 10% no `DEPH` — so this is an
                // ordinary file, not a broken one.
                //
                // It contributes no rows. Its row count is a property of the
                // arrays being read, and there are none; inventing one would
                // mean picking a grid from variables the query never asked for
                // and returning that many nulls.
                //
                // Reading it as a `COUNT(*)` instead, which is what this used to
                // do, built a batch of no columns against a schema that has
                // some, and the scan failed outright with "number of columns(0)
                // must match number of fields(1)".
                return Ok(Arc::new(Self {
                    queue: None,
                    output: Output::Nothing,
                }));
            }

            // `COUNT(*)`, or a scan of nothing but partition columns: no column
            // of the file is wanted, so the read is driven by columns of its own
            // and only the row counts leave.
            let counted = count_projection(&dataset, &dataset_schema, &predicate);
            (
                Output::Rows {
                    schema: projected_schema,
                    partitions,
                },
                counted,
            )
        } else {
            // The scan carries nd columns, so adaptation happens in the encoded
            // (struct) domain: reorder and null-fill onto the projected schema.
            // The partition columns join the source there, so the adapter puts
            // them wherever the projection asked for them.
            let mut source_fields: Vec<FieldRef> =
                beacon_datafusion_ext::nd::encoded_schema(&dataset_schema.project(&projection)?)
                    .fields()
                    .to_vec();
            source_fields.extend(partition_fields.iter().cloned());
            let source_schema: SchemaRef = Arc::new(Schema::new(source_fields));

            let partition_columns = partitions.scalar_columns(&projected_schema)?;
            let adapter =
                BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;
            (
                Output::Columns {
                    adapter: Arc::new(adapter),
                    partition_fields,
                    partition_columns,
                },
                projection,
            )
        };

        let dataset = project(dataset, &dataset_schema, projection)?;
        let queue =
            WorkQueue::build(dataset, batch_size, pushdown, output.encoded(), metrics).await?;

        Ok(Arc::new(Self {
            queue: Some(queue),
            output,
        }))
    }

    /// How much of the file is left to read. For tests and diagnostics.
    pub fn remaining(&self) -> usize {
        self.queue.as_ref().map_or(0, |queue| queue.remaining())
    }

    /// One partition's stream over the file.
    ///
    /// Every worker that reaches this file calls it on the same `FileRead`, and
    /// they draw from the one queue behind it, so no two of them read the same
    /// chunk. `metrics` are the calling partition's, so what each one read is
    /// what it reports.
    pub fn stream(
        &self,
        metrics: Option<ReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let Some(queue) = self.queue.clone() else {
            // The file holds none of the projected columns. See
            // [`FileRead::plan`].
            return futures::stream::empty().boxed();
        };
        let batches = queue.stream(metrics);
        match &self.output {
            Output::Columns {
                adapter,
                partition_fields,
                partition_columns,
            } => {
                let adapter = adapter.clone();
                let fields = partition_fields.clone();
                let columns = partition_columns.clone();
                batches
                    .and_then(move |batch| {
                        let adapted = with_partitions(&batch, &fields, &columns)
                            .and_then(|batch| adapter.adapt_batch(&batch))
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to adapt the batch onto the scan's schema: {e}"
                                ))
                            });
                        futures::future::ready(adapted)
                    })
                    .boxed()
            }
            Output::Rows { schema, partitions } => {
                let schema = schema.clone();
                let partitions = partitions.clone();
                batches
                    .and_then(move |batch| {
                        futures::future::ready(count_batch(&schema, &partitions, batch.num_rows()))
                    })
                    .boxed()
            }
            // Unreachable: `plan` pairs `Nothing` with no read, and the early
            // return above covers it.
            Output::Nothing => futures::stream::empty().boxed(),
        }
    }
}

/// `batch` with the file's `PARTITIONED BY` columns appended.
///
/// Each is one value on no axis, so it broadcasts over whatever grid the file's
/// own columns define and reaches every row the file contributes. Nothing is
/// built per row, and nothing is built per batch: the columns are the file's,
/// and [`FileRead::plan`] made them once.
fn with_partitions(
    batch: &RecordBatch,
    fields: &[FieldRef],
    columns: &[ArrayRef],
) -> Result<RecordBatch> {
    if fields.is_empty() {
        return Ok(batch.clone());
    }

    let schema: Vec<FieldRef> = batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .chain(fields.iter().cloned())
        .collect();
    let values: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .cloned()
        .chain(columns.iter().cloned())
        .collect();

    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(schema)),
        values,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// The batch a read that wants no column of the file emits for `rows` rows.
///
/// A plain `COUNT(*)` emits the row count and nothing else. A scan of nothing
/// but partition columns emits those columns instead: rank-0 columns alone
/// would define a rank-0 grid, which holds one row, so the rows of the read are
/// stated as an axis of their own.
fn count_batch(
    schema: &SchemaRef,
    partitions: &FilePartitions,
    rows: usize,
) -> Result<RecordBatch> {
    let (columns, encoded_rows) = if schema.fields().is_empty() {
        (Vec::new(), rows)
    } else {
        (partitions.row_columns(schema, rows)?, 1)
    };

    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(encoded_rows)),
    )
    .map_err(|e| DataFusionError::Execution(format!("Failed to build a count batch: {e}")))
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

    /// A projection that wants no column, so a plan takes the `COUNT(*)` path.
    ///
    /// These tests are about who opens the file, not what comes out of it, and
    /// this is the shortest way to a real [`FileRead`] with a filled queue.
    fn no_columns() -> SchemaRef {
        Arc::new(Schema::empty())
    }

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
        shared: Arc<WorkQueue>,
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

        let whole = WorkQueue::build(dataset(ROWS).await, BATCH, None, true, None)
            .await
            .expect("the read builds");
        let chunks = whole.remaining();
        let all = values_read(whole.stream(None)).await;
        assert_eq!(all.len(), ROWS, "the unfiltered read returns the file");

        let pruned = WorkQueue::build(
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

        let shared = WorkQueue::build(
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

        let nd = WorkQueue::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            true,
            None,
        )
        .await
        .expect("the read builds");
        let nd_rows = drain(nd.stream(None)).await;

        let flat = WorkQueue::build(
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

        let shared = WorkQueue::build(
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
            let shared = WorkQueue::build(dataset(ROWS).await, 512, None, true, None)
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
            let shared = WorkQueue::build(dataset(ROWS).await, 512, None, false, None)
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

        let shared = WorkQueue::build(dataset(ROWS).await, BATCH, None, true, None)
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
                let shared = WorkQueue::build(source, batch_size, None, true, None)
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
        let shared = WorkQueue::build(source, 8, None, false, None)
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
        let shared = WorkQueue::build(source, 8, None, true, None)
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

    /// A file holding none of the projected columns contributes nothing.
    ///
    /// A collection is not obliged to be uniform. Of one CORA year, 2% of the
    /// files carry no `TEMP` and 10% no `DEPH`, so `SELECT TEMP` meets files
    /// that have none of what it asked for. Those are ordinary files.
    ///
    /// This used to be read as a `COUNT(*)` — the projection resolves empty
    /// either way — which built a batch of no columns against a schema that has
    /// one, and failed the whole scan with "number of columns(0) must match
    /// number of fields(1)".
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_file_without_any_projected_column_is_read_as_nothing() {
        let wanted: SchemaRef = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            // The dataset holds "value" and nothing else.
            "absent",
            arrow::datatypes::DataType::Float64,
            true,
        )]));

        let planned = FileRead::plan(
            dataset(64).await,
            wanted,
            16,
            None,
            FilePartitions::none(),
            None,
        )
        .await
        .expect("a file without the column is planned, not rejected");

        assert_eq!(planned.remaining(), 0, "nothing is queued to read");
        let batches: Vec<RecordBatch> = planned
            .stream(None)
            .try_collect()
            .await
            .expect("and the stream is clean, not an error");
        assert!(batches.is_empty(), "it contributes no rows");
    }

    /// A `COUNT(*)` still counts. It projects no column *because it wants none*,
    /// which is the case the check above has to keep telling apart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_count_over_the_same_file_still_counts_it() {
        const ROWS: usize = 64;

        let planned = FileRead::plan(
            dataset(ROWS).await,
            no_columns(),
            16,
            None,
            FilePartitions::none(),
            None,
        )
        .await
        .expect("a count is planned");

        assert!(planned.remaining() > 0, "a count has work to do");
        assert_eq!(drain_flat(planned.stream(None)).await, ROWS);
    }

    /// More partitions than subsets is not an error. The surplus find the queue
    /// empty and finish at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partition_with_nothing_left_to_pop_just_finishes() {
        const ROWS: usize = 100;

        // One chunk, eight partitions.
        let shared = WorkQueue::build(dataset(ROWS).await, usize::MAX, None, true, None)
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

        let shared = WorkQueue::build(dataset(ROWS).await, BATCH, None, true, None)
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
