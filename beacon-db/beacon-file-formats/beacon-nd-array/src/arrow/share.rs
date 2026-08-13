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
    ChunkGrid, RaggedPlan, build_dataset_schema, chunk_grid, compute_predicate_masks,
    plan_ragged_read, read_chunk, read_ragged_range,
};
use crate::arrow::nd_provider::read_nd_chunk;
use crate::arrow::pushdown_filter::PushdownFilter;
use crate::dataset::AnyDataset;

/// Which batches a shared read produces.
///
/// A scan is one or the other throughout, so the mode is fixed when the queue is
/// filled. The first partition to arrive chooses it, and every partition of a
/// scan would choose the same, so whichever arrives first is the right one.
#[derive(Debug, Clone)]
pub enum ReadMode {
    /// `beacon.nd`-encoded batches, which an `NdSourceExec` decodes above the
    /// scan. This is what a column read produces.
    Encoded,
    /// Flat, broadcast batches. This is what the `COUNT(*)` path reads, and the
    /// only path that prunes chunks, so it carries the predicate that prunes
    /// them.
    Flat(Option<PushdownFilter>),
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
        let predicate = match &mode {
            ReadMode::Flat(predicate) => predicate.clone(),
            ReadMode::Encoded => None,
        };

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
                        // The flat path prunes: a chunk whose mask is all false
                        // holds nothing the query wants, and is skipped.
                        return read_chunk(&arrays, subset, schema, &dims, &masks)
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
                let encode = matches!(self.mode, ReadMode::Encoded);
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
