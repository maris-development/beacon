//! [`OrderedUnionExec`]: concatenate several inputs in a **deterministic** order
//! while still executing them concurrently.
//!
//! DataFusion's [`UnionExec`](datafusion::physical_plan::union::UnionExec) exposes
//! its children as separate output partitions, and a multi-partition plan is
//! collected in *completion* order. That makes row order vary run to run, which
//! is why splitting a Lance scan by fragment cost reproducible results.
//!
//! This node keeps the split (so decoding still fans out across threads) but
//! reports a **single** output partition whose batches are emitted strictly in
//! child order: everything from child 0, then child 1, and so on.
//!
//! Concurrency comes from readahead rather than from output partitioning. Each
//! child is driven by its own task feeding a bounded channel, so children run
//! ahead of the consumer until their buffer fills and then backpressure. Peak
//! memory is bounded by `children * READAHEAD_BATCHES` batches instead of the
//! whole result.
//!
//! This is cheap for filtered scans because beacon pushes the predicate into
//! Lance (see `supports_filters_pushdown`), so what flows through the single
//! output partition is only the matching rows, not the full table.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Readahead is bounded by **bytes**, not by a batch count.
///
/// Output is emitted strictly in child order, so while child 0 drains, child *i*
/// can only run ahead until its buffered bytes hit the budget. A batch count is a
/// poor dial for this: batch footprints vary by orders of magnitude with schema
/// (and cannot be predicted from the schema at all once there are strings or
/// other variable-width columns). Instead each child charges the *actual*
/// `RecordBatch::get_array_memory_size()` against a shared budget that is
/// reserved from DataFusion's `MemoryPool`, so this node stays inside the same
/// accounting as every other operator.
///
/// Permits are tracked in KiB so a 64-bit semaphore can express a large budget.
const PERMIT_UNIT: usize = 1024;

/// Fraction of a finite pool we are willing to hold for readahead.
const POOL_FRACTION: usize = 10;
/// Never hold more than this, however large the pool is: past roughly this point
/// extra readahead stops buying parallelism (measured: 128 MiB and 512 MiB are
/// within noise of each other).
const MAX_BUDGET: usize = 128 << 20;
/// Preferred floor, but see `readahead_budget`: on a small pool this is capped so
/// readahead can never crowd out the query it is feeding.
const PREFERRED_MIN_BUDGET: usize = 8 << 20;
/// Largest share of a small pool the floor may claim.
const SMALL_POOL_SHARE: usize = 4;

/// Derive the readahead budget from DataFusion's memory pool.
///
/// `FairSpillPool` (beacon's default) reports a finite limit, so this normally
/// resolves to `limit / POOL_FRACTION` capped at `MAX_BUDGET`. The floor is
/// deliberately *not* a flat constant: on a small pool a fixed 8 MiB floor could
/// be a large fraction of the whole budget and starve the operators downstream,
/// so it is additionally capped at `limit / SMALL_POOL_SHARE`.
///
/// The caller still steps the request down if the pool cannot honour it, so this
/// is a target rather than a guarantee.
fn readahead_budget(pool: &Arc<dyn MemoryPool>) -> usize {
    match pool.memory_limit() {
        MemoryLimit::Finite(limit) => {
            let floor = PREFERRED_MIN_BUDGET.min(limit / SMALL_POOL_SHARE).max(1);
            (limit / POOL_FRACTION).clamp(floor, MAX_BUDGET)
        }
        // No declared limit: stay modest rather than unbounded.
        MemoryLimit::Infinite | MemoryLimit::Unknown => PREFERRED_MIN_BUDGET * 4,
    }
}

#[derive(Debug, Clone)]
pub struct OrderedUnionExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl OrderedUnionExec {
    pub fn try_new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        match inputs.len() {
            0 => Err(DataFusionError::Internal(
                "OrderedUnionExec requires at least one input".into(),
            )),
            // A lone input still needs wrapping when it has several partitions;
            // only a genuinely single-partition input can be handed back as is.
            1 if inputs[0].output_partitioning().partition_count() <= 1 => {
                Ok(inputs.into_iter().next().expect("len checked"))
            }
            _ => {
                let schema = inputs[0].schema();
                let cache = Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema.clone()),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ));
                Ok(Arc::new(Self {
                    inputs,
                    schema,
                    cache,
                }))
            }
        }
    }
}

impl DisplayAs for OrderedUnionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OrderedUnionExec: inputs={}, byte-bounded readahead",
            self.inputs.len()
        )
    }
}

impl ExecutionPlan for OrderedUnionExec {
    fn name(&self) -> &str {
        "OrderedUnionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Self::try_new(children)
    }

    /// Statistics for the single output partition: the concatenation of every
    /// child partition, so row counts and byte sizes add up.
    ///
    /// Without this, the default `Statistics::new_unknown` hides the row count and
    /// DataFusion can no longer answer `count(*)` from metadata: it falls back to
    /// actually scanning. Measured on a 100M-row Lance table, that turned a 1.3ms
    /// `count(*)` into 21ms. `UnionExec` propagates statistics for the same reason.
    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        if matches!(partition, Some(p) if p != 0) {
            return Ok(Statistics::new_unknown(&self.schema));
        }
        let mut total = Statistics::new_unknown(&self.schema);
        let mut rows = Precision::Exact(0usize);
        let mut bytes = Precision::Exact(0usize);
        for input in &self.inputs {
            let s = input.partition_statistics(None)?;
            rows = rows.add(&s.num_rows);
            bytes = bytes.add(&s.total_byte_size);
        }
        total.num_rows = rows;
        total.total_byte_size = bytes;
        Ok(total)
    }

    /// Keep children in order: this node's whole purpose is that child *i*'s rows
    /// precede child *i+1*'s, so DataFusion must not reorder them.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OrderedUnionExec has exactly 1 partition, got {partition}"
            )));
        }

        // Reserve the readahead budget from DataFusion's pool so this node's
        // buffering is visible to (and limited by) the same accounting as every
        // other operator. If the pool cannot spare the target, step down rather
        // than fail: a smaller budget just means less readahead, not an error.
        let pool = context.memory_pool();
        let reservation = MemoryConsumer::new("OrderedUnionExec").register(pool);
        let mut budget = readahead_budget(pool);
        // Step down rather than fail: less readahead is fine, an error is not.
        while budget > 0 && reservation.try_grow(budget).is_err() {
            budget /= 2;
        }
        if reservation.size() == 0 {
            // Pool is fully subscribed; fall back to a single batch in flight per
            // child so we still make progress without over-committing memory.
            budget = 0;
        }

        // Permits are the byte budget in KiB. A child charges the real footprint
        // of each batch and holds the permit until the consumer has emitted it,
        // so slow-draining children naturally throttle their producers.
        // IMPORTANT: the budget is split *per child*, not shared.
        //
        // A shared budget deadlocks. Output is emitted in child order, so if
        // children 1..n race ahead and exhaust a shared pool, child 0 blocks
        // acquiring a permit; its permits are only released once the consumer
        // emits child 0's batches, which it cannot do while child 0 is blocked.
        // Circular wait.
        //
        // With a private budget per child, a child can only ever block on its own
        // allowance. The consumer always drains the child it is currently on,
        // freeing that child's permits, so progress is guaranteed. Total memory is
        // still bounded by `children * per_child == budget`.
        // Walk every (child, partition) pair in order. With one plan per child this
        // is just the children; with a single multi-partition child (e.g. a Parquet
        // DataSourceExec with 12 file groups) it is that child's partitions in
        // index order. Either way the concatenation is fully determined.
        let mut units: Vec<(Arc<dyn ExecutionPlan>, usize)> = Vec::new();
        for input in &self.inputs {
            let n = input.output_partitioning().partition_count();
            for part in 0..n {
                units.push((input.clone(), part));
            }
        }

        let n_children = units.len().max(1);
        let per_child = ((budget / n_children) / PERMIT_UNIT)
            .max(1)
            .min(Semaphore::MAX_PERMITS);

        let mut receivers = Vec::with_capacity(units.len());
        for (input, part) in units {
            // Unbounded queue: the *semaphore* is the bound, not the channel. A
            // small channel capacity would cap in-flight batches per child and
            // make the byte budget irrelevant.
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
                DataFusionResult<(RecordBatch, Option<OwnedSemaphorePermit>)>,
            >();
            let mut stream = input.execute(part, context.clone())?;
            // Each child gets its own allowance; see the note above.
            let sem = Arc::new(Semaphore::new(per_child));
            let max_permits = per_child;
            tokio::spawn(async move {
                while let Some(item) = stream.next().await {
                    let payload = match item {
                        Ok(batch) => {
                            // Charge the batch's real footprint. Clamp to the whole
                            // budget so one oversized batch cannot deadlock, but do
                            // NOT clamp to what happens to be free right now: that
                            // would let a child slip through under-charged and the
                            // budget would stop bounding anything.
                            let want = (batch.get_array_memory_size() / PERMIT_UNIT)
                                .max(1)
                                .min(max_permits);
                            let permit = sem.clone().acquire_many_owned(want as u32).await.ok();
                            Ok((batch, permit))
                        }
                        Err(e) => Err(e),
                    };
                    if tx.send(payload).is_err() {
                        break; // consumer gone (LIMIT, error, cancellation)
                    }
                }
            });
            receivers.push(rx);
        }

        // Drain child 0 fully, then child 1, ... -> deterministic row order.
        // Dropping the permit alongside the batch returns its bytes to the budget.
        let stream = futures::stream::unfold((receivers, 0usize), |(mut rxs, mut idx)| async move {
            loop {
                if idx >= rxs.len() {
                    return None;
                }
                match rxs[idx].recv().await {
                    Some(Ok((batch, _permit))) => return Some((Ok(batch), (rxs, idx))),
                    Some(Err(e)) => return Some((Err(e), (rxs, idx))),
                    None => idx += 1,
                }
            }
        });

        // Hold the reservation for the life of the stream.
        let stream = stream.inspect(move |_| {
            let _keep = &reservation;
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Order-preserving replacement for [`datafusion::physical_plan::execute_stream`].
///
/// DataFusion's version wraps any multi-partition plan in `CoalescePartitionsExec`,
/// which emits batches in *completion* order. That is why `read_parquet` (a
/// `DataSourceExec` with one partition per file group) returns different rows for
/// `LIMIT 5` on every run. This variant concatenates the partitions in index
/// order instead, so results are reproducible while the partitions still execute
/// concurrently.
pub fn execute_stream_ordered(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> DataFusionResult<datafusion::execution::SendableRecordBatchStream> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(Box::pin(
            datafusion::physical_plan::EmptyRecordBatchStream::new(plan.schema()),
        )),
        1 => plan.execute(0, context),
        _ => OrderedUnionExec::try_new(vec![plan])?.execute(0, context),
    }
}

/// Physical optimizer rule: make partition merging order-preserving.
///
/// DataFusion inserts [`CoalescePartitionsExec`] wherever a multi-partition plan
/// must funnel into one stream (notably under a `LIMIT`). That node emits batches
/// in *completion* order, so `read_parquet(...) LIMIT 5` returns different rows on
/// every run. Rewriting those nodes to [`OrderedUnionExec`] concatenates the
/// partitions in index order instead, which is reproducible; the partitions still
/// execute concurrently, bounded by the memory pool.
///
/// This only helps while a partition's *contents* are reproducible too. A
/// [`FastObjectScan`](crate::fast_object) with no limit shares one
/// queue between its partitions, so which one reads which file depends on
/// scheduling and concatenating them in index order is not stable. A scan that
/// carries a limit does not share: each partition reads its own contiguous
/// slice of the listing, so the `LIMIT` case this rule exists for is exactly
/// the case that stays reproducible.
///
/// A `fetch` on the original node is preserved by re-applying it above.
#[derive(Debug, Default)]
pub struct OrderedCoalesce;

impl datafusion::physical_optimizer::PhysicalOptimizerRule for OrderedCoalesce {
    fn name(&self) -> &str {
        "beacon_ordered_coalesce"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

        plan.transform_up(|node| {
            let Some(coalesce) = node.as_any().downcast_ref::<CoalescePartitionsExec>() else {
                return Ok(Transformed::no(node));
            };
            let child = Arc::clone(coalesce.input());
            if child.output_partitioning().partition_count() <= 1 {
                return Ok(Transformed::no(node));
            }
            let ordered = OrderedUnionExec::try_new(vec![child])?;
            let out = match coalesce.fetch() {
                Some(fetch) => Arc::new(
                    datafusion::physical_plan::limit::GlobalLimitExec::new(ordered, 0, Some(fetch)),
                ) as Arc<dyn ExecutionPlan>,
                None => ordered,
            };
            Ok(Transformed::yes(out))
        })
        .map(|t| t.data)
    }
}
