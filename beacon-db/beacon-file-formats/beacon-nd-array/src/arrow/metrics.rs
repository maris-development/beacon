use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

/// What one partition did with the files and file parts it read.
///
/// None of these numbers is visible anywhere else. `FileStream` reports what
/// the scan *emitted*. For an nd read that is one row per chunk, which says
/// nothing about how a file divided between the partitions or how much of it
/// the predicate skipped.
///
/// # Why not `output_rows`
///
/// Every name here is its own. `output_rows` and `output_batches` are reserved:
/// `FileStream`'s `BaselineMetrics` already registers them for this same
/// partition, and DataFusion sums metrics that share a name when it displays
/// them. Recording a read under those names would report the scan's own rows
/// plus these, which is a number that means nothing.
#[derive(Debug, Clone)]
pub struct ReadMetrics {
    /// Chunks (regular) or batches (ragged) this partition read.
    ///
    /// The parts of a split file read disjoint chunks, so these sum across the
    /// partitions to the file's total. A file read by one partition while the
    /// others idle shows up here and nowhere else.
    pub chunks_read: Count,
    /// Rows those chunks hold, counted as the scan will broadcast them.
    ///
    /// An nd batch carries a whole chunk in one row, so this is the row count
    /// the query sees, not the row count the scan emits.
    pub rows_read: Count,
    /// Chunks the predicate excluded before the chunk list was made.
    ///
    /// Recorded once for the file, by the partition that reads its first part.
    pub chunks_pruned: Count,
    /// Rows those chunks held.
    pub rows_pruned: Count,
    /// Files `skip_unbroadcastable` skipped because they did not fit the
    /// dimension list. Recorded by the partition that reads the first part.
    pub files_skipped: Count,
}

impl ReadMetrics {
    /// Register this partition's counters.
    ///
    /// Once per partition, not once per file. Every call here takes five
    /// `MetricBuilder`s, and each one ends in `register`, which locks the scan's
    /// one `ExecutionPlanMetricsSet` and pushes onto a `Vec` that is never
    /// pruned. Calling it per file made 24 partitions contend on that lock tens
    /// of thousands of times and left a metrics set to match; the counters are
    /// per partition anyway, so a file gets a [`Clone`] of its partition's.
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            chunks_read: MetricBuilder::new(metrics).counter("chunks_read", partition),
            rows_read: MetricBuilder::new(metrics).counter("rows_read", partition),
            chunks_pruned: MetricBuilder::new(metrics).counter("chunks_pruned", partition),
            rows_pruned: MetricBuilder::new(metrics).counter("rows_pruned", partition),
            files_skipped: MetricBuilder::new(metrics).counter("files_skipped", partition),
        }
    }
}
