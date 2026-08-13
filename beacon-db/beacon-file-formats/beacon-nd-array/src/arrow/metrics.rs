use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

/// Metrics for a single dataset read stream.
///
/// Create via [`DatasetReadMetrics::new`] and pass to the `dataset_as_record_batch_stream`
/// family of functions. All counters share the underlying [`ExecutionPlanMetricsSet`]
/// so they appear in DataFusion's standard metrics reporting.
#[derive(Debug, Clone)]
pub struct DatasetReadMetrics {
    /// Rows in output record batches.
    pub output_rows: Count,
    /// Number of record batches emitted.
    pub output_batches: Count,
    /// Approximate rows in chunks/casts that were entirely skipped by the predicate.
    pub rows_pruned: Count,
    /// Number of chunks (regular) or casts (ragged) entirely skipped by the predicate.
    pub batches_pruned: Count,
}

impl DatasetReadMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            // Use the dedicated typed builder (matching `output_rows` above) rather
            // than a generic `counter("output_batches", ..)`. `output_batches` is a
            // reserved DataFusion metric name: `FileStream`'s `BaselineMetrics`
            // already registers a typed `MetricValue::OutputBatches` under it. A
            // generic `Count` under the same name produces two metrics that share a
            // name but differ in variant, which panics when DataFusion aggregates
            // metrics by name for display (e.g. `EXPLAIN ANALYZE`).
            output_batches: MetricBuilder::new(metrics).output_batches(partition),
            rows_pruned: MetricBuilder::new(metrics).counter("rows_pruned", partition),
            batches_pruned: MetricBuilder::new(metrics).counter("batches_pruned", partition),
        }
    }
}

/// What one partition did with a file it read through a share.
///
/// These are the numbers the sharing exists for, and none of them is visible
/// anywhere else. `FileStream` reports what the scan *emitted* — for an nd read
/// that is one row per chunk, which says nothing about how the file divided
/// between the partitions or how much of it the predicate skipped.
///
/// # Why not `output_rows`
///
/// Every name here is its own. `output_rows` and `output_batches` are reserved:
/// `FileStream`'s `BaselineMetrics` already registers them for this same
/// partition, and DataFusion sums metrics that share a name when it displays
/// them. Recording a read under those names would report the scan's own rows
/// plus these, which is a number that means nothing.
#[derive(Debug, Clone)]
pub struct SharedReadMetrics {
    /// Chunks (regular) or batches (ragged) this partition took off the queue.
    ///
    /// The queue is shared, so these sum across the partitions to the file's
    /// total. A file read by one partition while the others idle shows up here
    /// and nowhere else.
    pub chunks_read: Count,
    /// Rows those chunks hold, counted as the scan will broadcast them.
    ///
    /// An nd batch carries a whole chunk in one row, so this is the row count
    /// the query sees, not the row count the scan emits.
    pub rows_read: Count,
    /// Chunks the predicate excluded before the queue was filled.
    ///
    /// Recorded once for the file, by the partition that opened it. The others
    /// find the work already gone, which is the point.
    pub chunks_pruned: Count,
    /// Rows those chunks held.
    pub rows_pruned: Count,
}

impl SharedReadMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            chunks_read: MetricBuilder::new(metrics).counter("chunks_read", partition),
            rows_read: MetricBuilder::new(metrics).counter("rows_read", partition),
            chunks_pruned: MetricBuilder::new(metrics).counter("chunks_pruned", partition),
            rows_pruned: MetricBuilder::new(metrics).counter("rows_pruned", partition),
        }
    }
}
