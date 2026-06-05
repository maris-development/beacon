use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

/// Metrics for a single dataset read stream.
///
/// Create via [`DatasetReadMetrics::new`] and pass to the `dataset_as_record_batch_stream`
/// family of functions. All counters share the underlying [`ExecutionPlanMetricsSet`]
/// so they appear in DataFusion's standard metrics reporting.
///
/// Only metrics that DataFusion does not already track are recorded here. Output
/// rows/batches are intentionally omitted: DataFusion's `FileStream` records the
/// standard `output_rows` and `output_batches` metrics on the same metric set, so
/// counting them again here would double-count and (for `output_batches`) clash
/// with DataFusion's typed metric of the same name during `EXPLAIN ANALYZE`.
#[derive(Debug, Clone)]
pub struct DatasetReadMetrics {
    /// Approximate rows in chunks/casts that were entirely skipped by the predicate.
    pub rows_pruned: Count,
    /// Number of chunks (regular) or casts (ragged) entirely skipped by the predicate.
    pub batches_pruned: Count,
}

impl DatasetReadMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            rows_pruned: MetricBuilder::new(metrics).counter("rows_pruned", partition),
            batches_pruned: MetricBuilder::new(metrics).counter("batches_pruned", partition),
        }
    }
}
