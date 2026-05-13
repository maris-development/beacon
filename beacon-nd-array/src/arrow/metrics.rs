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
            output_batches: MetricBuilder::new(metrics).counter("output_batches", partition),
            rows_pruned: MetricBuilder::new(metrics).counter("rows_pruned", partition),
            batches_pruned: MetricBuilder::new(metrics).counter("batches_pruned", partition),
        }
    }
}
