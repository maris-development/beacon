//! The counters of the scan.

use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

/// What one partition of the scan did with the files it read.
///
/// The names differ from the names of FileStream, because DataFusion adds metrics with the same name.
/// `FileStream` registers `output_rows` and `output_batches` for the same partition.
#[derive(Debug, Clone)]
pub struct ScanMetrics {
    /// The chunks this partition read.
    pub chunks_read: Count,
    /// The rows of the chunks this partition read.
    pub rows_read: Count,
    /// The chunks that the predicate excluded before the read.
    pub chunks_pruned: Count,
    /// The rows of the chunks that the predicate excluded.
    pub rows_pruned: Count,
    /// The files that the scan skipped.
    pub files_skipped: Count,
}

impl ScanMetrics {
    /// Register the counters of one partition in `metrics`.
    ///
    /// Call this once for each partition, not once for each file.
    /// Each call locks `metrics` five times and adds five entries that stay.
    /// A file gets a [`Clone`] of the counters of its partition.
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

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    use super::ScanMetrics;

    #[test]
    fn the_counters_have_the_names_that_beacon_core_reads() {
        let set = ExecutionPlanMetricsSet::new();
        let metrics = ScanMetrics::new(&set, 0);
        metrics.chunks_read.add(2);
        let names: Vec<String> = set
            .clone_inner()
            .iter()
            .map(|m| m.value().name().to_string())
            .collect();
        assert_eq!(
            names,
            [
                "chunks_read",
                "rows_read",
                "chunks_pruned",
                "rows_pruned",
                "files_skipped"
            ]
        );
    }
}
