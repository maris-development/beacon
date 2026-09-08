use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time};

/// Per-partition timings and counts for one Atlas scan partition.
///
/// Every field is an `Arc`-backed handle into the shared
/// [`ExecutionPlanMetricsSet`], so a clone is cheap and every clone accumulates
/// into the same metric.
#[derive(Debug, Clone)]
pub struct AtlasScanMetrics {
    /// Wall time opening collections, or hitting the reader cache for them.
    pub open_time: Time,
    /// Wall time deciding which datasets a predicate can rule out.
    pub prune_time: Time,
    pub dataset_build_time: Time,
    /// Datasets this partition opened and read.
    pub datasets_scanned: Count,
    /// Datasets it skipped because the collection's statistics ruled them out.
    pub datasets_pruned: Count,
    /// Pruning indexes built. One per collection a predicate scan touches, so a
    /// number above the collection count means a partition rebuilt one.
    pub index_builds: Count,
    /// Datasets those indexes covered, which is what the pruning pass looked
    /// at rather than read.
    pub index_rows: Count,
}

impl AtlasScanMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            open_time: MetricBuilder::new(metrics).subset_time("atlas_open_time", partition),
            prune_time: MetricBuilder::new(metrics).subset_time("atlas_prune_time", partition),
            dataset_build_time: MetricBuilder::new(metrics)
                .subset_time("atlas_dataset_build_time", partition),
            datasets_scanned: MetricBuilder::new(metrics)
                .counter("atlas_datasets_scanned", partition),
            datasets_pruned: MetricBuilder::new(metrics)
                .counter("atlas_datasets_pruned", partition),
            index_builds: MetricBuilder::new(metrics).counter("atlas_index_builds", partition),
            index_rows: MetricBuilder::new(metrics).counter("atlas_index_rows", partition),
        }
    }
}
