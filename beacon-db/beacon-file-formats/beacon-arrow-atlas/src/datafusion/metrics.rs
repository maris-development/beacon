//! Execution metrics for the atlas scan, surfaced through DataFusion's standard
//! metrics reporting (e.g. `EXPLAIN ANALYZE`).
//!
//! These complement
//! [`DatasetReadMetrics`](beacon_nd_array::arrow::metrics::DatasetReadMetrics)
//! (output rows/batches and engine-level chunk pruning) with the atlas-specific
//! costs: opening the store, pruning datasets, and building each dataset's lazy
//! backends. All names are `atlas_`-prefixed so they never collide with
//! DataFusion's reserved typed metrics (`output_rows`, `output_batches`, …),
//! which aggregate by name and panic on a variant mismatch.

use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time};

/// Per-partition timings and counts for one atlas scan partition. All fields are
/// `Arc`-backed handles into the shared [`ExecutionPlanMetricsSet`], so cloning
/// is cheap and every clone accumulates into the same metric.
#[derive(Debug, Clone)]
pub struct AtlasScanMetrics {
    /// Wall time opening (or cache-hitting) the atlas store for this partition.
    pub open_time: Time,
    /// Wall time computing which datasets the predicate can match (pruning).
    pub prune_time: Time,
    /// Wall time building lazy datasets — metadata, backends, projected
    /// attribute values, and the per-dataset schema adapter.
    pub dataset_build_time: Time,
    /// Datasets this partition opened and scanned.
    pub datasets_scanned: Count,
    /// Datasets this partition skipped because pruning ruled them out.
    pub datasets_pruned: Count,
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
        }
    }
}
