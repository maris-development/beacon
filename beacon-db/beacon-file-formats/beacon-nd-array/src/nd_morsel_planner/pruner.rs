//! Pruning of the planned morsels: computes the dimension masks of a file and counts the chunks they drop.

use std::sync::Arc;

use datafusion::{
    error::Result,
    physical_plan::{
        PhysicalExpr,
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder},
    },
};
use indexmap::IndexMap;

use super::external;
use crate::{
    NdArrayD,
    dataset_v2::pruning::{DimensionMasks, prune_mask_dimensions},
};

/// A file cut into at most this many chunks or cast ranges is not pruned.
pub const DEFAULT_PRUNING_CHUNK_THRESHOLD: usize = 4;

/// Drops the chunks of a file that hold no row the pushdown predicate wants.
///
/// A chunk is a chunk of the grid in a gridded file, and a range of casts in a ragged file.
#[derive(Debug)]
pub(super) struct MorselPruner {
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pub(super) threshold: usize,
    pub(super) metrics: PruningMetrics,
}

impl MorselPruner {
    pub(super) fn new(predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            predicate,
            threshold: DEFAULT_PRUNING_CHUNK_THRESHOLD,
            metrics: PruningMetrics::default(),
        }
    }

    /// Check if a plan needs the unprojected arrays of its file.
    pub(super) fn has_predicate(&self) -> bool {
        self.predicate.is_some()
    }

    /// Compute the dimension masks of a file, from its 0-D and 1-D arrays.
    ///
    /// Returns `None` without a predicate, or when the file has too few chunks to gain from it.
    ///
    /// # Arguments
    ///
    /// * `arrays` - The arrays of the file before the projection
    /// * `chunks` - The number of chunks of the file
    pub(super) async fn masks(
        &self,
        arrays: Option<&IndexMap<String, Arc<dyn NdArrayD>>>,
        chunks: usize,
    ) -> Result<Option<DimensionMasks>> {
        let (Some(predicate), Some(arrays)) = (&self.predicate, arrays) else {
            return Ok(None);
        };
        if chunks <= self.threshold {
            return Ok(None);
        }
        let low_rank: IndexMap<String, Arc<dyn NdArrayD>> = arrays
            .iter()
            .filter(|(_, array)| array.dimensions().len() <= 1)
            .map(|(name, array)| (name.clone(), Arc::clone(array)))
            .collect();
        let masks = prune_mask_dimensions(&low_rank, Arc::clone(predicate))
            .await
            .map_err(external)?;
        Ok(Some(masks))
    }

    /// Count the chunks that the masks dropped, and their rows.
    pub(super) fn record(&self, chunks: usize, rows: usize) {
        self.metrics.chunks_pruned.add(chunks);
        self.metrics.rows_pruned.add(rows);
    }
}

/// The pruning counters of a scan, shared by all its partitions.
#[derive(Debug, Clone)]
pub(super) struct PruningMetrics {
    chunks_pruned: Count,
    rows_pruned: Count,
}

impl PruningMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            chunks_pruned: MetricBuilder::new(metrics).global_counter("chunks_pruned"),
            rows_pruned: MetricBuilder::new(metrics).global_counter("rows_pruned"),
        }
    }
}

impl Default for PruningMetrics {
    /// Counters that no metrics set shows.
    fn default() -> Self {
        Self {
            chunks_pruned: Count::new(),
            rows_pruned: Count::new(),
        }
    }
}
