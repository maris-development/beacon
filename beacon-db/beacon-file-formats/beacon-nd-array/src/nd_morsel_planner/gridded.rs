//! Planning of a gridded file: one morsel per chunk of its grid.

use std::sync::Arc;

use beacon_datafusion_ext::morsel_planner::Morsel;
use datafusion::error::Result;
use indexmap::IndexMap;

use super::NdMorselPlanner;
use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    dataset_v2::{gridded::GriddedDataset, morsel::GriddedChunkMorsel},
};

impl NdMorselPlanner {
    /// Cut a gridded dataset into one morsel per chunk of its grid.
    pub(super) async fn plan_gridded(
        &self,
        dataset: GriddedDataset,
        unprojected: Option<&IndexMap<String, Arc<dyn NdArrayD>>>,
    ) -> Result<Vec<Box<dyn Morsel>>> {
        let mut chunks = dataset.chunks(self.batch_size);
        if let Some(masks) = self.pruner.masks(unprojected, chunks.len()).await? {
            let (kept, pruned): (Vec<_>, Vec<_>) = chunks
                .into_iter()
                .partition(|chunk| masks.keeps(&dataset.grid_dimensions, chunk));
            let pruned_rows = pruned.iter().map(ArraySubset::rows).sum();
            self.pruner.record(pruned.len(), pruned_rows);
            chunks = kept;
        }

        let dataset = Arc::new(dataset);
        Ok(chunks
            .into_iter()
            .map(|subset| {
                let morsel = GriddedChunkMorsel::new(
                    Arc::clone(&dataset),
                    subset,
                    Arc::clone(&self.table_schema),
                );
                Box::new(morsel) as Box<dyn Morsel>
            })
            .collect())
    }
}
