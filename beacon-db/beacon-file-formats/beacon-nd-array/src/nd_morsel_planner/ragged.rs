//! Planning of a ragged file: one morsel per range of casts.

use std::{ops::Range, sync::Arc};

use beacon_datafusion_ext::morsel_planner::Morsel;
use datafusion::error::Result;
use indexmap::IndexMap;

use super::{NdMorselPlanner, external};
use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    dataset_v2::{morsel::RaggedCastsMorsel, ragged::RaggedDataset},
};

impl NdMorselPlanner {
    /// Cut a ragged dataset into one morsel per range of casts.
    pub(super) async fn plan_ragged(
        &self,
        dataset: RaggedDataset,
        unprojected: Option<&IndexMap<String, Arc<dyn NdArrayD>>>,
    ) -> Result<Vec<Box<dyn Morsel>>> {
        let mut ranges = dataset
            .cast_ranges(self.batch_size)
            .await
            .map_err(external)?;
        let offsets = Arc::new(dataset.row_offsets().await.map_err(external)?);

        if let Some(masks) = self.pruner.masks(unprojected, ranges.len()).await? {
            let instance_dimension = std::slice::from_ref(&dataset.instance_dimension);
            let (kept, pruned): (Vec<_>, Vec<_>) = ranges.into_iter().partition(|casts| {
                let subset = ArraySubset::new(vec![casts.start], vec![casts.len()]);
                masks.keeps(instance_dimension, &subset)
            });
            // The rows of a range are the most observations one row-size array counts for it.
            let rows = |casts: &Range<usize>| {
                offsets
                    .values()
                    .map(|o| o[casts.end] - o[casts.start])
                    .max()
                    .unwrap_or(0)
            };
            self.pruner
                .record(pruned.len(), pruned.iter().map(rows).sum());
            ranges = kept;
        }

        let dataset = Arc::new(dataset);
        Ok(ranges
            .into_iter()
            .map(|casts| {
                let morsel = RaggedCastsMorsel::new(
                    Arc::clone(&dataset),
                    casts,
                    Arc::clone(&offsets),
                    Arc::clone(&self.table_schema),
                );
                Box::new(morsel) as Box<dyn Morsel>
            })
            .collect())
    }
}
