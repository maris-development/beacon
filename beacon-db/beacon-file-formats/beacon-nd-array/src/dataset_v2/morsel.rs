//! Morsels over `dataset_v2` datasets: one chunk of a grid, or one range of casts.
//!
//! A format's [`MorselPlanner`](beacon_datafusion_ext::morsel_planner::MorselPlanner)
//! opens a file, builds a [`GriddedDataset`] or a [`RaggedDataset`], and cuts it
//! into these morsels with [`GriddedDataset::chunks`] or [`RaggedDataset::cast_ranges`].

use std::{fmt, ops::Range, sync::Arc};

use beacon_datafusion_ext::{morsel_planner::Morsel, nd::NdRecordBatch};
use datafusion::error::{DataFusionError, Result};
use futures::future::BoxFuture;

use crate::{
    array::subset::ArraySubset,
    dataset_v2::{gridded::GriddedDataset, ragged::RaggedDataset},
};

/// One chunk of a gridded dataset.
pub struct GriddedChunkMorsel {
    dataset: Arc<GriddedDataset>,
    subset: ArraySubset,
}

impl GriddedChunkMorsel {
    /// Create the morsel that reads `subset` of the grid of `dataset`.
    pub fn new(dataset: Arc<GriddedDataset>, subset: ArraySubset) -> Self {
        Self { dataset, subset }
    }

    /// The part of the grid that this morsel reads.
    pub fn subset(&self) -> &ArraySubset {
        &self.subset
    }
}

impl fmt::Debug for GriddedChunkMorsel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GriddedChunkMorsel")
            .field("dataset", &self.dataset.inner.name)
            .field("start", &self.subset.start)
            .field("shape", &self.subset.shape)
            .finish()
    }
}

impl Morsel for GriddedChunkMorsel {
    fn produce(self: Box<Self>) -> BoxFuture<'static, Result<NdRecordBatch>> {
        // TODO: read the chunk of each projected array into an NdRecordBatch.
        Box::pin(async {
            Err(DataFusionError::NotImplemented(
                "reading a gridded chunk morsel".to_string(),
            ))
        })
    }
}

/// One range of casts of a ragged dataset.
pub struct RaggedCastsMorsel {
    dataset: Arc<RaggedDataset>,
    casts: Range<usize>,
    columns: Arc<[String]>, // The arrays that the scan projects, or all arrays when empty
}

impl RaggedCastsMorsel {
    /// Create the morsel that reads the casts `casts` of `dataset`.
    ///
    /// # Arguments
    ///
    /// * `dataset` - The ragged dataset of one file
    /// * `casts` - The cast indices to read
    /// * `columns` - The arrays that the scan projects
    pub fn new(dataset: Arc<RaggedDataset>, casts: Range<usize>, columns: Arc<[String]>) -> Self {
        Self {
            dataset,
            casts,
            columns,
        }
    }

    /// The cast indices that this morsel reads.
    pub fn casts(&self) -> &Range<usize> {
        &self.casts
    }
}

impl fmt::Debug for RaggedCastsMorsel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaggedCastsMorsel")
            .field("dataset", &self.dataset.inner.name)
            .field("casts", &self.casts)
            .field("columns", &self.columns)
            .finish()
    }
}

impl Morsel for RaggedCastsMorsel {
    fn produce(self: Box<Self>) -> BoxFuture<'static, Result<NdRecordBatch>> {
        // TODO: read the projected arrays of the casts into an NdRecordBatch.
        Box::pin(async {
            Err(DataFusionError::NotImplemented(
                "reading a ragged casts morsel".to_string(),
            ))
        })
    }
}
