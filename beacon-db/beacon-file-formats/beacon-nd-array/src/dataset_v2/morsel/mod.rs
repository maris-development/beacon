//! Morsels over `dataset_v2` datasets: one chunk of a grid, or one range of casts.
//!
//! A format's [`MorselPlanner`](beacon_datafusion_ext::morsel_planner::MorselPlanner)
//! opens a file, builds a [`GriddedDataset`](super::gridded::GriddedDataset) or a
//! [`RaggedDataset`](super::ragged::RaggedDataset), and cuts it into these morsels.

mod common;
mod gridded;
mod ragged;
#[cfg(test)]
pub(crate) mod test_util;

pub use gridded::GriddedChunkMorsel;
pub use ragged::{OBSERVATION_DIMENSION, RaggedCastsMorsel};
