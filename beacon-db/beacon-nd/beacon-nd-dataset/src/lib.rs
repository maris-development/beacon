//! The dataset contract of the Beacon ND framework.
//!
//! A format crate implements this contract for one opened file.
#![warn(missing_docs)]

pub mod attributes;
pub mod dataset;
pub mod error;
pub mod grid;
pub mod memory;
pub mod region;
pub mod schema;
pub mod statistics;
pub mod view;

pub use attributes::attribute_columns;
pub use dataset::{NdDataset, NdVariable, validate};
pub use error::{Error, Result};
pub use grid::ChunkGrid;
pub use memory::{MemoryDataset, MemoryVariable};
pub use region::Region;
pub use schema::{DatasetSchema, VariableInfo};
pub use statistics::value_range;
pub use view::{GridSelection, default_grid, select_columns, select_grid};
