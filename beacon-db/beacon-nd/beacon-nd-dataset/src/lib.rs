//! The dataset contract of the Beacon ND framework.
//!
//! A format crate implements this contract for one opened file.
#![warn(missing_docs)]

pub mod dataset;
pub mod error;
pub mod region;
pub mod schema;

pub use dataset::{NdDataset, NdVariable, validate};
pub use error::{Error, Result};
pub use region::Region;
pub use schema::{DatasetSchema, VariableInfo};
