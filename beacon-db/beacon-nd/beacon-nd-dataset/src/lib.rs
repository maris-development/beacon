//! The dataset contract of the Beacon ND framework.
//!
//! A format crate implements this contract for one opened file.
#![warn(missing_docs)]

pub mod error;
pub mod region;

pub use error::{Error, Result};
pub use region::Region;
