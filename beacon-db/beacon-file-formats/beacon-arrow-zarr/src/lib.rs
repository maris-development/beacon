//! `beacon-arrow-zarr` reads Zarr v3 groups through the Beacon ND framework.
//! It also writes query results as Zarr stores.

pub mod config;
pub mod data_types;
pub mod datafusion;
pub mod nd;
pub mod reader;
#[cfg(test)]
pub(crate) mod testing;
pub mod util;
pub mod writer;

pub use config::ZarrConfig;
