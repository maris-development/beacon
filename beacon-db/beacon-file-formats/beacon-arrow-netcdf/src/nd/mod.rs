//! NetCDF on the ND framework.

mod attributes;
mod dataset;
mod decode;
mod opener;
mod variable;

pub use opener::NetcdfOpener;
