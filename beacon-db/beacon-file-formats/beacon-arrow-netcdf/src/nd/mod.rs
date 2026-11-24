//! NetCDF on the ND framework.

mod attributes;
mod dataset;
mod decode;
mod opener;
mod variable;

pub use attributes::attributes_of;
pub use opener::NetcdfOpener;
pub use variable::{stored_length, NetcdfVariable};
