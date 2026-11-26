//! NetCDF on the ND framework.

mod attributes;
mod dataset;
mod decode;
mod opener;
mod variable;

pub use attributes::attributes_of;
pub use opener::NetcdfOpener;
pub use variable::{pad, stored_length, NetcdfVariable};
