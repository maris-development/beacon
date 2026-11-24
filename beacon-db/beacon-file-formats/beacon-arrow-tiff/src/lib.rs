//! `beacon-arrow-tiff` reads TIFF/GeoTIFF files through the Beacon ND framework.

pub(crate) mod block;
mod nd;
pub(crate) mod reader;

pub use nd::TiffOpener;
