//! `beacon-arrow-tiff` reads TIFF/GeoTIFF files through the Beacon ND framework.

pub(crate) mod block;
/// DataFusion integration for TIFF file scanning.
pub mod datafusion;
mod nd;
pub(crate) mod reader;

pub use nd::TiffOpener;
