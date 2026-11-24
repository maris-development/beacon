//! TIFF on the ND framework.

mod dataset;
mod opener;
mod variable;

pub use opener::TiffOpener;

/// The dimension of the image rows.
const DIM_Y: &str = "y";
/// The dimension of the image columns.
const DIM_X: &str = "x";
/// The latitude of each image row, on `y`.
const GEO_LAT: &str = "geo.lat";
/// The longitude of each image column, on `x`.
const GEO_LON: &str = "geo.lon";

/// The column name of band `index`. The first band is `band.0`.
fn band_name(index: usize) -> String {
    format!("band.{index}")
}
