//! TIFF metadata: the object store reader, the image IFD and the pixel coordinates.

use std::{ops::Range, panic::AssertUnwindSafe, sync::Arc};

use async_tiff::ImageFileDirectory;
use async_tiff::metadata::{TiffMetadataReader, cache::ReadaheadMetadataCache};
use async_tiff::reader::{AsyncFileReader, Endianness};
use futures::FutureExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

#[derive(Clone)]
pub(crate) struct ObjectStoreAsyncReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ObjectStoreAsyncReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }
}

impl std::fmt::Debug for ObjectStoreAsyncReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreAsyncReader")
            .field("path", &self.path)
            .finish()
    }
}

#[async_trait::async_trait]
impl AsyncFileReader for ObjectStoreAsyncReader {
    async fn get_bytes(
        &self,
        range: Range<u64>,
    ) -> async_tiff::error::AsyncTiffResult<bytes::Bytes> {
        // Object stores reject an empty range, so answer it here.
        if range.is_empty() {
            return Ok(bytes::Bytes::new());
        }
        self.store.get_range(&self.path, range).await.map_err(|e| {
            async_tiff::error::AsyncTiffError::General(format!(
                "Object store read failed for '{}': {}",
                self.path, e
            ))
        })
    }
}

/// Join a list of numbers with commas.
pub(crate) fn format_f64_list(values: &[f64]) -> String {
    values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Derive 1-D coordinates from GeoTIFF geolocation tags.
///
/// Returns `(lon, lat)`: `lon` has `image_width` values, `lat` has `image_height` values.
/// Each value is the center of its pixel, the same as a CF coordinate.
///
/// Returns `None` when no supported geolocation tags are found, or when the
/// transformation involves a rotation that cannot be collapsed to 1-D axes.
pub(crate) fn coordinates(ifd: &ImageFileDirectory) -> Option<(Vec<f64>, Vec<f64>)> {
    let image_width = ifd.image_width() as usize;
    let image_height = ifd.image_height() as usize;

    // For PixelIsArea, raster point (i, j) is the top-left corner of pixel (i, j),
    // so its center is at (i + 0.5, j + 0.5). For PixelIsPoint, it is the center.
    let center = pixel_center_offset(ifd);

    // Tiepoint + pixel scale (most common GeoTIFF encoding).
    // Formula: lon[x] = tie_wx + (x + center - tie_px) * scale_x
    //          lat[y] = tie_wy - (y + center - tie_py) * scale_y
    if let (Some(tiepoints), Some(pixel_scale)) = (ifd.model_tiepoint(), ifd.model_pixel_scale())
        && tiepoints.len() >= 6
        && pixel_scale.len() >= 2
    {
        let (tie_px, tie_py, tie_wx, tie_wy) =
            (tiepoints[0], tiepoints[1], tiepoints[3], tiepoints[4]);
        let (scale_x, scale_y) = (pixel_scale[0], pixel_scale[1]);
        let lons = (0..image_width)
            .map(|x| tie_wx + (x as f64 + center - tie_px) * scale_x)
            .collect();
        let lats = (0..image_height)
            .map(|y| tie_wy - (y as f64 + center - tie_py) * scale_y)
            .collect();
        return Some((lons, lats));
    }

    // Model transformation matrix (4×4 affine, row-major).
    // Only supported for rectilinear (non-rotated) grids where the off-diagonal
    // terms b (transform[1]) and e (transform[4]) are zero.
    // lon[x] = a * (x + center) + d
    // lat[y] = f * (y + center) + h
    if let Some(transform) = ifd.model_transformation()
        && transform.len() >= 16
    {
        let (a, b, d) = (transform[0], transform[1], transform[3]);
        let (e, f_coeff, h) = (transform[4], transform[5], transform[7]);
        if b.abs() < 1e-10 && e.abs() < 1e-10 {
            let lons = (0..image_width)
                .map(|x| a * (x as f64 + center) + d)
                .collect();
            let lats = (0..image_height)
                .map(|y| f_coeff * (y as f64 + center) + h)
                .collect();
            return Some((lons, lats));
        }
    }

    None
}

/// The raster offset from a tiepoint to a pixel center, from `GTRasterTypeGeoKey`.
///
/// Returns 0.5 for PixelIsArea (1), and 0 for PixelIsPoint (2). GeoTIFF makes
/// PixelIsArea the default, so a missing or unknown key also gives 0.5.
fn pixel_center_offset(ifd: &ImageFileDirectory) -> f64 {
    const PIXEL_IS_AREA: u16 = 1;
    const PIXEL_IS_POINT: u16 = 2;
    match ifd.geo_key_directory().and_then(|keys| keys.raster_type) {
        Some(PIXEL_IS_POINT) => 0.0,
        Some(PIXEL_IS_AREA) | None => 0.5,
        Some(other) => {
            tracing::warn!(
                raster_type = other,
                "unknown GTRasterTypeGeoKey, using PixelIsArea"
            );
            0.5
        }
    }
}

/// Read the IFDs and return the full-resolution image and the byte order.
///
/// `async-tiff` panics on some malformed IFDs. The panic becomes an error here,
/// so a bad file fails its query and not the whole server task.
pub(crate) async fn read_image_ifd(
    reader: &ObjectStoreAsyncReader,
) -> anyhow::Result<(ImageFileDirectory, Endianness)> {
    let cached_reader = ReadaheadMetadataCache::new(reader.clone());
    let read = async {
        let mut metadata_reader = TiffMetadataReader::try_open(&cached_reader)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open TIFF metadata: {e}"))?;
        let endianness = metadata_reader.endianness();
        let ifds = metadata_reader
            .read_all_ifds(&cached_reader)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read TIFF metadata IFDs: {e}"))?;
        Ok::<_, anyhow::Error>((ifds, endianness))
    };
    let (ifds, endianness) = AssertUnwindSafe(read)
        .catch_unwind()
        .await
        .map_err(|panic| {
            let reason = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown cause".to_string());
            anyhow::anyhow!("Failed to read TIFF metadata IFDs: malformed IFD ({reason})")
        })??;

    // NewSubfileType bit 0 marks an overview and bit 2 marks a mask. Skip both.
    let is_full_image = |ifd: &ImageFileDirectory| ifd.new_subfile_type().unwrap_or(0) & 0b101 == 0;
    let index = ifds.iter().position(is_full_image).unwrap_or(0);
    let ifd = ifds
        .into_iter()
        .nth(index)
        .ok_or_else(|| anyhow::anyhow!("TIFF contains no image file directories (IFDs)."))?;
    Ok((ifd, endianness))
}
