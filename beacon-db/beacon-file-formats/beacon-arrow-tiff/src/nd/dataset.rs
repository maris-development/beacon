//! The variables of one TIFF file on the ND contract.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, UInt16Array, UInt32Array, UInt64Array};
use async_tiff::ImageFileDirectory;
use async_tiff::reader::Endianness;
use async_tiff::tags::SampleFormat;
use beacon_nd_dataset::{
    DatasetSchema, Dimension, Dimensions, Error, MemoryVariable, NdArrowArray, NdVariable, Result,
};

use super::variable::TiffBand;
use crate::block::{BlockLayout, TiffImage, TiffSample};
use crate::reader::{ObjectStoreAsyncReader, coordinates, format_f64_list};

/// The dataset of the image `ifd`: the bands, the coordinates and the tags, sorted by name.
///
/// The bands are on the dimensions `y` and `x`, `geo.lat` on `y` and `geo.lon` on `x`.
/// The tags are rank-0 variables.
pub(crate) fn dataset_of(
    name: &str,
    ifd: &ImageFileDirectory,
    endianness: Endianness,
    reader: ObjectStoreAsyncReader,
) -> Result<DatasetSchema> {
    let layout = BlockLayout::from_ifd(ifd, endianness)
        .map_err(|e| Error::format(e.context("Failed to read TIFF block layout")))?;
    let (height, width) = (layout.image_height, layout.image_width);

    let mut variables: Vec<Arc<dyn NdVariable>> = Vec::new();
    let mut scalar = |name: &str, value: ArrayRef| -> Result<()> {
        let array = NdArrowArray::try_new_scalar(value)?;
        variables.push(Arc::new(MemoryVariable::new(name, array)));
        Ok(())
    };
    let utf8 = |text: String| Arc::new(StringArray::from(vec![text])) as ArrayRef;

    scalar(
        "image.width",
        Arc::new(UInt32Array::from(vec![ifd.image_width()])),
    )?;
    scalar(
        "image.height",
        Arc::new(UInt32Array::from(vec![ifd.image_height()])),
    )?;
    scalar(
        "image.samples_per_pixel",
        Arc::new(UInt16Array::from(vec![ifd.samples_per_pixel()])),
    )?;
    if let Some(&bits) = ifd.bits_per_sample().first() {
        scalar(
            "image.bits_per_sample",
            Arc::new(UInt16Array::from(vec![bits])),
        )?;
    }
    if layout.tiled {
        scalar(
            "image.tile_width",
            Arc::new(UInt32Array::from(vec![layout.block_width as u32])),
        )?;
        scalar(
            "image.tile_height",
            Arc::new(UInt32Array::from(vec![layout.block_height as u32])),
        )?;
        scalar(
            "image.tile_count_x",
            Arc::new(UInt64Array::from(vec![layout.blocks_across as u64])),
        )?;
        scalar(
            "image.tile_count_y",
            Arc::new(UInt64Array::from(vec![layout.blocks_down as u64])),
        )?;
    }
    if let Some(epsg) = ifd.geo_key_directory().and_then(|keys| keys.epsg_code()) {
        scalar("geo.epsg", Arc::new(UInt16Array::from(vec![epsg])))?;
        scalar("geo.crs", utf8(format!("EPSG:{epsg}")))?;
    }
    if let Some(values) = ifd.model_pixel_scale() {
        scalar("geo.model_pixel_scale", utf8(format_f64_list(values)))?;
    }
    if let Some(values) = ifd.model_tiepoint() {
        scalar("geo.model_tiepoint", utf8(format_f64_list(values)))?;
    }
    if let Some(values) = ifd.model_transformation() {
        scalar("geo.model_transformation", utf8(format_f64_list(values)))?;
    }

    // GDAL ends the tag with a NUL byte, and some writers add spaces.
    let nodata = ifd
        .gdal_nodata()
        .map(|s| s.trim_matches(|c: char| c == '\0' || c.is_whitespace()))
        .filter(|s| !s.is_empty());
    if let Some(nodata) = nodata {
        scalar("geo.nodata", utf8(nodata.to_string()))?;
    }
    if let Some(metadata) = ifd.gdal_metadata() {
        scalar("geo.gdal_metadata", utf8(metadata.to_string()))?;
    }

    if let Some((lons, lats)) = coordinates(ifd) {
        let axis = |name: &str, dimension: &str, values: Vec<f64>| -> Result<Arc<dyn NdVariable>> {
            let dimensions = Dimensions::try_new([Dimension::new(dimension, values.len())])?;
            let values = Arc::new(Float64Array::from(values)) as ArrayRef;
            let array = NdArrowArray::try_new(values, dimensions)?;
            Ok(Arc::new(MemoryVariable::new(name, array)))
        };
        variables.push(axis("geo.lat", "y", lats)?);
        variables.push(axis("geo.lon", "x", lons)?);
    }

    variables.extend(bands(layout, reader, nodata).map_err(Error::format)?);
    variables.sort_by(|a, b| a.name().cmp(b.name()));

    let dimensions =
        Dimensions::try_new([Dimension::new("y", height), Dimension::new("x", width)])?;
    DatasetSchema::try_new(name, dimensions, variables)
}

/// One band variable per sample of the image. No pixel data is fetched here.
fn bands(
    layout: BlockLayout,
    reader: ObjectStoreAsyncReader,
    nodata: Option<&str>,
) -> anyhow::Result<Vec<Arc<dyn NdVariable>>> {
    fn make<T: TiffSample>(
        layout: BlockLayout,
        reader: ObjectStoreAsyncReader,
        nodata: Option<&str>,
    ) -> anyhow::Result<Vec<Arc<dyn NdVariable>>> {
        let nodata = nodata.and_then(|text| {
            let value = T::parse_nodata(text);
            if value.is_none() {
                tracing::warn!(value = %text, "ignoring GDAL_NODATA value that the band type cannot hold");
            }
            value
        });
        let n_bands = layout.n_bands;
        let image = Arc::new(TiffImage::<T>::new(reader, layout)?);
        (0..n_bands)
            .map(|band| {
                let band = TiffBand::new(Arc::clone(&image), band, nodata)?;
                Ok(Arc::new(band) as Arc<dyn NdVariable>)
            })
            .collect()
    }

    match (layout.sample_format, layout.bits_per_sample) {
        (SampleFormat::Float, 32) => make::<f32>(layout, reader, nodata),
        (SampleFormat::Float, 64) => make::<f64>(layout, reader, nodata),
        (SampleFormat::Uint, 8) => make::<u8>(layout, reader, nodata),
        (SampleFormat::Uint, 16) => make::<u16>(layout, reader, nodata),
        (SampleFormat::Uint, 32) => make::<u32>(layout, reader, nodata),
        (SampleFormat::Uint, 64) => make::<u64>(layout, reader, nodata),
        (SampleFormat::Int, 8) => make::<i8>(layout, reader, nodata),
        (SampleFormat::Int, 16) => make::<i16>(layout, reader, nodata),
        (SampleFormat::Int, 32) => make::<i32>(layout, reader, nodata),
        (SampleFormat::Int, 64) => make::<i64>(layout, reader, nodata),
        (sample_format, bits) => {
            anyhow::bail!("Unsupported TIFF format: {sample_format:?} / {bits} bits per sample")
        }
    }
}
