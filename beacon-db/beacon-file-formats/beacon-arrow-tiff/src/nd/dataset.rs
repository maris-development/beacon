//! The variables of one TIFF file on the ND contract.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, UInt16Array, UInt32Array, UInt64Array};
use async_tiff::ImageFileDirectory;
use async_tiff::tags::SampleFormat;
use beacon_nd_dataset::{
    DatasetSchema, Dimension, Dimensions, Error, MemoryVariable, NdArrowArray, NdVariable, Result,
};

use super::variable::TiffBand;
use super::{DIM_X, DIM_Y, GEO_LAT, GEO_LON};
use crate::block::{BlockLayout, TiffImage, TiffSample};
use crate::reader::{ObjectStoreAsyncReader, coordinates, format_f64_list, nodata_text};

type Variables = Vec<Arc<dyn NdVariable>>;

/// The dataset of the image `ifd`: the bands, the coordinates and the tags, sorted by name.
///
/// The bands are on the dimensions `y` and `x`, `geo.lat` on `y` and `geo.lon` on `x`.
/// The tags are rank-0 variables.
pub(crate) fn dataset_of(
    name: &str,
    ifd: &ImageFileDirectory,
    layout: BlockLayout,
    reader: ObjectStoreAsyncReader,
) -> Result<DatasetSchema> {
    let dimensions = Dimensions::try_new([
        Dimension::new(DIM_Y, layout.image_height),
        Dimension::new(DIM_X, layout.image_width),
    ])?;

    let mut variables = tag_variables(ifd, &layout)?;
    variables.extend(coordinate_variables(ifd)?);
    variables.extend(band_variables(layout, reader, nodata_text(ifd)).map_err(Error::format)?);
    variables.sort_by(|a, b| a.name().cmp(b.name()));

    DatasetSchema::try_new(name, dimensions, variables)
}

/// A rank-0 variable with the one value of `value`.
fn scalar(name: &str, value: ArrayRef) -> Result<Arc<dyn NdVariable>> {
    let array = NdArrowArray::try_new_scalar(value)?;
    Ok(Arc::new(MemoryVariable::new(name, array)))
}

fn u16_tag(name: &str, value: u16) -> Result<Arc<dyn NdVariable>> {
    scalar(name, Arc::new(UInt16Array::from(vec![value])))
}

fn u32_tag(name: &str, value: u32) -> Result<Arc<dyn NdVariable>> {
    scalar(name, Arc::new(UInt32Array::from(vec![value])))
}

fn u64_tag(name: &str, value: u64) -> Result<Arc<dyn NdVariable>> {
    scalar(name, Arc::new(UInt64Array::from(vec![value])))
}

fn utf8_tag(name: &str, value: impl Into<String>) -> Result<Arc<dyn NdVariable>> {
    scalar(name, Arc::new(StringArray::from(vec![value.into()])))
}

/// The `image.*` and `geo.*` tag variables. A tag that the file lacks gives no variable.
fn tag_variables(ifd: &ImageFileDirectory, layout: &BlockLayout) -> Result<Variables> {
    let mut tags = vec![
        u32_tag("image.width", ifd.image_width())?,
        u32_tag("image.height", ifd.image_height())?,
        u16_tag("image.samples_per_pixel", ifd.samples_per_pixel())?,
    ];
    if let Some(&bits) = ifd.bits_per_sample().first() {
        tags.push(u16_tag("image.bits_per_sample", bits)?);
    }
    if layout.tiled {
        tags.push(u32_tag("image.tile_width", layout.block_width as u32)?);
        tags.push(u32_tag("image.tile_height", layout.block_height as u32)?);
        tags.push(u64_tag("image.tile_count_x", layout.blocks_across as u64)?);
        tags.push(u64_tag("image.tile_count_y", layout.blocks_down as u64)?);
    }
    if let Some(epsg) = ifd.geo_key_directory().and_then(|keys| keys.epsg_code()) {
        tags.push(u16_tag("geo.epsg", epsg)?);
        tags.push(utf8_tag("geo.crs", format!("EPSG:{epsg}"))?);
    }
    if let Some(values) = ifd.model_pixel_scale() {
        tags.push(utf8_tag("geo.model_pixel_scale", format_f64_list(values))?);
    }
    if let Some(values) = ifd.model_tiepoint() {
        tags.push(utf8_tag("geo.model_tiepoint", format_f64_list(values))?);
    }
    if let Some(values) = ifd.model_transformation() {
        tags.push(utf8_tag(
            "geo.model_transformation",
            format_f64_list(values),
        )?);
    }
    if let Some(nodata) = nodata_text(ifd) {
        tags.push(utf8_tag("geo.nodata", nodata)?);
    }
    if let Some(metadata) = ifd.gdal_metadata() {
        tags.push(utf8_tag("geo.gdal_metadata", metadata)?);
    }
    Ok(tags)
}

/// `geo.lat` on `y` and `geo.lon` on `x`. Empty when the file has no usable geolocation.
fn coordinate_variables(ifd: &ImageFileDirectory) -> Result<Variables> {
    fn axis(name: &str, dimension: &str, values: Vec<f64>) -> Result<Arc<dyn NdVariable>> {
        let dimensions = Dimensions::try_new([Dimension::new(dimension, values.len())])?;
        let array = NdArrowArray::try_new(Arc::new(Float64Array::from(values)), dimensions)?;
        Ok(Arc::new(MemoryVariable::new(name, array)))
    }

    let Some((lons, lats)) = coordinates(ifd) else {
        return Ok(Vec::new());
    };
    Ok(vec![
        axis(GEO_LAT, DIM_Y, lats)?,
        axis(GEO_LON, DIM_X, lons)?,
    ])
}

/// One band variable per sample of the image. No pixel data is fetched here.
///
/// The arms agree with the `TiffSample` implementations in `block.rs`.
fn band_variables(
    layout: BlockLayout,
    reader: ObjectStoreAsyncReader,
    nodata: Option<&str>,
) -> anyhow::Result<Variables> {
    fn make<T: TiffSample>(
        layout: BlockLayout,
        reader: ObjectStoreAsyncReader,
        nodata: Option<&str>,
    ) -> anyhow::Result<Variables> {
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
