//! One band of a TIFF image on the ND contract.

use std::sync::Arc;

use arrow::array::PrimitiveArray;
use arrow::buffer::NullBuffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use async_trait::async_trait;
use beacon_nd_dataset::{
    Attributes, Dimension, Dimensions, Error, NdArrowArray, NdVariable, Region, Result,
    VariableInfo,
};

use super::{DIM_X, DIM_Y, band_name};
use crate::block::{TiffImage, TiffSample};

/// One band of the image, on the dimensions `y` and `x`.
///
/// All bands share one [`TiffImage`], so the bands of a pixel-interleaved image
/// decode each block once.
pub(crate) struct TiffBand<T: TiffSample> {
    info: VariableInfo,
    image: Arc<TiffImage<T>>,
    band: usize,
    nodata: Option<T>,
}

impl<T: TiffSample> TiffBand<T> {
    /// Band `band` of `image`. A pixel equal to `nodata` reads as null.
    pub(crate) fn new(image: Arc<TiffImage<T>>, band: usize, nodata: Option<T>) -> Result<Self> {
        let layout = &image.layout;
        let dimensions = Dimensions::try_new([
            Dimension::new(DIM_Y, layout.image_height),
            Dimension::new(DIM_X, layout.image_width),
        ])?;
        let info = VariableInfo::new(band_name(band), T::Arrow::DATA_TYPE, dimensions)
            .with_chunk_shape(layout.chunk_shape());
        Ok(Self {
            info,
            image,
            band,
            nodata,
        })
    }
}

impl<T: TiffSample> std::fmt::Debug for TiffBand<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiffBand")
            .field("name", &self.info.name)
            .field("image", &self.image)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: TiffSample> NdVariable for TiffBand<T> {
    fn name(&self) -> &str {
        &self.info.name
    }

    fn data_type(&self) -> &DataType {
        &self.info.data_type
    }

    fn dimensions(&self) -> &Dimensions {
        &self.info.dimensions
    }

    fn chunk_shape(&self) -> Option<&[usize]> {
        self.info.chunk_shape.as_deref()
    }

    fn attributes(&self) -> &Attributes {
        &self.info.attributes
    }

    async fn read(&self, region: &Region) -> Result<NdArrowArray> {
        region.check(self.name(), self.dimensions())?;
        let (start, shape) = (region.start(), region.shape());
        let rows = start[0]..start[0] + shape[0];
        let cols = start[1]..start[1] + shape[1];
        // A sparse block gets nodata, so it reads as null. Without nodata it gets 0.
        let fill = self.nodata.unwrap_or_default();
        let samples = self
            .image
            .read_window(self.band, rows, cols, fill)
            .await
            .map_err(Error::format)?;

        let nulls = self.nodata.map(|nodata| {
            let valid: Vec<bool> = samples
                .iter()
                .map(|value| !value.is_nodata(nodata))
                .collect();
            NullBuffer::from(valid)
        });
        let values = PrimitiveArray::<T::Arrow>::new(samples.into(), nulls);
        let dimensions = Dimensions::try_new([
            Dimension::new(DIM_Y, shape[0]),
            Dimension::new(DIM_X, shape[1]),
        ])?;
        Ok(NdArrowArray::try_new(Arc::new(values), dimensions)?)
    }
}
