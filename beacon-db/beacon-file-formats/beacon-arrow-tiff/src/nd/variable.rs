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
use futures::StreamExt;

use crate::block::{TiffImage, TiffSample};

/// The maximum number of blocks that one region read fetches at the same time.
const MAX_CONCURRENT_BLOCKS: usize = 16;

/// One band of the image, on the dimensions `y` and `x`.
///
/// A read fetches only the blocks that intersect the region. All bands share
/// one [`TiffImage`], so the bands of a pixel-interleaved image decode each block once.
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
            Dimension::new("y", layout.image_height),
            Dimension::new("x", layout.image_width),
        ])?;
        let info = VariableInfo::new(format!("band.{band}"), T::Arrow::DATA_TYPE, dimensions)
            .with_chunk_shape(layout.chunk_shape());
        Ok(Self {
            info,
            image,
            band,
            nodata,
        })
    }

    /// The samples of `region`, row by row. A sparse block gives nodata, or 0 without nodata.
    async fn samples(&self, region: &Region) -> anyhow::Result<Vec<T>> {
        let layout = &self.image.layout;
        let (row_start, col_start) = (region.start()[0], region.start()[1]);
        let (n_rows, n_cols) = (region.shape()[0], region.shape()[1]);
        let mut result = vec![self.nodata.unwrap_or_default(); n_rows * n_cols];
        if n_rows == 0 || n_cols == 0 {
            return Ok(result);
        }

        let (bw, bh) = (layout.block_width, layout.block_height);
        let rows = row_start / bh..=(row_start + n_rows - 1) / bh;
        let cols = col_start / bw..=(col_start + n_cols - 1) / bw;
        let coords: Vec<(usize, usize)> = rows
            .flat_map(|by| cols.clone().map(move |bx| (bx, by)))
            .collect();

        let image = &self.image;
        let band = self.band;
        let mut blocks = futures::stream::iter(coords)
            .map(|(bx, by)| async move { (bx, by, image.block(band, bx, by).await) })
            .buffer_unordered(MAX_CONCURRENT_BLOCKS);

        // A block of a pixel-interleaved image holds every band of each pixel.
        let (samples, sample) = if layout.planar {
            (1, 0)
        } else {
            (layout.n_bands, self.band)
        };

        while let Some((bx, by, block)) = blocks.next().await {
            // `None` is a sparse block. Its pixels keep the fill.
            let Some(block) = block? else { continue };
            let block_rows = block.len() / (bw * samples);

            let row_end = (row_start + n_rows).min(by * bh + block_rows);
            let col_first = col_start.max(bx * bw);
            let col_end = (col_start + n_cols).min((bx + 1) * bw);
            for row in row_start.max(by * bh)..row_end {
                let src_row = (row - by * bh) * bw;
                let dst_row = (row - row_start) * n_cols;
                for col in col_first..col_end {
                    let src = (src_row + col - bx * bw) * samples + sample;
                    result[dst_row + col - col_start] = block[src];
                }
            }
        }
        Ok(result)
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
        let samples = self.samples(region).await.map_err(Error::format)?;
        let nulls = self.nodata.map(|nodata| {
            let valid: Vec<bool> = samples
                .iter()
                .map(|value| !value.is_nodata(nodata))
                .collect();
            NullBuffer::from(valid)
        });
        let values = PrimitiveArray::<T::Arrow>::new(samples.into(), nulls);
        let dimensions = Dimensions::try_new(
            self.dimensions()
                .iter()
                .zip(region.shape())
                .map(|(dimension, &length)| Dimension::new(dimension.name(), length)),
        )?;
        Ok(NdArrowArray::try_new(Arc::new(values), dimensions)?)
    }
}
