use std::sync::Arc;

use beacon_nd_array::array::{backend::ArrayBackend, subset::ArraySubset};
use futures::StreamExt;

use crate::block::{TiffImage, TiffSample};

/// The maximum number of blocks that one subset read fetches at the same time.
const MAX_CONCURRENT_BLOCKS: usize = 16;

/// Lazy backend for one band of a tiled or stripped TIFF image.
///
/// No data is fetched until [`read_subset`] is called. Each call fetches only
/// the blocks that intersect the requested region. Pixels of a sparse block get
/// the fill value, or 0 when the image has no nodata value.
pub(crate) struct TiffBandBackend<T: TiffSample> {
    pub(crate) image: Arc<TiffImage<T>>,
    pub(crate) band: usize,
    pub(crate) fill_value: Option<T>,
}

impl<T: TiffSample> std::fmt::Debug for TiffBandBackend<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiffBandBackend")
            .field("image", &self.image)
            .field("band", &self.band)
            .finish()
    }
}

#[async_trait::async_trait]
impl<T: TiffSample> ArrayBackend<T> for TiffBandBackend<T> {
    fn len(&self) -> usize {
        self.image.layout.image_width * self.image.layout.image_height
    }

    fn shape(&self) -> Vec<usize> {
        vec![
            self.image.layout.image_height,
            self.image.layout.image_width,
        ]
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.image.layout.chunk_shape()
    }

    fn dimensions(&self) -> Vec<String> {
        vec!["y".to_string(), "x".to_string()]
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        self.validate_subset(&subset)?;

        let layout = &self.image.layout;
        let (row_start, col_start) = (subset.start[0], subset.start[1]);
        let (n_rows, n_cols) = (subset.shape[0], subset.shape[1]);
        let mut result = vec![self.fill_value.unwrap_or_default(); n_rows * n_cols];

        if n_rows > 0 && n_cols > 0 {
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
                // `None` is a sparse block. Its pixels keep the fill value.
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
        }

        ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[n_rows, n_cols]), result)
            .map_err(|e| anyhow::anyhow!("Failed to build result array: {e}"))
    }
}
