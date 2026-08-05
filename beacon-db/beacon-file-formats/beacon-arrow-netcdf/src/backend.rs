//! Array backend implementations used by NetCDF readers.

use std::sync::Arc;

use crate::decoders::VariableDecoder;
use beacon_nd_array::{
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::NdArrayType,
};

/// The chunk shape the file stores for a variable.
///
/// The read costs no bytes. A netCDF-4 file keeps the layout of each variable
/// in its header, which the open already read.
///
/// The value falls back to `shape`:
///
/// - for a contiguous variable, and for a classic file, which has no chunks;
/// - for a chunk shape of a lower rank than `shape`, which cannot map onto it.
///
/// A fixed-size string variable drops its length axis from `shape`. The chunk
/// shape drops the same axis, so both keep the same rank.
fn file_chunk_shape(nc_file: &netcdf::File, variable_name: &str, shape: &[usize]) -> Vec<usize> {
    let chunks = nc_file
        .variable(variable_name)
        .and_then(|variable| variable.chunking().ok().flatten());

    match chunks {
        Some(chunks) if chunks.len() >= shape.len() => chunks[..shape.len()].to_vec(),
        _ => shape.to_vec(),
    }
}

/// Backend that reads variable data lazily from a NetCDF file.
#[derive(Debug)]
pub struct VariableBackend<T: NdArrayType + 'static> {
    decoder: Arc<dyn VariableDecoder<T>>,
    nc_file: Arc<netcdf::File>,
    shape: Vec<usize>,
    chunk_shape: Vec<usize>,
    dimensions: Vec<String>,
}

impl<T: NdArrayType + 'static> VariableBackend<T> {
    /// Create a lazy variable backend.
    ///
    /// The chunk shape comes from the file once, here. A later read of it costs
    /// nothing.
    pub fn new(
        decoder: Arc<dyn VariableDecoder<T>>,
        nc_file: Arc<netcdf::File>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> Self {
        let chunk_shape = file_chunk_shape(&nc_file, decoder.variable_name(), &shape);
        Self {
            decoder,
            nc_file,
            shape,
            chunk_shape,
            dimensions,
        }
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType + 'static> ArrayBackend<T> for VariableBackend<T> {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }
    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }
    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }
    fn fill_value(&self) -> Option<T> {
        self.decoder.fill_value()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        let var_name = self.decoder.variable_name();
        let var = self
            .nc_file
            .variable(var_name)
            .ok_or_else(|| anyhow::anyhow!("Variable '{}' not found in NetCDF file", var_name))?;

        // translate subset to netcdf extents
        let mut extents = vec![];
        for axis in 0..self.shape.len() {
            let start = subset.start.get(axis).copied().ok_or(anyhow::anyhow!(
                "Variable '{}' subset is missing start for axis {}",
                var_name,
                axis
            ))?;
            let len = subset.shape.get(axis).copied().ok_or(anyhow::anyhow!(
                "Variable '{}' subset is missing length for axis {}",
                var_name,
                axis
            ))?;
            extents.push(netcdf::Extent::from(start..start + len));
        }

        self.decoder.read(&var, netcdf::Extents::Extent(extents))
    }
}

/// Backend for scalar attribute values surfaced as rank-0 arrays.
#[derive(Debug)]
pub struct AttributeBackend<T: NdArrayType> {
    value: T,
}

impl<T: NdArrayType> AttributeBackend<T> {
    /// Create an attribute backend from a single scalar value.
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType + Clone> ArrayBackend<T> for AttributeBackend<T> {
    fn len(&self) -> usize {
        1
    }

    fn shape(&self) -> Vec<usize> {
        vec![]
    }
    fn dimensions(&self) -> Vec<String> {
        vec![]
    }
    fn fill_value(&self) -> Option<T> {
        None
    }
    async fn read_subset(&self, _subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        Ok(ndarray::arr0(self.value.clone()).into_dyn())
    }
}
