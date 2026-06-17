//! Array backend implementations used by NetCDF readers.

use std::sync::Arc;

use crate::decoders::VariableDecoder;
use beacon_nd_array::{
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::NdArrayType,
};

/// Backend that reads variable data lazily from a NetCDF file.
#[derive(Debug)]
pub struct VariableBackend<T: NdArrayType + 'static> {
    decoder: Arc<dyn VariableDecoder<T>>,
    nc_file: Arc<netcdf::File>,
    shape: Vec<usize>,
    dimensions: Vec<String>,
}

impl<T: NdArrayType + 'static> VariableBackend<T> {
    /// Create a lazy variable backend.
    pub fn new(
        decoder: Arc<dyn VariableDecoder<T>>,
        nc_file: Arc<netcdf::File>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> Self {
        Self {
            decoder,
            nc_file,
            shape,
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
