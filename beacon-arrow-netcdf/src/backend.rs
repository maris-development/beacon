use std::sync::Arc;

use arrow_schema::FieldRef;
use beacon_nd_arrow::array::{
    backend::ArrayBackend, compat_typings::ArrowTypeConversion, subset::ArraySubset,
};
use netcdf::NcTypeDescriptor;

use crate::decoders::VariableDecoder;

#[derive(Debug)]
pub struct VariableBackend<T: ArrowTypeConversion + NcTypeDescriptor + 'static> {
    decoder: Arc<dyn VariableDecoder<T>>,
    nc_file: Arc<netcdf::File>,
    shape: Vec<usize>,
    dimensions: Vec<String>,
}

impl<T: ArrowTypeConversion + NcTypeDescriptor + 'static> VariableBackend<T> {
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
impl<T: ArrowTypeConversion + NcTypeDescriptor + 'static> ArrayBackend<T> for VariableBackend<T> {
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
            let start = subset.start[axis];
            let len = subset.shape[axis];
            extents.push(netcdf::Extent::from(start..start + len));
        }

        self.decoder.read(&var, netcdf::Extents::Extent(extents))
    }
}

#[derive(Debug)]
pub struct AttributeBackend<T: ArrowTypeConversion> {
    _field: FieldRef,
    value: T,
}

impl<T: ArrowTypeConversion> AttributeBackend<T> {
    pub fn new(field: FieldRef, value: T) -> Self {
        Self {
            _field: field,
            value,
        }
    }
}

#[async_trait::async_trait]
impl<T: ArrowTypeConversion + Clone> ArrayBackend<T> for AttributeBackend<T> {
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
