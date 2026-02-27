use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Scalar, UInt32Array},
    compute::TakeOptions,
};
use arrow_schema::FieldRef;
use beacon_nd_arrow::array::backend::ArrayBackend;

use crate::decoders::VariableDecoder;

#[derive(Debug)]
pub struct VariableBackend {
    decoder: Arc<dyn VariableDecoder>,
    nc_file: Arc<netcdf::File>,
    shape: Vec<usize>,
    dimensions: Vec<String>,
}

#[async_trait::async_trait]
impl ArrayBackend for VariableBackend {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }
    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }
    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef> {
        let variable = self
            .nc_file
            .variable(self.decoder.variable_name())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Variable {} not found in NetCDF file",
                    self.decoder.variable_name()
                )
            })?;

        // Read using extents all
        let array = self.decoder.read(&variable, netcdf::Extents::All)?;

        // Slice the array according to the requested start and length
        if start + length > self.len() {
            return Err(anyhow::anyhow!(
                "Slice out of bounds: start {} + length {} > total length {}",
                start,
                length,
                self.len()
            ));
        }

        let copy_slice_enabled = std::env::var("BEACON_ARROW_NETCDF_COPY_SLICE").is_ok();
        let sliced = if copy_slice_enabled {
            // If copying is enabled, we create a new array with the sliced data
            let sliced = array.slice(start, length);
            let sliced_take_indices = UInt32Array::from_iter_values(0..length as u32);
            // Use the take kernel to create a new array with the sliced data
            let take_opts = TakeOptions {
                check_bounds: false, // We have already checked bounds above
            };
            arrow::compute::take(&sliced, &sliced_take_indices, Some(take_opts))?
        } else {
            array.slice(start, length)
        };

        Ok(sliced)
    }
}

#[derive(Debug)]
pub struct AttributeBackend {
    _field: FieldRef,
    value: Scalar<ArrayRef>,
}

#[async_trait::async_trait]
impl ArrayBackend for AttributeBackend {
    fn len(&self) -> usize {
        1
    }

    fn shape(&self) -> Vec<usize> {
        vec![]
    }
    fn dimensions(&self) -> Vec<String> {
        vec![]
    }
    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef> {
        if start != 0 || length != 1 {
            return Err(anyhow::anyhow!(
                "Attribute array can only be sliced with start=0 and length=1"
            ));
        }
        Ok(self.value.clone().into_inner())
    }
}
