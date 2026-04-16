//! Decoder abstractions for translating NetCDF variables into ND arrays.
//!
//! A decoder receives a NetCDF variable plus read extents and returns an
//! `ndarray::ArrayD<T>` in the target logical type.

use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{make_array, ArrayRef, Datum, Scalar},
    buffer::NullBuffer,
};
use beacon_nd_arrow::datatypes::{NdArrayDataType, NdArrayType};
use netcdf::{
    types::{FloatType, IntType, NcVariableType},
    Extents, NcTypeDescriptor,
};

/// Decoders for CF-style time units.
pub mod cf_time;
/// Decoders for NetCDF string and fixed-size char-string representations.
pub mod strings;

/// Generic NetCDF variable decoder.
///
/// Implementations define how to read and convert variable values into `T`.
pub trait VariableDecoder<T>: Debug + Send + Sync
where
    T: NdArrayType + NcTypeDescriptor,
{
    /// Read values from `variable` for the requested NetCDF `extents`.
    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: Extents,
    ) -> anyhow::Result<ndarray::ArrayD<T>>;
    /// Optional fill value used for broadcasting/shape operations.
    fn fill_value(&self) -> Option<T> {
        None
    }
    /// Name of the source variable.
    fn variable_name(&self) -> &str;
    /// Nd Array Data Type
    fn data_type(&self) -> NdArrayDataType {
        T::data_type()
    }
}

/// Default decoder that reads a variable directly as `T` without transforms.
#[derive(Debug)]
pub struct DefaultVariableDecoder<T>
where
    T: NdArrayType + NcTypeDescriptor,
{
    pub variable_name: String,
    pub fill_value: Option<T>,
    marker: std::marker::PhantomData<T>,
}

impl<T> DefaultVariableDecoder<T>
where
    T: NdArrayType + NcTypeDescriptor,
{
    /// Construct a default decoder with an Arrow field and optional fill value.
    pub fn new(variable_name: impl Into<String>, fill_value: Option<T>) -> Self {
        Self {
            variable_name: variable_name.into(),
            fill_value,
            marker: std::marker::PhantomData,
        }
    }
}

impl<T: NdArrayType + NcTypeDescriptor + Copy> VariableDecoder<T> for DefaultVariableDecoder<T> {
    fn variable_name(&self) -> &str {
        &self.variable_name
    }

    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: Extents,
    ) -> anyhow::Result<ndarray::ArrayD<T>> {
        let array = variable.get::<T, _>(extents)?;
        Ok(array)
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value
    }
}
