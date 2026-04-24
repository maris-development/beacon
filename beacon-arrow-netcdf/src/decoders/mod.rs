//! Decoder abstractions for translating NetCDF variables into ND arrays.
//!
//! A decoder receives a NetCDF variable plus read extents and returns an
//! `ndarray::ArrayD<T>` in the target logical type.

use beacon_nd_arrow::array::compat_typings::ArrowTypeConversion;
use netcdf::{Extents, NcTypeDescriptor};

/// Decoders for CF-style time units.
pub mod cf_time;
/// Decoders for NetCDF string and fixed-size char-string representations.
pub mod strings;

/// Generic NetCDF variable decoder.
///
/// Implementations define how to read and convert variable values into `T`.
pub trait VariableDecoder<T>: std::fmt::Debug + Send + Sync
where
    T: ArrowTypeConversion + NcTypeDescriptor,
{
    /// Arrow field metadata for the decoded values.
    fn arrow_field(&self) -> &arrow::datatypes::Field;
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
}

/// Default decoder that reads a variable directly as `T` without transforms.
#[derive(Debug)]
pub struct DefaultVariableDecoder<T>
where
    T: ArrowTypeConversion + NcTypeDescriptor,
{
    pub arrow_field: arrow::datatypes::FieldRef,
    pub fill_value: Option<T>,
    marker: std::marker::PhantomData<T>,
}

impl<T> DefaultVariableDecoder<T>
where
    T: ArrowTypeConversion + NcTypeDescriptor,
{
    /// Construct a default decoder with an Arrow field and optional fill value.
    pub fn new(arrow_field: arrow::datatypes::FieldRef, fill_value: Option<T>) -> Self {
        Self {
            arrow_field,
            fill_value,
            marker: std::marker::PhantomData,
        }
    }
}

impl<T: ArrowTypeConversion + NcTypeDescriptor + Copy> VariableDecoder<T>
    for DefaultVariableDecoder<T>
{
    fn variable_name(&self) -> &str {
        self.arrow_field.name()
    }

    fn arrow_field(&self) -> &arrow::datatypes::Field {
        &self.arrow_field
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
