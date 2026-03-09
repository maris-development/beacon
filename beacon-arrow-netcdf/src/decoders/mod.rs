use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{make_array, ArrayRef, Datum, Scalar},
    buffer::NullBuffer,
};
use beacon_nd_arrow::array::compat_typings::ArrowTypeConversion;
use netcdf::{
    types::{FloatType, IntType, NcVariableType},
    Extents, NcTypeDescriptor,
};

pub mod cf_time;
pub mod strings;

pub trait VariableDecoder<T>: Debug + Send + Sync
where
    T: ArrowTypeConversion + NcTypeDescriptor,
{
    fn arrow_field(&self) -> &arrow::datatypes::Field;
    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: Extents,
    ) -> anyhow::Result<ndarray::ArrayD<T>>;
    fn fill_value(&self) -> Option<T> {
        None
    }
    fn variable_name(&self) -> &str;
}

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
}
