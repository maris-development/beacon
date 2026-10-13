//! Encoder abstraction for writing Arrow arrays and batches to NetCDF.

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use std::collections::{HashMap, HashSet};

use netcdf::FileMut;

use crate::{datafusion::options::Compression, time_units::TimeUnits};

/// Default encoder implementation.
pub mod default;

/// Settings that an encoder applies when it defines the variables.
#[derive(Debug, Clone, Default)]
pub struct EncoderOptions {
    /// Deflate compression of each variable. `None` writes the data without compression.
    pub compression: Option<Compression>,
    /// Rows in one chunk of the record dimension. `None` keeps the netCDF default chunks.
    pub chunk_rows: Option<usize>,
    /// Source `_FillValue` of each variable. A variable not in the map uses the default fill.
    pub fill_values: HashMap<String, netcdf::AttributeValue>,
    /// CF units of each timestamp variable. `None` keeps the Arrow unit since 1970.
    pub time_units: Option<TimeUnits>,
    /// Timestamp variables with a value that is not a whole number in `time_units`.
    pub float_time_columns: HashSet<String>,
    /// Length of the record dimension. `None` makes it unlimited.
    pub record_rows: Option<usize>,
}

/// Trait implemented by concrete NetCDF encoders.
pub trait Encoder {
    /// Create an encoder instance for an open mutable NetCDF file and schema.
    fn create(
        nc_file: FileMut,
        schema: SchemaRef,
        options: &EncoderOptions,
    ) -> Result<Self, EncoderError>
    where
        Self: Sized;
    /// Write one named Arrow column.
    fn write_column(&mut self, name: &str, array: ArrayRef) -> Result<(), EncoderError>;
    /// Write all columns from a record batch.
    fn write_record_batch(&mut self, batch: RecordBatch) -> Result<(), EncoderError> {
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let array = batch.column(i);
            self.write_column(field.name(), array.clone())?;
        }
        Ok(())
    }
    /// Write one attribute on `variable`, or a global attribute when `variable` is `None`.
    fn put_attribute(
        &mut self,
        _variable: Option<&str>,
        _name: &str,
        _value: netcdf::AttributeValue,
    ) -> Result<(), EncoderError> {
        Err(format!("{} does not support attributes", Self::encoder_name()).into())
    }
    /// Return a static encoder identifier.
    fn encoder_name() -> &'static str;
}

/// Unified error type returned by encoders.
pub type EncoderError = Box<dyn std::error::Error + Send + Sync>;
