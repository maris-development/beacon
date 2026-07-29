//! Encoder abstraction for writing Arrow arrays and batches to NetCDF.

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use netcdf::FileMut;

/// Default encoder implementation.
pub mod default;

/// Trait implemented by concrete NetCDF encoders.
pub trait Encoder {
    /// Create an encoder instance for an open mutable NetCDF file and schema.
    fn create(nc_file: FileMut, schema: SchemaRef) -> Result<Self, EncoderError>
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
    /// Return a static encoder identifier.
    fn encoder_name() -> &'static str;
}

/// Unified error type returned by encoders.
pub type EncoderError = Box<dyn std::error::Error + Send + Sync>;
