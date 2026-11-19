//! Encoder abstraction for writing Arrow arrays and batches to NetCDF.

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use oxcdf::{AttributeValue, FileMut};

use crate::datafusion::options::Compression;

/// Default encoder implementation.
pub mod default;

/// Settings that an encoder applies when it defines the variables.
#[derive(Debug, Clone, Default)]
pub struct EncoderOptions {
    /// Deflate compression of each variable. `None` writes the data without compression.
    pub compression: Option<Compression>,
    /// Rows in one chunk of the record dimension. `None` keeps the netCDF default chunks.
    pub chunk_rows: Option<usize>,
}

/// Trait implemented by concrete NetCDF encoders.
pub trait Encoder {
    /// Create an encoder instance for an open mutable NetCDF file and schema.
    ///
    /// After an error, an implementor must leave no file: it discards `nc_file` with `into_bytes`.
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
        _value: AttributeValue,
    ) -> Result<(), EncoderError> {
        Err(format!("{} does not support attributes", Self::encoder_name()).into())
    }
    /// Give back the NetCDF file, to close or discard it.
    fn into_file(self) -> FileMut
    where
        Self: Sized;
    /// Return a static encoder identifier.
    fn encoder_name() -> &'static str;
}

/// Remove a NetCDF file from `oxcdf::create` before its close.
///
/// A drop of a `FileMut` moves the partial file to its path, so an error path calls this.
pub(crate) fn discard(file: FileMut) {
    // `into_bytes` on a file on disk removes the temporary file and returns an error.
    let _ = file.into_bytes();
}

/// Unified error type returned by encoders.
pub type EncoderError = Box<dyn std::error::Error + Send + Sync>;
