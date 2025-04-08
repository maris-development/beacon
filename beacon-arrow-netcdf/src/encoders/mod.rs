use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use netcdf::FileMut;

pub mod default;

pub trait Encoder {
    fn create(nc_file: FileMut, schema: SchemaRef) -> Result<Self, EncoderError>
    where
        Self: Sized;
    fn write_column(&mut self, name: &str, array: ArrayRef) -> Result<(), EncoderError>;
    fn write_record_batch(&mut self, batch: RecordBatch) -> Result<(), EncoderError> {
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let array = batch.column(i);
            self.write_column(field.name(), array.clone())?;
        }
        Ok(())
    }
    fn encoder_name() -> &'static str;
}

pub type EncoderError = Box<dyn std::error::Error + Send + Sync>;
