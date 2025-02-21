use std::{cell::RefCell, rc::Rc};

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use netcdf::FileMut;

pub mod default;

pub trait Encoder {
    fn create(nc_file: Rc<RefCell<FileMut>>, schema: SchemaRef) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn write_column(&mut self, name: &str, array: ArrayRef) -> anyhow::Result<()>;
    fn write_record_batch(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let array = batch.column(i);
            self.write_column(field.name(), array.clone())?;
        }
        Ok(())
    }
}
