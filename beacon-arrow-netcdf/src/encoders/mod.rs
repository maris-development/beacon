use std::{cell::RefCell, rc::Rc};

use arrow::{array::ArrayRef, datatypes::SchemaRef};
use netcdf::FileMut;

pub mod default;

pub trait Encoder {
    fn create(nc_file: Rc<RefCell<FileMut>>, schema: SchemaRef) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn write_column(&mut self, name: &str, array: ArrayRef) -> anyhow::Result<()>;
}
