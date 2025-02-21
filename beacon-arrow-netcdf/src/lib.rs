use netcdf::{types::NcVariableType, NcTypeDescriptor};

pub mod encoders;
pub mod reader;
pub mod writer;

#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct NcChar(u8);
unsafe impl NcTypeDescriptor for NcChar {
    fn type_descriptor() -> NcVariableType {
        NcVariableType::Char
    }
}
