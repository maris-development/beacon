use netcdf::{types::NcVariableType, NcTypeDescriptor};

pub mod encoders;
pub mod nc_array;
pub mod reader;
pub mod writer;
#[repr(transparent)]
#[derive(Clone)]
pub struct NcFixedSizedString(Vec<u8>);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct NcChar(u8);
unsafe impl NcTypeDescriptor for NcChar {
    fn type_descriptor() -> NcVariableType {
        NcVariableType::Char
    }
}
