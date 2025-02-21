use netcdf::{types::NcVariableType, NcTypeDescriptor};

pub mod encoders;
pub mod reader;
pub mod writer;

#[repr(transparent)]
#[derive(Copy, Clone)]
struct FixedSizeString(u8);
unsafe impl NcTypeDescriptor for FixedSizeString {
    fn type_descriptor() -> NcVariableType {
        NcVariableType::Char
    }
}
