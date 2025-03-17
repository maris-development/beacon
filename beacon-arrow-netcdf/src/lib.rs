use std::ffi::CString;

use netcdf::{types::NcVariableType, NcTypeDescriptor};

pub mod cf_time;
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

#[repr(transparent)]
pub struct NcString(*mut i8);
unsafe impl NcTypeDescriptor for NcString {
    fn type_descriptor() -> NcVariableType {
        NcVariableType::String
    }
}

impl NcString {
    pub fn new(s: &str) -> Self {
        let c_str = CString::new(s).unwrap();
        let ptr = c_str.into_raw();
        Self(ptr.cast())
    }
}

impl Drop for NcString {
    fn drop(&mut self) {
        unsafe {
            drop(CString::from_raw(self.0.cast()));
        }
    }
}
