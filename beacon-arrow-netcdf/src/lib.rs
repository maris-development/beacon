//! `beacon-arrow-netcdf` bridges NetCDF files and Beacon ND Arrow arrays.
//!
//! The crate provides:
//! - Reading NetCDF variables and attributes into lazy ND Arrow arrays.
//! - Writing Arrow record batches into NetCDF files.
//! - Decoder/encoder building blocks for extending conversion behaviour.
//!
//! # Key modules
//! - [`reader`]: high-level NetCDF -> Arrow reader API.
//! - [`writer`]: high-level Arrow -> NetCDF writer API.
//! - [`compat`]: variable/attribute conversion helpers.
//! - [`decoders`] and [`encoders`]: pluggable conversion components.

use std::ffi::{CStr, CString};

use netcdf::{types::NcVariableType, NcTypeDescriptor};

/// Encoder implementations for writing Arrow values to NetCDF variables.
pub mod encoders;
/// High-level NetCDF reader.
pub mod reader;
/// High-level NetCDF writer.
pub mod writer;
/// Re-export of the `netcdf` crate used by this crate.
pub use netcdf;
/// Re-export of low-level `netcdf-sys` bindings.
pub use netcdf_sys;
/// Array backend implementations used by decoders.
pub mod backend;
/// Conversion helpers from NetCDF values to ND Arrow arrays.
pub mod compat;
/// DataFusion integration components.
pub mod datafusion;
/// Decoder implementations and decoder traits.
pub mod decoders;

/// Placeholder wrapper for fixed-size string payload bytes.
///
/// The type is currently used as an internal marker in conversion paths.
#[repr(transparent)]
#[derive(Clone)]
pub struct NcFixedSizedString(Vec<u8>);

/// Wrapper for a single NetCDF `char` value.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct NcChar(u8);
unsafe impl NcTypeDescriptor for NcChar {
    fn type_descriptor() -> NcVariableType {
        NcVariableType::Char
    }
}

/// Wrapper around a raw C string pointer used by NetCDF string variables.
///
/// `NcString` mirrors the C-level representation expected by the NetCDF API.
/// Prefer [`OwnedNcString`] for owned Rust values.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct NcString(*mut std::ffi::c_char);
unsafe impl NcTypeDescriptor for NcString {
    fn type_descriptor() -> NcVariableType {
        NcVariableType::String
    }
}

impl NcString {
    /// Create a NetCDF string from a Rust `&str`.
    pub fn new(s: &str) -> Self {
        let c_str = CString::new(s).unwrap();
        let ptr = c_str.into_raw();
        Self(ptr.cast())
    }

    /// Copy the pointed C string into an owned Rust [`String`].
    pub fn copy_to_string(&self) -> String {
        unsafe {
            let c_str = CStr::from_ptr(self.0.cast());
            let string = c_str.to_string_lossy().into_owned();
            string
        }
    }
}

/// Owned string representation used by decoders and Arrow conversion.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnedNcString(String);

unsafe impl NcTypeDescriptor for OwnedNcString {
    fn type_descriptor() -> NcVariableType {
        panic!("Logical type only - not directly readable/writable as a NetCDF variable. Use decoders/encoders to convert to/from NcString or fixed-size string representations.")
    }
}
