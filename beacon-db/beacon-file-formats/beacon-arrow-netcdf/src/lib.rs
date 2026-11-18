//! `beacon-arrow-netcdf` bridges NetCDF files and Beacon ND Arrow arrays.
//!
//! The crate provides:
//! - Reading NetCDF variables and attributes into lazy ND Arrow arrays.
//! - Writing Arrow record batches into NetCDF files.
//! - Decoder/encoder building blocks for extending conversion behaviour.
//!
//! # Reading and writing
//!
//! [`oxcdf_reader`] reads through [`oxcdf`], a pure-Rust reader. It holds no
//! global lock and reads through `object_store`, so scans run in parallel and
//! S3, GCS and Azure need no local copy. [`writer`] writes through netcdf-c.
//!
//! # Key modules
//! - [`oxcdf_reader`]: high-level NetCDF -> Arrow reader API.
//! - [`writer`]: high-level Arrow -> NetCDF writer API.
//! - [`encoders`]: pluggable conversion components for the writer.
//! - [`cf_time`]: CF time conversion.
//! - [`dimensions`]: unifies the dimensions netCDF invents for a plain HDF5
//!   file, so two groups of one file broadcast against each other.

use std::ffi::CString;

use netcdf::{types::NcVariableType, NcTypeDescriptor};

/// Encoder implementations for writing Arrow values to NetCDF variables.
pub mod encoders;
/// High-level NetCDF writer.
pub mod writer;
/// Folds `variable.attribute` columns into NetCDF attributes on write.
pub mod attributes;
/// Re-export of the `netcdf` crate used by this crate.
pub use netcdf;
/// Re-export of low-level `netcdf-sys` bindings.
pub use netcdf_sys;
/// Re-export of the pure-Rust `oxcdf` reader used by [`oxcdf_reader`].
pub use oxcdf;
/// CF time conversion.
pub mod cf_time;
#[cfg(test)]
mod cf_time_fill_tests;
/// DataFusion integration components.
pub mod datafusion;
/// Unifies the dimensions netCDF invents for a file that names none.
pub mod dimensions;
/// Pure-Rust NetCDF reader over `object_store`.
pub mod oxcdf_reader;

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
    ///
    /// A C string cannot contain an interior NUL byte. If `s` has one (which a C
    /// consumer would treat as the string terminator anyway), the value is
    /// truncated at the first NUL and a warning is logged, rather than panicking.
    pub fn new(s: &str) -> Self {
        let c_str = CString::new(s).unwrap_or_else(|e| {
            let nul = e.nul_position();
            tracing::warn!(
                nul_position = nul,
                "NetCDF string contains an interior NUL byte; truncating at the first NUL"
            );
            // The prefix before the first NUL is by definition NUL-free.
            CString::new(&s.as_bytes()[..nul]).expect("prefix before first NUL has no NUL byte")
        });
        let ptr = c_str.into_raw();
        Self(ptr.cast())
    }
}
