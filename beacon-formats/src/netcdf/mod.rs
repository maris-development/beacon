//! Compatibility re-exports for NetCDF DataFusion integration.
//!
//! The NetCDF DataFusion implementation now lives in beacon-arrow-netcdf.
//! This module remains as a thin forwarding layer for downstream crates that
//! still import symbols from beacon-formats::netcdf.

pub use beacon_arrow_netcdf::datafusion::{NetCDFFormatFactory, NetcdfFormat, NetcdfOptions};
