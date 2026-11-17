//! The plugin FFI of Beacon. This crate re-exports each part; the code is in the
//! `beacon-ffi-*` crates.
//!
//! | Module | Crate | Content |
//! | --- | --- | --- |
//! | [`abi`] | `beacon-ffi-abi` | the ABI version and the panic helpers |
//! | [`object_store`] | `beacon-ffi-object-store` | one object store across the boundary |
//! | [`scalar_functions`] | `beacon-ffi-scalar-functions` | scalar function plugins |
//! | [`file_format`] | `beacon-ffi-file-format` | file format plugins |
//! | [`host`] | `beacon-ffi-host` | the Beacon server for table code |
//! | [`table_type`] | `beacon-ffi-table-type` | table type plugins |
//!
//! A plugin author uses `beacon-plugin-sdk`, which adds the `export_plugin!` macro.

pub use beacon_ffi_abi as abi;
pub use beacon_ffi_file_format as file_format;
pub use beacon_ffi_host as host;
pub use beacon_ffi_object_store as object_store;
pub use beacon_ffi_scalar_functions as scalar_functions;
pub use beacon_ffi_table_type as table_type;

pub use beacon_ffi_abi::{
    ABI_SYMBOL, AbiFn, BEACON_PLUGIN_SDK_ABI, BeaconPluginAbi, sdk_abi_version,
};
