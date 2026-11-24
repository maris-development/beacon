//! Zarr v3 stores on the Beacon ND framework (`beacon-nd-dataset`).

use zarrs_storage::AsyncReadableListableStorageTraits;

// A later change reads Zarr variables through these functions.
#[allow(dead_code)]
mod attributes;
// A later change decodes Zarr variables through this module.
#[allow(dead_code)]
mod decode;
#[cfg(test)]
mod testing;
// A later change opens Zarr groups through this module.
#[allow(dead_code)]
mod variable;

/// A Zarr array over an async store that can read and list.
pub type ZarrArray = zarrs::array::Array<dyn AsyncReadableListableStorageTraits>;
