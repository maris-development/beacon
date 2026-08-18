//! [`Hdf5Config`]: the runtime settings of the HDF5 format.

/// Runtime configuration for the HDF5 format.
#[derive(Debug, Clone)]
pub struct Hdf5Config {
    /// Whether reads go through the pure-Rust reader instead of netcdf-c.
    ///
    /// On by default. The Rust reader reads in parallel, reads an object store,
    /// records per-file statistics, and covers the two layouts netcdf-c cannot
    /// report: a nested group and a compound dataset. See [`crate::reader`].
    ///
    /// Turn it off for netcdf-c, the fallback. A NetCDF-4 file *is* an HDF5
    /// file, and netcdf-c's HDF5 dispatch opens a plain one too, so it reads
    /// every file this format serves. It is the reader this crate used before
    /// the Rust one existed. Writes always use netcdf-c.
    pub use_rust_reader: bool,
    /// Whether to generate per-file statistics during planning.
    ///
    /// Statistics need the Rust reader. Under netcdf-c the format reports
    /// unknown statistics whatever this says, because generating them would
    /// park a tokio worker behind the process-global netcdf-c lock.
    pub enable_statistics: bool,
}

impl Default for Hdf5Config {
    fn default() -> Self {
        Self {
            use_rust_reader: true,
            enable_statistics: true,
        }
    }
}
