//! [`Hdf5Config`]: the runtime settings of the HDF5 format.

/// Runtime configuration for the HDF5 format.
///
/// Plain data with sensible defaults; the caller populates it (there is no
/// environment parsing here, so the crate stays reusable and the host decides
/// where the values come from). These are the *defaults* for a runtime — each
/// can be overridden per table via `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct Hdf5Config {
    /// Whether reads go through the pure-Rust reader instead of netcdf-c.
    ///
    /// Off by default, so a server that leaves it off behaves exactly as it did
    /// before this reader existed: an HDF5 file is opened by netcdf-c, through
    /// [`beacon_arrow_netcdf`]. Turn it on for parallel reads, native object
    /// store access, per-file statistics, nested groups and compound datasets;
    /// see [`crate::reader`]. Writes always use netcdf-c.
    pub use_rust_reader: bool,
    /// Whether reads consult the shared reader cache by default.
    ///
    /// Only the Rust reader has a cache of its own. Under netcdf-c the netCDF
    /// format's cache applies instead.
    pub use_reader_cache: bool,
    /// Capacity (number of opened datasets) of the shared reader cache.
    pub reader_cache_size: usize,
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
            use_rust_reader: false,
            use_reader_cache: true,
            reader_cache_size: 128,
            enable_statistics: true,
        }
    }
}
