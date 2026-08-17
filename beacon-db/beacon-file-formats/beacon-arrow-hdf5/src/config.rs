//! [`Hdf5Config`]: the runtime settings of the HDF5 format.

use beacon_arrow_netcdf::datafusion::ReaderBackend;

/// Runtime configuration for the HDF5 format.
///
/// Plain data with sensible defaults; the caller populates it (there is no
/// environment parsing here, so the crate stays reusable and the host decides
/// where the values come from). These are the *defaults* for a runtime — each
/// can be overridden per table via `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct Hdf5Config {
    /// Which reader opens a file.
    ///
    /// [`ReaderBackend::Oxcdf`], the pure-Rust reader, by default. It reads
    /// parallel, reads an object store, records per-file statistics, and covers
    /// the two layouts netcdf-c cannot report: a nested group and a compound
    /// dataset. See [`crate::reader`].
    ///
    /// [`ReaderBackend::NetcdfC`] is the fallback. A NetCDF-4 file *is* an HDF5
    /// file, and netcdf-c's HDF5 dispatch opens a plain one too, so it reads
    /// every file this format serves. It is the reader this crate used before
    /// the Rust one existed. Writes always use netcdf-c.
    ///
    /// This is separate from the netCDF backend, so a runtime moves one format
    /// at a time.
    pub backend: ReaderBackend,
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
            backend: ReaderBackend::Oxcdf,
            enable_statistics: true,
        }
    }
}
