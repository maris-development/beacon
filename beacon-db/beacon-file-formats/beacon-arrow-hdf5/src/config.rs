//! [`Hdf5Config`]: the runtime settings of the HDF5 format.

/// A layout convention beacon reads on top of the HDF5 container.
///
/// A plain HDF5 file follows no standard, so nothing about it is safe to assume.
/// One vendor's layout does describe itself, and a convention here says which
/// one to expect. [`Hdf5Convention::None`] is the default: no file is inspected
/// for a convention it may not follow, and no read pays for one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Hdf5Convention {
    /// Read the container and nothing else. The default.
    #[default]
    None,
    /// An ASN OptoDAS acquisition file. See [`crate::conventions::optodas`].
    OptoDas,
}

impl Hdf5Convention {
    /// The convention one option value names.
    ///
    /// # Errors
    ///
    /// Returns the offending value when it names no convention.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" | "" => Ok(Self::None),
            "optodas" | "opto-das" | "opto_das" => Ok(Self::OptoDas),
            other => Err(other.to_string()),
        }
    }
}

/// What one read of an HDF5 file needs beyond the object itself.
///
/// The settings of a table, resolved against [`Hdf5Config`]. They travel
/// together because every read takes all of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOptions {
    /// Whether every dimension netCDF invents is renamed by its length.
    pub unify_phony_dimensions: bool,
    /// The layout convention to read on top of the container.
    pub convention: Hdf5Convention,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            unify_phony_dimensions: true,
            convention: Hdf5Convention::None,
        }
    }
}

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
    /// Whether every dimension netCDF invents for a file that names none is
    /// renamed by its length, over every group of the file.
    ///
    /// On by default, and the reason a plain HDF5 file reads as one table: the
    /// payload of the root group and the description of each column in another
    /// group then share an axis and broadcast. See
    /// [`beacon_arrow_netcdf::dimensions`].
    ///
    /// Turn it off to keep the names the reader gave, one per length per group.
    /// Two groups then never share an axis, which is right for a file whose
    /// groups hold unrelated axes of one length, and wrong for most others.
    pub unify_phony_dimensions: bool,
    /// The layout convention every table of this runtime reads by default.
    ///
    /// [`Hdf5Convention::None`] unless a server says otherwise, so a file is
    /// read as the container describes it and nothing more. A table names its
    /// own convention with the `convention` option.
    pub convention: Hdf5Convention,
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
            unify_phony_dimensions: true,
            convention: Hdf5Convention::None,
            enable_statistics: true,
        }
    }
}
