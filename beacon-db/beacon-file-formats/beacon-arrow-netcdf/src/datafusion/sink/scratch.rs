//! The local scratch file of a sink, and its upload to the object store.

use std::path::PathBuf;

use datafusion::error::DataFusionError;
use datafusion::execution::{disk_manager::RefCountedTempFile, TaskContext};
use object_store::{path::Path as ObjectPath, ObjectStore};
use oxcdf::FileMut;

/// The local path of the NetCDF file before its upload.
///
/// The path is in the temp folder of the DataFusion disk manager. When the disk manager
/// cannot give a file, the path is in a new folder in the OS temp folder.
///
/// oxcdf writes `<path>.tmp` and removes it at the close or the discard. A drop of the
/// scratch file removes `<path>.tmp` if a stopped upload leaves it.
pub(super) struct ScratchFile {
    path: PathBuf,
    /// Keeps the managed file and its folder until the drop.
    _managed: Option<RefCountedTempFile>,
    /// The fallback folder in the OS temp folder. A drop removes it with its files.
    _folder: Option<tempfile::TempDir>,
}

impl ScratchFile {
    /// Reserve a scratch path in the temp folder of `context`.
    pub(super) fn new(context: &TaskContext) -> datafusion::error::Result<Self> {
        match context
            .runtime_env()
            .disk_manager
            .create_tmp_file("NetCDF output")
        {
            Ok(managed) => Ok(Self {
                path: with_suffix(managed.path(), ".nc"),
                _managed: Some(managed),
                _folder: None,
            }),
            Err(e) => {
                tracing::debug!("NetCDF scratch file goes to the OS temp folder: {}", e);
                let folder = tempfile::Builder::new()
                    .prefix("beacon-netcdf-")
                    .tempdir()
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to create a NetCDF scratch folder: {}",
                            e
                        ))
                    })?;
                Ok(Self {
                    path: folder.path().join("output.nc"),
                    _managed: None,
                    _folder: Some(folder),
                })
            }
        }
    }

    pub(super) fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for ScratchFile {
    fn drop(&mut self) {
        // Only an upload that stops at an await leaves this file.
        let _ = std::fs::remove_file(with_suffix(&self.path, ".tmp"));
    }
}

/// `path` with `suffix` added to its file name.
fn with_suffix(path: &std::path::Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Write `nc_file` and upload it to `location` in `store`.
///
/// An error leaves the earlier object at `location`, or no object.
pub(super) async fn upload(
    nc_file: FileMut,
    store: &dyn ObjectStore,
    location: &ObjectPath,
) -> datafusion::error::Result<()> {
    nc_file.close_to_store(store, location).await.map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to finish writing NetCDF to '{}': {}",
            location, e
        ))
    })
}
