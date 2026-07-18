//! Helpers for opening atlas stores from beacon-managed object paths.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use atlas::Atlas;
use object_store::path::Path as OsPath;

/// Resolve the parent directory of an `atlas.json` object path to a
/// local filesystem path suitable for [`Atlas::open_path`].
///
/// Atlas opens files natively rather than through `object_store`, so the object
/// path is joined under the datasets store's local root. Reads therefore require
/// a local-filesystem datasets store.
fn store_dir_for_object(
    datasets_root: &Path,
    object_path: &OsPath,
) -> datafusion::error::Result<PathBuf> {
    let local_marker = beacon_object_storage::local_object_path(datasets_root, object_path)
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to resolve atlas object path {object_path} to local path: {e}"
            ))
        })?;

    let marker = PathBuf::from(local_marker);
    let dir = marker.parent().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "Atlas marker path {marker:?} has no parent directory"
        ))
    })?;
    Ok(dir.to_path_buf())
}

/// Open the atlas store whose `atlas.json` lives at `object_path`.
pub async fn open_atlas_store(
    datasets_root: PathBuf,
    object_path: &OsPath,
) -> datafusion::error::Result<Arc<Atlas>> {
    let dir = store_dir_for_object(&datasets_root, object_path)?;
    let atlas = Atlas::open_path(&dir).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open atlas store at {dir:?}: {e}"
        ))
    })?;
    Ok(Arc::new(atlas))
}
