//! Helpers for opening atlas stores from beacon-managed object paths.

use std::sync::Arc;

use atlas::Atlas;
use beacon_object_storage::DatasetsStore;
use object_store::path::Path as OsPath;

/// Resolve the parent directory of an `atlas.json` object path to a
/// local filesystem path suitable for [`Atlas::open_path`].
///
/// Atlas requires an `Arc<dyn ObjectStore>` from object_store **0.13**,
/// while beacon's `DatasetsStore` is wired against object_store **0.12**.
/// Until the two version pins converge we restrict reads to local-FS
/// datasets and rely on `DatasetsStore::translate_netcdf_url_path` to
/// produce the plain filesystem path.
fn store_dir_for_object(
    datasets_object_store: &Arc<DatasetsStore>,
    object_path: &OsPath,
) -> datafusion::error::Result<std::path::PathBuf> {
    let local_marker = datasets_object_store
        .translate_netcdf_url_path(object_path)
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to translate atlas object path {object_path} to local path: {e}"
            ))
        })?;

    if local_marker.starts_with("http://") || local_marker.starts_with("https://") {
        return Err(datafusion::error::DataFusionError::NotImplemented(
            "Atlas datasets on remote object stores are not yet supported — only local filesystem stores are readable".to_string(),
        ));
    }

    let marker = std::path::PathBuf::from(local_marker);
    let dir = marker.parent().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "Atlas marker path {marker:?} has no parent directory"
        ))
    })?;
    Ok(dir.to_path_buf())
}

/// Open the atlas store whose `atlas.json` lives at `object_path`.
pub async fn open_atlas_store(
    datasets_object_store: Arc<DatasetsStore>,
    object_path: &OsPath,
) -> datafusion::error::Result<Arc<Atlas>> {
    let dir = store_dir_for_object(&datasets_object_store, object_path)?;
    let atlas = Atlas::open_path(&dir).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open atlas store at {dir:?}: {e}"
        ))
    })?;
    Ok(Arc::new(atlas))
}
