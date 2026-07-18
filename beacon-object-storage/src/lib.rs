//! Beacon object storage abstraction.
//!
//! This crate wraps implementations from [`object_store`] to provide a consistent
//! storage layer for Beacon.
//!
//! **Stores**
//! - **Datasets**: either local filesystem or S3-compatible storage (depending on
//!   config). A plain [`ObjectStore`] — queries read through it via the DataFusion
//!   object-store registry. Formats that open files natively rather than through
//!   `object_store` (netCDF/Atlas/TIFF/GDAL) are given the datasets **local root**
//!   ([`StorageConfig::datasets_dir`]) and join object paths under it with
//!   [`local_object_path`]; native reads therefore work only when the datasets
//!   store is a local filesystem.
//! - **Tables**: a single-file [`beacon_redb_store::RedbStore`] (`beacon.db`)
//!   holding the catalog and managed Lance data.
//! - **Tmp**: local filesystem.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use beacon_redb_store::RedbStore;
use object_store::{ObjectStore, local::LocalFileSystem};

pub mod config;
pub mod error;

pub use config::{S3Config, StorageConfig};

use crate::error::StorageResult;

/// The set of object stores owned by a single Beacon runtime.
///
/// Built once from a [`StorageConfig`] and the resolved data directories, then
/// owned by the runtime and handed to the components that need it. There is no
/// process-global store, so independent runtimes can use different backends.
#[derive(Clone)]
pub struct ObjectStores {
    /// Datasets store (local filesystem or S3, per `storage`).
    pub datasets: Arc<dyn ObjectStore>,
    /// Local filesystem store backing Beacon's tables.
    pub tables: Arc<dyn ObjectStore>,
    /// Local filesystem store backing Beacon's temporary files.
    pub tmp: Arc<dyn ObjectStore>,
}

impl ObjectStores {
    /// Build all three object stores, returning a structured error if any fails.
    ///
    /// The local roots and S3 backend selection all come from `storage`.
    pub async fn new(storage: &StorageConfig) -> StorageResult<Self> {
        let datasets = build_datasets_backend(storage)?;
        Self::assemble(storage, datasets)
    }

    /// Build the stores over an embedder-supplied datasets backend, bypassing
    /// `storage`'s own backend *construction* (`datasets_dir` / `s3`). The tables and
    /// tmp stores still come from `storage`.
    ///
    /// The injected `backend` is registered as the datasets store as-is. Two
    /// consequences follow from a raw [`ObjectStore`] carrying no description of
    /// itself:
    ///
    /// - It has no change notifications, so external tables over it become current
    ///   on an explicit `REFRESH` only.
    /// - The netCDF/Atlas/TIFF readers translate paths under
    ///   [`StorageConfig::datasets_dir`] (a local root), since a backend cannot
    ///   report its own root. An embedder that injects a *local* store and reads
    ///   netCDF/Atlas must therefore point `storage.datasets_dir` at the same
    ///   directory the backend is rooted at; an S3-backed store cannot be read
    ///   natively at all.
    pub async fn new_with_datasets_backend(
        storage: &StorageConfig,
        backend: Arc<dyn ObjectStore>,
    ) -> StorageResult<Self> {
        Self::assemble(storage, backend)
    }

    fn assemble(storage: &StorageConfig, datasets: Arc<dyn ObjectStore>) -> StorageResult<Self> {
        // The tables store — the catalog (`table.json`, …) and managed Lance data
        // — is either a redb single-file (persistent, exclusively locked) or an
        // ephemeral in-memory store, DuckDB-style.
        let tables: Arc<dyn ObjectStore> = match &storage.db_path {
            Some(path) => {
                tracing::info!(path = %path.display(), "tables store: redb single-file");
                Arc::new(RedbStore::open(path)?)
            }
            None => {
                tracing::info!("tables store: in-memory (ephemeral)");
                Arc::new(object_store::memory::InMemory::new())
            }
        };

        let tmp =
            Arc::new(LocalFileSystem::new_with_prefix(&storage.tmp_dir)?) as Arc<dyn ObjectStore>;

        tracing::info!("object stores initialized");
        Ok(Self {
            datasets,
            tables,
            tmp,
        })
    }
}

/// Build the datasets object store from the storage configuration.
///
/// - When `storage.s3` is `Some`, an S3-compatible store is configured from those
///   settings (credentials still come from the standard AWS chain; see
///   [`AmazonS3Builder::from_env`]).
/// - Otherwise a local filesystem store rooted at `storage.datasets_dir` is used.
///
/// [`AmazonS3Builder::from_env`]: object_store::aws::AmazonS3Builder::from_env
fn build_datasets_backend(storage: &StorageConfig) -> StorageResult<Arc<dyn ObjectStore>> {
    if let Some(s3) = &storage.s3 {
        tracing::info!("Using S3 object store for datasets");
        // `object_store::Error` converts into `StorageError::ObjectStore` via `?`.
        Ok(Arc::new(s3.amazon_s3_builder().build()?))
    } else {
        tracing::info!("Using local filesystem object store for datasets");
        let store = LocalFileSystem::new_with_prefix(&storage.datasets_dir)?
            .with_automatic_cleanup(true);
        Ok(Arc::new(store))
    }
}

/// Build a local filesystem datasets store rooted at `datasets_dir`.
///
/// Intended for tests and embedders that only need a plain local datasets store
/// without composing a full [`ObjectStores`].
pub async fn local_datasets_store(datasets_dir: PathBuf) -> StorageResult<Arc<dyn ObjectStore>> {
    let store = LocalFileSystem::new_with_prefix(&datasets_dir)?.with_automatic_cleanup(true);
    Ok(Arc::new(store))
}

/// Build an absolute local filesystem path for `object` under `root`.
///
/// The datasets store's object paths are relative to its local root, so a native
/// reader (netCDF/Atlas/TIFF/GDAL) that opens files directly rather than through
/// `object_store` joins them under `root` here. No `file://` scheme is emitted —
/// those libraries open local paths directly.
pub fn local_object_path(
    root: &Path,
    object: &object_store::path::Path,
) -> StorageResult<String> {
    let root = std::fs::canonicalize(root).map_err(|e| {
        crate::error::StorageError::InitializationError(format!(
            "Failed to canonicalize local datasets root {root:?}: {e}"
        ))
    })?;
    let full_path = root.join(object.as_ref());
    Ok(full_path.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_object_path_joins_object_under_root() {
        let dir = tempfile::tempdir().expect("temp dir");
        let got = local_object_path(dir.path(), &object_store::path::Path::from("a/b.nc")).unwrap();

        // Should be the canonicalized root joined with the object path; compare
        // against the same construction to stay platform-agnostic.
        let expected = std::fs::canonicalize(dir.path())
            .unwrap()
            .join("a/b.nc")
            .to_string_lossy()
            .into_owned();
        assert_eq!(got, expected);
        // Never a URL scheme — NetCDF opens local paths directly.
        assert!(!got.starts_with("file://"));
    }
}
