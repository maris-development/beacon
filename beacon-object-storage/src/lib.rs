//! Beacon object storage abstraction.
//!
//! This crate wraps implementations from [`object_store`] to provide a consistent
//! storage layer for Beacon.
//!
//! **Stores**
//! - **Datasets**: either local filesystem or S3-compatible storage (depending on
//!   config). See [`DatasetsStore`], which additionally hides Beacon-internal
//!   objects, serves listings from an optional event-driven cache, exposes a
//!   prefix-scoped event subscription, and provides NetCDF URL translation.
//! - **Tables** / **tmp**: local filesystem.

use std::{path::PathBuf, sync::Arc};

use object_store::{ObjectStore, local::LocalFileSystem};

pub mod config;
pub mod datasets_store;
pub mod error;
pub mod event;
pub mod fs_event_listener;
pub mod object_cache;

pub use config::{S3Config, StorageConfig};
pub use datasets_store::{DATASETS_WRITEABLE_PREFIX, DatasetsStore, local_datasets_store};

use crate::error::StorageResult;

/// The set of object stores owned by a single Beacon runtime.
///
/// Built once from a [`StorageConfig`] and the resolved data directories, then
/// owned by the runtime and handed to the components that need it. There is no
/// process-global store, so independent runtimes can use different backends.
#[derive(Clone)]
pub struct ObjectStores {
    /// Datasets store (local filesystem or S3, per `storage`).
    pub datasets: Arc<DatasetsStore>,
    /// Local filesystem store backing Beacon's tables.
    pub tables: Arc<dyn ObjectStore>,
    /// Local filesystem store backing Beacon's temporary files.
    pub tmp: Arc<dyn ObjectStore>,
}

impl ObjectStores {
    /// Build all three object stores, returning a structured error if any fails.
    ///
    /// `datasets_dir`/`tables_dir`/`tmp_dir` are the resolved local roots (from
    /// the runtime's configured data directories).
    pub async fn new(
        storage: &StorageConfig,
        datasets_dir: PathBuf,
        tables_dir: PathBuf,
        tmp_dir: PathBuf,
    ) -> StorageResult<Self> {
        let datasets =
            Arc::new(datasets_store::create_datasets_store(storage, datasets_dir).await?);

        let tables = Arc::new(
            LocalFileSystem::new_with_prefix(tables_dir)?.with_automatic_cleanup(true),
        ) as Arc<dyn ObjectStore>;

        let tmp = Arc::new(LocalFileSystem::new_with_prefix(tmp_dir)?) as Arc<dyn ObjectStore>;

        tracing::info!("object stores initialized");
        Ok(Self {
            datasets,
            tables,
            tmp,
        })
    }
}
