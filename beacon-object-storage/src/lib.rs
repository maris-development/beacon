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
//! - **Tables**: a single-file [`beacon_redb_store::RedbStore`] (`beacon.db`)
//!   holding the catalog and managed Lance data.
//! - **Tmp**: local filesystem.

use std::sync::Arc;

use beacon_redb_store::RedbStore;
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
    /// The local roots and S3 backend selection all come from `storage`.
    pub async fn new(storage: &StorageConfig) -> StorageResult<Self> {
        let datasets = Arc::new(datasets_store::create_datasets_store(storage).await?);

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
