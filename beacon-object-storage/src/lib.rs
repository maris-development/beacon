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

use std::{env::temp_dir, sync::Arc};

use object_store::{ObjectStore, local::LocalFileSystem};

pub mod datasets_store;
pub mod error;
pub mod event;
pub mod fs_event_listener;
pub mod object_cache;

pub use datasets_store::DatasetsStore;

use crate::error::StorageResult;

/// Global datasets store.
///
/// Using a `OnceCell` ensures we build the store exactly once (important so any
/// background event-polling task and its caches live for the process lifetime).
static DATASETS_OBJECT_STORE: tokio::sync::OnceCell<Arc<DatasetsStore>> =
    tokio::sync::OnceCell::const_new();
static TABLES_OBJECT_STORE: tokio::sync::OnceCell<Arc<dyn ObjectStore>> =
    tokio::sync::OnceCell::const_new();
static TMP_OBJECT_STORE: tokio::sync::OnceCell<Arc<dyn ObjectStore>> =
    tokio::sync::OnceCell::const_new();

pub async fn init_datastores() -> StorageResult<()> {
    let _ = get_datasets_object_store().await;
    let _ = get_tables_object_store().await;
    let _ = get_tmp_object_store().await;
    Ok(())
}

pub async fn get_datasets_object_store() -> Arc<DatasetsStore> {
    DATASETS_OBJECT_STORE
        .get_or_init(|| async {
            Arc::new(
                datasets_store::create_datasets_store()
                    .await
                    .expect("Failed to initialize datasets object store"),
            )
        })
        .await
        .clone()
}

pub async fn get_tables_object_store() -> Arc<dyn ObjectStore> {
    TABLES_OBJECT_STORE
        .get_or_init(|| async {
            // For tables, we always use the local filesystem
            Arc::new(
                LocalFileSystem::new_with_prefix(beacon_config::TABLES_DIR.clone())
                    .expect("Failed to initialize tables object store"),
            ) as Arc<dyn ObjectStore>
        })
        .await
        .clone()
}

pub async fn get_tmp_object_store() -> Arc<dyn ObjectStore> {
    TMP_OBJECT_STORE
        .get_or_init(|| async {
            Arc::new(
                LocalFileSystem::new_with_prefix(temp_dir())
                    .expect("Failed to initialize tmp object store"),
            ) as Arc<dyn ObjectStore>
        })
        .await
        .clone()
}
