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

pub use datasets_store::{DATASETS_WRITEABLE_PREFIX, DatasetsStore};

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

/// Eagerly initializes all three object stores, returning a structured error if
/// any fails to build.
///
/// Call this once early in startup so configuration/I/O problems surface cleanly
/// here. Once a store's cell is populated, the corresponding `get_*` accessor
/// returns it without re-running (and therefore without the fallback panic).
pub async fn init_datastores() -> StorageResult<()> {
    DATASETS_OBJECT_STORE
        .get_or_try_init(build_datasets_store)
        .await?;
    TABLES_OBJECT_STORE
        .get_or_try_init(|| async { build_tables_store() })
        .await?;
    TMP_OBJECT_STORE
        .get_or_try_init(|| async { build_tmp_store() })
        .await?;
    tracing::info!("object stores initialized");
    Ok(())
}

/// Builds the datasets store from configuration.
async fn build_datasets_store() -> StorageResult<Arc<DatasetsStore>> {
    Ok(Arc::new(datasets_store::create_datasets_store().await?))
}

/// Builds the local filesystem store backing Beacon's tables.
fn build_tables_store() -> StorageResult<Arc<dyn ObjectStore>> {
    let store = LocalFileSystem::new_with_prefix(beacon_config::TABLES_DIR.clone())?
        .with_automatic_cleanup(true);
    Ok(Arc::new(store) as Arc<dyn ObjectStore>)
}

/// Builds the local filesystem store backing Beacon's temporary files.
fn build_tmp_store() -> StorageResult<Arc<dyn ObjectStore>> {
    let store = LocalFileSystem::new_with_prefix(temp_dir())?;
    Ok(Arc::new(store) as Arc<dyn ObjectStore>)
}

pub async fn get_datasets_object_store() -> Arc<DatasetsStore> {
    DATASETS_OBJECT_STORE
        .get_or_init(|| async {
            build_datasets_store().await.unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to initialize datasets object store");
                panic!("failed to initialize datasets object store: {e}");
            })
        })
        .await
        .clone()
}

pub async fn get_tables_object_store() -> Arc<dyn ObjectStore> {
    TABLES_OBJECT_STORE
        .get_or_init(|| async {
            // For tables, we always use the local filesystem.
            build_tables_store().unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to initialize tables object store");
                panic!("failed to initialize tables object store: {e}");
            })
        })
        .await
        .clone()
}

pub async fn get_tmp_object_store() -> Arc<dyn ObjectStore> {
    TMP_OBJECT_STORE
        .get_or_init(|| async {
            build_tmp_store().unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to initialize tmp object store");
                panic!("failed to initialize tmp object store: {e}");
            })
        })
        .await
        .clone()
}
