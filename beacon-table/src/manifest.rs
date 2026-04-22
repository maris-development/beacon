//! Table manifest management for Beacon tables.
//!
//! The manifest is a JSON file (`manifest.json`) stored alongside the table data that
//! tracks the table schema, schema version, and the list of data files that make up
//! the table. All mutations (inserts, vacuums) update the manifest atomically to
//! maintain consistency.

use std::sync::Arc;

use object_store::ObjectStore;

use crate::index_exec::TableIndex;

/// A reference-counted pointer to a [`TableManifest`].
pub type ManifestRef = Arc<TableManifest>;

/// The persistent metadata for a Beacon table.
///
/// Serialized as JSON and stored at `<table_directory>/manifest.json` in the
/// object store. It records the current Arrow schema, a monotonically increasing
/// schema version (used for optimistic concurrency control), and the ordered
/// list of data files that comprise the table's content.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TableManifest {
    /// The Arrow schema shared by every data file in this table.
    pub(crate) schema: arrow::datatypes::SchemaRef,

    /// Monotonically increasing version number, bumped on schema changes.
    /// Used to detect concurrent mutations during insert and vacuum operations.
    pub(crate) schema_version: u64,

    /// Ordered list of data files that make up the table's content.
    /// Each entry references a Parquet file and optional deletion vectors.
    pub(crate) data_files: Vec<DataFile>,

    /// Optional index configuration for the table (clustered or Z-order).
    #[serde(default)]
    pub(crate) index: Option<TableIndex>,
}

/// A single data file entry within the table manifest.
///
/// Each data file consists of a primary Parquet file containing the row data
/// and zero or more deletion vector files that mark rows as logically deleted.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DataFile {
    /// Relative path to the Parquet file within the table directory.
    pub(crate) parquet_file: String,

    /// Relative paths to deletion vector Parquet files.
    ///
    /// Each deletion vector file contains a single boolean column where `true`
    /// indicates the corresponding row has been logically deleted.
    pub(crate) deletion_vector_files: Vec<String>,
}

/// Persists a [`TableManifest`] to the object store as JSON.
///
/// Overwrites the manifest file at `manifest_path` atomically using the
/// object store's `put` operation.
pub async fn flush_table_manifest(
    store: Arc<dyn ObjectStore>,
    manifest_path: &object_store::path::Path,
    manifest: &TableManifest,
) -> anyhow::Result<()> {
    let manifest_data = serde_json::to_vec(manifest)?;
    store.put(manifest_path, manifest_data.into()).await?;
    Ok(())
}

/// Loads a [`TableManifest`] from the object store.
///
/// Reads and deserializes the JSON manifest file at `manifest_path`.
pub async fn load_table_manifest(
    store: Arc<dyn ObjectStore>,
    manifest_path: &object_store::path::Path,
) -> anyhow::Result<TableManifest> {
    let manifest_data = store.get(manifest_path).await?.bytes().await?;
    let manifest: TableManifest = serde_json::from_slice(&manifest_data)?;
    Ok(manifest)
}
