use std::sync::Arc;

use object_store::ObjectStore;

pub type ManifestRef = Arc<TableManifest>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TableManifest {
    pub(crate) schema: arrow::datatypes::SchemaRef,
    pub(crate) schema_version: u64,
    pub(crate) data_files: Vec<DataFile>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DataFile {
    pub(crate) parquet_file: String,
    pub(crate) deletion_vector_files: Vec<String>, // Optional paths to deletion vector files for handling deletions.
}

pub async fn flush_table_manifest(
    store: Arc<dyn ObjectStore>,
    manifest_path: &object_store::path::Path,
    manifest: &TableManifest,
) -> anyhow::Result<()> {
    let manifest_data = serde_json::to_vec(manifest)?;
    store.put(manifest_path, manifest_data.into()).await?;
    Ok(())
}

pub async fn load_table_manifest(
    store: Arc<dyn ObjectStore>,
    manifest_path: &object_store::path::Path,
) -> anyhow::Result<TableManifest> {
    let manifest_data = store.get(manifest_path).await?.bytes().await?;
    let manifest: TableManifest = serde_json::from_slice(&manifest_data)?;
    Ok(manifest)
}
