use std::sync::Arc;

use tokio::sync::Mutex;

use crate::manifest;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum IndexType {
    Multi {
        indexes: Vec<IndexType>,
    },
    ZOrder {
        columns: Vec<IndexType>,
    },
    Column {
        name: String,
    },
    GeoHash {
        longitude_column: String,
        latitude_column: String,
        precision: u8,
    },
}

pub struct IndexExec {
    manifest_path: object_store::path::Path,
    mutation_handle: Arc<Mutex<()>>,
    manifest_handle: Arc<Mutex<manifest::TableManifest>>,
    initial_manifest: manifest::TableManifest,
}
