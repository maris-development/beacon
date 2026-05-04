use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::schema::{DatasetId, SchemaStore};

// ─── CollectionFooter ─────────────────────────────────────────────────────────

/// The main metadata file for an atlas collection (`collection.atlas`).
///
/// Contains the interned schema store and a format version for future-proofing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionFooter {
    pub version: u32,
    pub schema_store: SchemaStore,
}

impl CollectionFooter {
    /// Current format version.
    pub const CURRENT_VERSION: u32 = 1;

    /// Create a new footer with an empty schema store.
    pub fn new() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            schema_store: SchemaStore::new(),
        }
    }

    /// Create a footer wrapping an existing schema store.
    pub fn with_schema_store(schema_store: SchemaStore) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            schema_store,
        }
    }

    /// Serialize using bincode + zstd.
    pub fn save(&self, writer: impl Write) -> anyhow::Result<()> {
        let encoder = zstd::Encoder::new(writer, 3)?.auto_finish();
        bincode::serialize_into(encoder, self)?;
        Ok(())
    }

    /// Deserialize from bincode + zstd.
    pub fn load(reader: impl Read) -> anyhow::Result<Self> {
        let decoder = zstd::Decoder::new(reader)?;
        let mut footer: Self = bincode::deserialize_from(decoder)?;
        footer.schema_store.rebuild_lookups();
        Ok(footer)
    }

    /// Save to an object store at the given path.
    pub async fn save_to_object_store(
        &self,
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        self.save(&mut buf)?;
        store.put(path, bytes::Bytes::from(buf).into()).await?;
        Ok(())
    }

    /// Load from an object store at the given path.
    pub async fn load_from_object_store(
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<Self> {
        let result = store.get(path).await?;
        let bytes = result.bytes().await?;
        Self::load(&bytes[..])
    }
}

// ─── DeletionMask ─────────────────────────────────────────────────────────────

/// A boolean mask tracking which datasets have been soft-deleted (`deleted.atlas`).
///
/// Indexed by `DatasetId.as_u32()`. A `true` value means the dataset is deleted
/// and should be invisible to readers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionMask {
    deleted: Vec<bool>,
}

impl DeletionMask {
    /// Create an empty mask.
    pub fn new() -> Self {
        Self {
            deleted: Vec::new(),
        }
    }

    pub fn as_slice(&self) -> &[bool] {
        &self.deleted
    }

    /// Check if a dataset has been deleted.
    pub fn is_deleted(&self, id: DatasetId) -> bool {
        let idx = id.as_u32() as usize;
        self.deleted.get(idx).copied().unwrap_or(false)
    }

    /// Mark a dataset as deleted.
    pub fn mark_deleted(&mut self, id: DatasetId) {
        let idx = id.as_u32() as usize;
        self.ensure_capacity(idx + 1);
        self.deleted[idx] = true;
    }

    /// Ensure the internal vec can hold at least `capacity` entries.
    pub fn ensure_capacity(&mut self, capacity: usize) {
        if self.deleted.len() < capacity {
            self.deleted.resize(capacity, false);
        }
    }

    /// Number of entries tracked (not the number of deleted datasets).
    pub fn len(&self) -> usize {
        self.deleted.len()
    }

    /// Returns true if no entries are tracked.
    pub fn is_empty(&self) -> bool {
        self.deleted.is_empty()
    }

    /// Serialize using bincode + zstd.
    pub fn save(&self, writer: impl Write) -> anyhow::Result<()> {
        let encoder = zstd::Encoder::new(writer, 3)?.auto_finish();
        bincode::serialize_into(encoder, self)?;
        Ok(())
    }

    /// Deserialize from bincode + zstd.
    pub fn load(reader: impl Read) -> anyhow::Result<Self> {
        let decoder = zstd::Decoder::new(reader)?;
        let mask: Self = bincode::deserialize_from(decoder)?;
        Ok(mask)
    }

    /// Save to an object store at the given path.
    pub async fn save_to_object_store(
        &self,
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        self.save(&mut buf)?;
        store.put(path, bytes::Bytes::from(buf).into()).await?;
        Ok(())
    }

    /// Load from an object store at the given path.
    /// Returns an empty mask if the file doesn't exist.
    pub async fn load_from_object_store(
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<Self> {
        match store.get(path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Self::load(&bytes[..])
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self::new()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnInput;
    use beacon_nd_array::datatypes::NdArrayDataType;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    #[test]
    fn test_collection_footer_roundtrip() {
        let mut footer = CollectionFooter::new();
        footer.schema_store.register_schema(
            "dataset-1",
            vec![
                ColumnInput {
                    name: "temp".into(),
                    data_type: NdArrayDataType::F64,
                },
                ColumnInput {
                    name: "depth".into(),
                    data_type: NdArrayDataType::F32,
                },
            ],
        );

        let mut buf = Vec::new();
        footer.save(&mut buf).unwrap();

        let loaded = CollectionFooter::load(&buf[..]).unwrap();
        assert_eq!(loaded.version, CollectionFooter::CURRENT_VERSION);
        assert_eq!(loaded.schema_store.dataset_count(), 1);
        let schema = loaded.schema_store.get_schema("dataset-1").unwrap();
        assert_eq!(schema.columns.len(), 2);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"temp"));
        assert!(names.contains(&"depth"));
    }

    #[test]
    fn test_deletion_mask_basic() {
        let mut mask = DeletionMask::new();
        assert!(mask.is_empty());

        let id0 = DatasetId::from_raw(0);
        let id1 = DatasetId::from_raw(1);
        let id5 = DatasetId::from_raw(5);

        assert!(!mask.is_deleted(id0));
        assert!(!mask.is_deleted(id5));

        mask.mark_deleted(id1);
        assert!(!mask.is_deleted(id0));
        assert!(mask.is_deleted(id1));
        assert!(!mask.is_deleted(id5));
        assert_eq!(mask.len(), 2); // expanded to hold index 1

        mask.mark_deleted(id5);
        assert!(mask.is_deleted(id5));
        assert_eq!(mask.len(), 6); // expanded to hold index 5
    }

    #[test]
    fn test_deletion_mask_roundtrip() {
        let mut mask = DeletionMask::new();
        mask.mark_deleted(DatasetId::from_raw(3));
        mask.mark_deleted(DatasetId::from_raw(7));

        let mut buf = Vec::new();
        mask.save(&mut buf).unwrap();

        let loaded = DeletionMask::load(&buf[..]).unwrap();
        assert!(!loaded.is_deleted(DatasetId::from_raw(0)));
        assert!(loaded.is_deleted(DatasetId::from_raw(3)));
        assert!(!loaded.is_deleted(DatasetId::from_raw(5)));
        assert!(loaded.is_deleted(DatasetId::from_raw(7)));
    }

    #[tokio::test]
    async fn test_footer_object_store_roundtrip() {
        let store = InMemory::new();
        let path = Path::from("test/collection.atlas");

        let mut footer = CollectionFooter::new();
        footer.schema_store.register_schema(
            "ocean-001",
            vec![ColumnInput {
                name: "salinity".into(),
                data_type: NdArrayDataType::F32,
            }],
        );

        footer.save_to_object_store(&store, &path).await.unwrap();
        let loaded = CollectionFooter::load_from_object_store(&store, &path)
            .await
            .unwrap();

        assert_eq!(loaded.schema_store.dataset_count(), 1);
        assert!(loaded.schema_store.get_schema("ocean-001").is_some());
    }

    #[tokio::test]
    async fn test_deletion_mask_missing_file_returns_empty() {
        let store = InMemory::new();
        let path = Path::from("nonexistent/deleted.atlas");

        let mask = DeletionMask::load_from_object_store(&store, &path)
            .await
            .unwrap();
        assert!(mask.is_empty());
    }

    #[tokio::test]
    async fn test_deletion_mask_object_store_roundtrip() {
        let store = InMemory::new();
        let path = Path::from("test/deleted.atlas");

        let mut mask = DeletionMask::new();
        mask.mark_deleted(DatasetId::from_raw(2));

        mask.save_to_object_store(&store, &path).await.unwrap();
        let loaded = DeletionMask::load_from_object_store(&store, &path)
            .await
            .unwrap();

        assert!(loaded.is_deleted(DatasetId::from_raw(2)));
        assert!(!loaded.is_deleted(DatasetId::from_raw(0)));
    }
}
