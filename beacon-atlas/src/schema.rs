//! Memory-efficient schema store for millions of datasets.
//!
//! This module provides [`SchemaStore`], an interned, deduplicated registry that maps
//! dataset names to their column schemas. It is designed to hold millions of dataset
//! schemas in memory with minimal overhead by sharing common strings, column
//! definitions, and entire schemas across datasets.
//!
//! # Architecture
//!
//! The store uses three levels of deduplication:
//!
//! 1. **StringPool** — all unique strings stored once, referenced by [`StringId`]
//! 2. **ColumnDefPool** — unique (name, datatype) tuples, referenced by [`ColumnDefId`]
//! 3. **SchemaPool** — unique ordered column-def lists, referenced by [`SchemaId`]
//!
//! A dataset index maps each dataset name to its [`SchemaId`]. When many datasets
//! share the same schema (common in practice), only one copy of the schema is stored.
//!
//! # Example
//!
//! ```rust
//! use beacon_atlas::schema::{SchemaStore, ColumnInput};
//! use beacon_nd_array::datatypes::NdArrayDataType;
//!
//! let mut store = SchemaStore::new();
//!
//! // Register a dataset with two columns
//! let entry = store.register_schema("ocean-buoy-001", vec![
//!     ColumnInput { name: "temperature".to_string(), data_type: NdArrayDataType::F32 },
//!     ColumnInput { name: "depth".to_string(), data_type: NdArrayDataType::F64 },
//! ]);
//!
//! // Another dataset with the same schema shares the SchemaId
//! let entry2 = store.register_schema("ocean-buoy-002", vec![
//!     ColumnInput { name: "temperature".to_string(), data_type: NdArrayDataType::F32 },
//!     ColumnInput { name: "depth".to_string(), data_type: NdArrayDataType::F64 },
//! ]);
//!
//! assert_eq!(entry.schema_id, entry2.schema_id); // Same schema, deduplicated
//! assert_ne!(entry.dataset_id, entry2.dataset_id); // Different dataset ids
//! assert_eq!(store.dataset_count(), 2);
//! assert_eq!(store.schema_count(), 1);
//!
//! // Look up a dataset's schema
//! let resolved = store.get_schema("ocean-buoy-001").unwrap();
//! assert_eq!(resolved.columns.len(), 2);
//! ```
//!
//! # Persistence
//!
//! The store serializes to/from bincode for compact on-disk storage:
//!
//! ```rust
//! use beacon_atlas::schema::{SchemaStore, ColumnInput};
//! use beacon_nd_array::datatypes::NdArrayDataType;
//!
//! let mut store = SchemaStore::new();
//! store.register_schema("ds-1", vec![
//!     ColumnInput { name: "value".to_string(), data_type: NdArrayDataType::F64 },
//! ]);
//!
//! // Save to bytes
//! let mut buf = Vec::new();
//! store.save(&mut buf).unwrap();
//!
//! // Load from bytes
//! let loaded = SchemaStore::load(&buf[..]).unwrap();
//! assert_eq!(loaded.dataset_count(), 1);
//! ```

use std::collections::HashMap;
use std::io::{Read, Write};

use beacon_nd_array::datatypes::NdArrayDataType;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

// ─── ID Newtypes ──────────────────────────────────────────────────────────────

/// Interned string identifier. Lightweight handle (4 bytes) that references
/// a string in the [`StringPool`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StringId(u32);

/// Interned column definition identifier. References a unique
/// (name, datatype) tuple in the [`ColumnDefPool`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDefId(u32);

/// Interned schema identifier. References a unique ordered list of column
/// definitions in the [`SchemaPool`]. Multiple datasets can share the same
/// [`SchemaId`] when they have identical column structures.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaId(u32);

/// Incremental dataset index, assigned on registration and never reused.
///
/// Even after a dataset is removed, its `DatasetId` is not recycled.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DatasetId(u32);

impl DatasetId {
    /// Returns the raw `u32` value of this dataset index.
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

// ─── CompactDataType ──────────────────────────────────────────────────────────

/// The data type used for column definitions, re-exported from `beacon-nd-array`.
pub type DataType = NdArrayDataType;

// ─── Interned Structs ─────────────────────────────────────────────────────────

/// An interned column definition consisting of a name and data type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: StringId,
    pub data_type: DataType,
}

/// An interned schema — an ordered list of column definitions.
///
/// Columns are stored sorted by name for deterministic identity: two schemas
/// with the same columns in different insertion order will produce the same [`SchemaId`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnDefId>,
}

// ─── Pools ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringPool {
    strings: Vec<Box<str>>,
    #[serde(skip)]
    lookup: HashMap<Box<str>, StringId>,
}

impl StringPool {
    fn new() -> Self {
        Self {
            strings: Vec::new(),
            lookup: HashMap::new(),
        }
    }

    pub fn intern(&mut self, s: &str) -> StringId {
        if let Some(&id) = self.lookup.get(s) {
            return id;
        }
        let id = StringId(self.strings.len() as u32);
        let boxed: Box<str> = s.into();
        self.lookup.insert(boxed.clone(), id);
        self.strings.push(boxed);
        id
    }

    pub fn resolve(&self, id: StringId) -> &str {
        &self.strings[id.0 as usize]
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    fn rebuild_lookup(&mut self) {
        self.lookup.clear();
        self.lookup.reserve(self.strings.len());
        for (i, s) in self.strings.iter().enumerate() {
            self.lookup.insert(s.clone(), StringId(i as u32));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefPool {
    defs: Vec<ColumnDef>,
    #[serde(skip)]
    lookup: HashMap<ColumnDef, ColumnDefId>,
}

impl ColumnDefPool {
    fn new() -> Self {
        Self {
            defs: Vec::new(),
            lookup: HashMap::new(),
        }
    }

    pub fn intern(&mut self, def: ColumnDef) -> ColumnDefId {
        if let Some(&id) = self.lookup.get(&def) {
            return id;
        }
        let id = ColumnDefId(self.defs.len() as u32);
        self.lookup.insert(def.clone(), id);
        self.defs.push(def);
        id
    }

    pub fn resolve(&self, id: ColumnDefId) -> &ColumnDef {
        &self.defs[id.0 as usize]
    }

    pub fn len(&self) -> usize {
        self.defs.len()
    }

    fn rebuild_lookup(&mut self) {
        self.lookup.clear();
        self.lookup.reserve(self.defs.len());
        for (i, def) in self.defs.iter().enumerate() {
            self.lookup.insert(def.clone(), ColumnDefId(i as u32));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaPool {
    schemas: Vec<Schema>,
    #[serde(skip)]
    lookup: HashMap<Vec<ColumnDefId>, SchemaId>,
}

impl SchemaPool {
    fn new() -> Self {
        Self {
            schemas: Vec::new(),
            lookup: HashMap::new(),
        }
    }

    pub fn intern(&mut self, schema: Schema) -> SchemaId {
        if let Some(&id) = self.lookup.get(&schema.columns) {
            return id;
        }
        let id = SchemaId(self.schemas.len() as u32);
        self.lookup.insert(schema.columns.clone(), id);
        self.schemas.push(schema);
        id
    }

    pub fn resolve(&self, id: SchemaId) -> &Schema {
        &self.schemas[id.0 as usize]
    }

    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    fn rebuild_lookup(&mut self) {
        self.lookup.clear();
        self.lookup.reserve(self.schemas.len());
        for (i, schema) in self.schemas.iter().enumerate() {
            self.lookup
                .insert(schema.columns.clone(), SchemaId(i as u32));
        }
    }
}

// ─── User-Facing Input / Output Types ────────────────────────────────────────

/// Input type for registering a column definition.
///
/// This is the user-facing struct passed to [`SchemaStore::register_schema`].
///
/// # Example
///
/// ```rust
/// use beacon_atlas::schema::ColumnInput;
/// use beacon_nd_array::datatypes::NdArrayDataType;
///
/// let col = ColumnInput {
///     name: "temperature".to_string(),
///     data_type: NdArrayDataType::F32,
/// };
/// ```
pub struct ColumnInput {
    pub name: String,
    pub data_type: DataType,
}

/// A fully resolved schema with all strings materialized.
///
/// Returned by [`SchemaStore::get_schema`] and [`SchemaStore::resolve_schema`].
/// This is a standalone value that does not borrow from the store.
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    pub columns: Vec<ResolvedColumn>,
}

/// A fully resolved column definition with materialized strings.
#[derive(Debug, Clone)]
pub struct ResolvedColumn {
    pub name: String,
    pub data_type: DataType,
}

/// Entry stored per dataset in the index.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetEntry {
    pub dataset_id: DatasetId,
    pub schema_id: SchemaId,
}

// ─── SchemaStore ──────────────────────────────────────────────────────────────

/// A memory-efficient, interned schema registry for millions of datasets.
///
/// `SchemaStore` maps dataset names to their column schemas using multi-level
/// deduplication. Identical strings, column definitions, and entire schemas
/// are stored only once, making it practical to hold millions of dataset
/// schemas in memory.
///
/// # Usage
///
/// ```rust
/// use beacon_atlas::schema::{SchemaStore, ColumnInput};
/// use beacon_nd_array::datatypes::NdArrayDataType;
///
/// let mut store = SchemaStore::new();
///
/// // Register datasets
/// store.register_schema("argo-float-001", vec![
///     ColumnInput { name: "temperature".to_string(), data_type: NdArrayDataType::F32 },
///     ColumnInput { name: "pressure".to_string(), data_type: NdArrayDataType::F32 },
/// ]);
///
/// // Look up a schema
/// let schema = store.get_schema("argo-float-001").unwrap();
/// assert_eq!(schema.columns.len(), 2);
///
/// // Persistence via bincode
/// let mut buf = Vec::new();
/// store.save(&mut buf).unwrap();
/// let loaded = SchemaStore::load(&buf[..]).unwrap();
/// assert_eq!(loaded.dataset_count(), 1);
/// ```
///
/// # Memory characteristics
///
/// For 1M datasets with 10K unique column definitions (20 columns each):
/// - Minimal memory overhead per dataset (just a SchemaId reference)
/// - Serialized ~20 bytes per dataset on disk (amortized)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaStore {
    strings: StringPool,
    column_defs: ColumnDefPool,
    schemas: SchemaPool,
    /// Maps dataset name (StringId) → DatasetEntry, ordered by insertion.
    dataset_index: IndexMap<StringId, DatasetEntry>,
    /// Monotonically increasing counter for the next dataset id.
    next_dataset_id: u32,
}

impl SchemaStore {
    /// Create a new, empty schema store.
    pub fn new() -> Self {
        Self {
            strings: StringPool::new(),
            column_defs: ColumnDefPool::new(),
            schemas: SchemaPool::new(),
            dataset_index: IndexMap::new(),
            next_dataset_id: 0,
        }
    }

    /// Register a dataset's schema. Returns the assigned [`DatasetEntry`]
    /// containing both the incremental [`DatasetId`] and the interned [`SchemaId`].
    ///
    /// If a dataset with the same name was already registered, the existing
    /// entry is returned without modification. Column order does not matter
    /// for identity — columns are sorted by name internally.
    ///
    /// # Example
    ///
    /// ```rust
    /// use beacon_atlas::schema::{SchemaStore, ColumnInput};
    /// use beacon_nd_array::datatypes::NdArrayDataType;
    ///
    /// let mut store = SchemaStore::new();
    ///
    /// let entry = store.register_schema("my-dataset", vec![
    ///     ColumnInput { name: "salinity".to_string(), data_type: NdArrayDataType::F64 },
    /// ]);
    /// assert_eq!(entry.dataset_id.as_u32(), 0);
    ///
    /// // Re-registering the same dataset returns the same entry
    /// let entry2 = store.register_schema("my-dataset", vec![]);
    /// assert_eq!(entry, entry2);
    /// ```
    pub fn register_schema(
        &mut self,
        dataset_name: &str,
        columns: impl IntoIterator<Item = ColumnInput>,
    ) -> DatasetEntry {
        let name_id = self.strings.intern(dataset_name);

        if let Some(&existing) = self.dataset_index.get(&name_id) {
            return existing;
        }

        let mut col_def_ids: Vec<(StringId, ColumnDefId)> = columns
            .into_iter()
            .map(|col| {
                let col_name_id = self.strings.intern(&col.name);
                let def = ColumnDef {
                    name: col_name_id,
                    data_type: col.data_type,
                };
                let def_id = self.column_defs.intern(def);
                (col_name_id, def_id)
            })
            .collect();

        // Sort columns by name for deterministic schema identity
        col_def_ids.sort_by_key(|(name_id, _)| self.strings.resolve(*name_id).to_owned());
        let columns_sorted: Vec<ColumnDefId> = col_def_ids.into_iter().map(|(_, id)| id).collect();

        let schema = Schema {
            columns: columns_sorted,
        };
        let schema_id = self.schemas.intern(schema);
        let dataset_id = DatasetId(self.next_dataset_id);
        self.next_dataset_id += 1;
        let entry = DatasetEntry {
            dataset_id,
            schema_id,
        };
        self.dataset_index.insert(name_id, entry);

        entry
    }

    /// Look up a dataset's schema by name, returning a fully resolved copy.
    ///
    /// Returns `None` if the dataset name was never registered.
    ///
    /// # Example
    ///
    /// ```rust
    /// use beacon_atlas::schema::{SchemaStore, ColumnInput};
    /// use beacon_nd_array::datatypes::NdArrayDataType;
    ///
    /// let mut store = SchemaStore::new();
    /// store.register_schema("ds-1", vec![
    ///     ColumnInput { name: "temp".to_string(), data_type: NdArrayDataType::F32 },
    /// ]);
    ///
    /// let schema = store.get_schema("ds-1").unwrap();
    /// assert_eq!(schema.columns[0].name, "temp");
    /// assert!(store.get_schema("nonexistent").is_none());
    /// ```
    pub fn get_schema(&self, dataset_name: &str) -> Option<ResolvedSchema> {
        let name_id = self.strings.lookup.get(dataset_name)?;
        let entry = self.dataset_index.get(name_id)?;
        Some(self.resolve_schema(entry.schema_id))
    }

    /// Get the raw [`SchemaId`] for a dataset without resolving it.
    pub fn get_schema_id(&self, dataset_name: &str) -> Option<SchemaId> {
        let name_id = self.strings.lookup.get(dataset_name)?;
        self.dataset_index.get(name_id).map(|e| e.schema_id)
    }

    /// Get the full [`DatasetEntry`] (dataset id + schema id) for a dataset.
    pub fn get_dataset_entry(&self, dataset_name: &str) -> Option<DatasetEntry> {
        let name_id = self.strings.lookup.get(dataset_name)?;
        self.dataset_index.get(name_id).copied()
    }

    /// Get the [`DatasetId`] for a dataset by name.
    pub fn get_dataset_id(&self, dataset_name: &str) -> Option<DatasetId> {
        self.get_dataset_entry(dataset_name).map(|e| e.dataset_id)
    }

    /// Resolve a [`SchemaId`] to a full [`ResolvedSchema`] with all strings materialized.
    pub fn resolve_schema(&self, id: SchemaId) -> ResolvedSchema {
        let schema = self.schemas.resolve(id);
        let columns = schema
            .columns
            .iter()
            .map(|&col_def_id| {
                let def = self.column_defs.resolve(col_def_id);
                ResolvedColumn {
                    name: self.strings.resolve(def.name).to_owned(),
                    data_type: def.data_type.clone(),
                }
            })
            .collect();
        ResolvedSchema { columns }
    }

    /// Number of registered datasets.
    pub fn dataset_count(&self) -> usize {
        self.dataset_index.len()
    }

    /// Returns an iterator over all registered datasets in insertion order.
    pub fn datasets(&self) -> impl Iterator<Item = (&str, DatasetEntry)> {
        self.dataset_index
            .iter()
            .map(|(name_id, entry)| (self.strings.resolve(*name_id), *entry))
    }

    /// Returns an iterator over all registered dataset names in insertion order.
    pub fn dataset_names(&self) -> impl Iterator<Item = &str> {
        self.dataset_index
            .keys()
            .map(|id| self.strings.resolve(*id))
    }

    /// Remove a dataset by name. Returns the removed [`DatasetEntry`] if it existed.
    pub fn remove_dataset(&mut self, dataset_name: &str) -> Option<DatasetEntry> {
        let name_id = *self.strings.lookup.get(dataset_name)?;
        self.dataset_index.shift_remove(&name_id)
    }

    /// Number of unique schemas.
    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }

    /// Number of unique interned strings.
    pub fn string_count(&self) -> usize {
        self.strings.len()
    }

    /// Number of unique column definitions.
    pub fn column_def_count(&self) -> usize {
        self.column_defs.len()
    }

    // ─── Persistence ──────────────────────────────────────────────────────

    /// Serialize the entire store to a writer using bincode + zstd compression.
    ///
    /// # Example
    ///
    /// ```rust
    /// use beacon_atlas::schema::SchemaStore;
    ///
    /// let store = SchemaStore::new();
    /// let mut buf = Vec::new();
    /// store.save(&mut buf).unwrap();
    /// assert!(!buf.is_empty());
    /// ```
    pub fn save(&self, writer: impl Write) -> anyhow::Result<()> {
        let encoder = zstd::Encoder::new(writer, 3)?.auto_finish();
        bincode::serialize_into(encoder, self)?;
        Ok(())
    }

    /// Deserialize a store from a zstd-compressed reader, rebuilding internal lookup indexes.
    ///
    /// # Example
    ///
    /// ```rust
    /// use beacon_atlas::schema::{SchemaStore, ColumnInput};
    /// use beacon_nd_array::datatypes::NdArrayDataType;
    ///
    /// let mut store = SchemaStore::new();
    /// store.register_schema("ds", vec![
    ///     ColumnInput { name: "x".into(), data_type: NdArrayDataType::F64 },
    /// ]);
    ///
    /// let mut buf = Vec::new();
    /// store.save(&mut buf).unwrap();
    ///
    /// let loaded = SchemaStore::load(&buf[..]).unwrap();
    /// assert_eq!(loaded.dataset_count(), 1);
    /// ```
    pub fn load(reader: impl Read) -> anyhow::Result<Self> {
        let decoder = zstd::Decoder::new(reader)?;
        let mut store: Self = bincode::deserialize_from(decoder)?;
        store.rebuild_lookups();
        Ok(store)
    }

    /// Save to an object store path (async).
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

    /// Load from an object store path (async).
    pub async fn load_from_object_store(
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<Self> {
        let result = store.get(path).await?;
        let bytes = result.bytes().await?;
        Self::load(&bytes[..])
    }

    fn rebuild_lookups(&mut self) {
        self.strings.rebuild_lookup();
        self.column_defs.rebuild_lookup();
        self.schemas.rebuild_lookup();
    }
}

impl Default for SchemaStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::datatypes::NdArrayDataType as DT;

    fn make_column(name: &str, dt: DT) -> ColumnInput {
        ColumnInput {
            name: name.to_string(),
            data_type: dt,
        }
    }

    #[test]
    fn test_register_and_lookup() {
        let mut store = SchemaStore::new();

        let cols = vec![
            make_column("temperature", DT::F32),
            make_column("depth", DT::F64),
        ];

        let id = store.register_schema("dataset-0", cols);
        assert_eq!(store.dataset_count(), 1);
        assert_eq!(store.schema_count(), 1);

        let resolved = store.get_schema("dataset-0").unwrap();
        assert_eq!(resolved.columns.len(), 2);

        // Same schema registered under different dataset name should share SchemaId
        let cols2 = vec![
            make_column("temperature", DT::F32),
            make_column("depth", DT::F64),
        ];
        let id2 = store.register_schema("dataset-1", cols2);
        assert_eq!(id.schema_id, id2.schema_id);
        assert_ne!(id.dataset_id, id2.dataset_id);
        assert_eq!(store.dataset_count(), 2);
        assert_eq!(store.schema_count(), 1);
    }

    #[test]
    fn test_different_schemas() {
        let mut store = SchemaStore::new();

        let cols_a = vec![make_column("temperature", DT::F32)];
        let cols_b = vec![make_column("salinity", DT::F64)];

        let id_a = store.register_schema("ds-a", cols_a);
        let id_b = store.register_schema("ds-b", cols_b);
        assert_ne!(id_a, id_b);
        assert_eq!(store.schema_count(), 2);
    }

    #[test]
    fn test_deduplication_across_registrations() {
        let mut store = SchemaStore::new();

        for i in 0..100 {
            let cols = vec![
                make_column("temperature", DT::F32),
                make_column("depth", DT::F64),
                make_column("salinity", DT::F64),
            ];
            store.register_schema(&format!("dataset-{}", i), cols);
        }

        assert_eq!(store.dataset_count(), 100);
        assert_eq!(store.schema_count(), 1);
        assert_eq!(store.column_def_count(), 3);
    }

    #[test]
    fn test_bincode_roundtrip() {
        let mut store = SchemaStore::new();

        let cols = vec![
            make_column("temperature", DT::F32),
            make_column("depth", DT::F64),
        ];
        store.register_schema("dataset-0", cols);

        let cols2 = vec![make_column("salinity", DT::F64)];
        store.register_schema("dataset-1", cols2);

        // Serialize
        let mut buf = Vec::new();
        store.save(&mut buf).unwrap();

        // Deserialize
        let loaded = SchemaStore::load(&buf[..]).unwrap();

        assert_eq!(loaded.dataset_count(), 2);
        assert_eq!(loaded.schema_count(), 2);

        let resolved = loaded.get_schema("dataset-0").unwrap();
        assert_eq!(resolved.columns.len(), 2);

        // Verify deduplication still works after reload
        let mut loaded = loaded;
        let cols3 = vec![
            make_column("temperature", DT::F32),
            make_column("depth", DT::F64),
        ];
        let id = loaded.register_schema("dataset-2", cols3);
        assert_eq!(loaded.get_schema_id("dataset-0").unwrap(), id.schema_id);
        assert_eq!(loaded.schema_count(), 2);
    }

    #[tokio::test]
    async fn test_object_store_roundtrip() {
        let obj_store = object_store::memory::InMemory::new();
        let path = object_store::path::Path::from("test/schema_store.bin");

        let mut store = SchemaStore::new();
        let cols = vec![
            make_column("temperature", DT::F32),
            make_column("depth", DT::F64),
        ];
        store.register_schema("dataset-0", cols);

        store.save_to_object_store(&obj_store, &path).await.unwrap();

        let loaded = SchemaStore::load_from_object_store(&obj_store, &path)
            .await
            .unwrap();
        assert_eq!(loaded.dataset_count(), 1);
        assert_eq!(loaded.schema_count(), 1);

        let resolved = loaded.get_schema("dataset-0").unwrap();
        assert_eq!(resolved.columns.len(), 2);
    }

    #[test]
    fn test_remove_dataset() {
        let mut store = SchemaStore::new();
        store.register_schema("ds-1", vec![make_column("x", DT::F64)]);
        assert_eq!(store.dataset_count(), 1);

        let removed = store.remove_dataset("ds-1");
        assert!(removed.is_some());
        assert_eq!(store.dataset_count(), 0);

        assert!(store.remove_dataset("ds-1").is_none());
    }

    #[test]
    #[ignore] // Run with: cargo test -p beacon-atlas -- --ignored --nocapture
    fn test_memory_consumption() {
        let num_datasets: usize = 1_000_000;
        let cols_per_dataset: usize = 20;
        let target_unique_col_defs: usize = 10_000;

        let col_names: Vec<String> = (0..1000).map(|i| format!("column_{:04}", i)).collect();
        let data_types = [
            DT::F32,
            DT::F64,
            DT::I32,
            DT::I64,
            DT::String,
            DT::U32,
            DT::U64,
            DT::I16,
            DT::Bool,
            DT::Timestamp,
        ];

        let mut store = SchemaStore::new();

        for i in 0..num_datasets {
            let columns: Vec<ColumnInput> = (0..cols_per_dataset)
                .map(|c| {
                    let slot = i * cols_per_dataset + c;
                    let unique_def_idx = slot % target_unique_col_defs;
                    let name_idx = unique_def_idx % col_names.len();
                    let type_idx = (unique_def_idx / col_names.len()) % data_types.len();

                    ColumnInput {
                        name: col_names[name_idx].clone(),
                        data_type: data_types[type_idx].clone(),
                    }
                })
                .collect();

            store.register_schema(&format!("dataset-{}", i), columns);
        }

        assert_eq!(store.dataset_count(), num_datasets);

        let mut buf = Vec::new();
        store.save(&mut buf).unwrap();
        let serialized_mb = buf.len() as f64 / (1024.0 * 1024.0);
        println!(
            "Serialized: {:.2} MB ({:.0} bytes/dataset)",
            serialized_mb,
            buf.len() as f64 / num_datasets as f64
        );
    }
}
