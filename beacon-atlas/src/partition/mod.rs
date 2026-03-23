pub mod ops;

use std::{collections::HashMap, sync::Arc};

use arrow::{array::AsArray, datatypes::UInt32Type};

use crate::{
    array::io_cache::IoCache,
    column::ColumnReader,
    consts::PARTITION_METADATA_FILE,
    partition::ops::{load_partition_entries, read::init_column_reader},
    schema::AtlasSchema,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionMetadata {
    pub name: String,
    pub description: Option<String>,
    pub schema: AtlasSchema,
}

#[derive(Debug, Clone)]
pub struct Partition<S: object_store::ObjectStore + Clone> {
    store: S,
    name: String,
    directory: object_store::path::Path,
    metadata: PartitionMetadata,
    state: PartitionState,
    reader_cache:
        Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::OnceCell<Arc<ColumnReader<S>>>>>>>,
    io_cache: Arc<IoCache>,
}

impl<S: object_store::ObjectStore + Clone> Partition<S> {
    pub(crate) fn new(
        store: S,
        name: String,
        directory: object_store::path::Path,
        metadata: PartitionMetadata,
        state: PartitionState,
        io_cache: Arc<IoCache>,
    ) -> Self {
        let reader_cache = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        Self {
            store,
            name,
            directory,
            metadata,
            state,
            reader_cache,
            io_cache,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> &object_store::path::Path {
        &self.directory
    }

    pub fn metadata(&self) -> &PartitionMetadata {
        &self.metadata
    }

    pub fn schema(&self) -> &AtlasSchema {
        &self.metadata.schema
    }

    pub fn arrow_schema(&self) -> arrow::datatypes::Schema {
        self.schema().to_arrow_schema()
    }

    pub(crate) async fn column_reader(
        &self,
        column_name: &str,
    ) -> anyhow::Result<Arc<ColumnReader<S>>> {
        let mut cache = self.reader_cache.lock().await;
        let cell = cache
            .entry(column_name.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone();
        let store = self.store.clone();
        let partition_path = self.directory.clone();
        let io_cache = self.io_cache.clone();

        let reader_result = cell
            .get_or_try_init(|| async move {
                init_column_reader(store, &partition_path, column_name, io_cache).await
            })
            .await;

        reader_result.cloned()
    }

    pub(crate) fn state(&self) -> &PartitionState {
        &self.state
    }

    pub fn entry_keys(&self) -> &[String] {
        self.state().entry_keys()
    }

    pub fn dataset_indexes(&self) -> &[u32] {
        self.state().dataset_indexes()
    }

    pub fn deletion_flags(&self) -> &[bool] {
        self.state().deletion_flags()
    }

    pub fn undeleted_dataset_indexes(&self) -> Vec<u32> {
        self.state().undeleted_dataset_indexes()
    }

    pub fn logical_entries(&self) -> Vec<&str> {
        self.state().logical_entries()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PartitionState {
    entry_keys: Vec<String>,
    dataset_indexes: Vec<u32>,
    deletion_flags: Vec<bool>,
}

impl PartitionState {
    pub(crate) fn empty() -> Self {
        Self {
            entry_keys: vec![],
            dataset_indexes: vec![],
            deletion_flags: vec![],
        }
    }

    pub(crate) fn entry_keys(&self) -> &[String] {
        &self.entry_keys
    }

    pub(crate) fn dataset_indexes(&self) -> &[u32] {
        &self.dataset_indexes
    }

    pub(crate) fn deletion_flags(&self) -> &[bool] {
        &self.deletion_flags
    }

    pub(crate) fn logical_entries(&self) -> Vec<&str> {
        self.entry_keys
            .iter()
            .zip(self.deletion_flags.iter())
            .filter_map(|(entry, deleted)| (!deleted).then_some(entry.as_str()))
            .collect()
    }

    pub(crate) fn undeleted_dataset_indexes(&self) -> Vec<u32> {
        self.dataset_indexes
            .iter()
            .zip(self.deletion_flags.iter())
            .filter_map(|(dataset_index, deleted)| (!deleted).then_some(*dataset_index))
            .collect()
    }
}

pub async fn load_partition<S: object_store::ObjectStore + Clone>(
    object_store: S,
    partition_directory: object_store::path::Path,
    io_cache: Arc<IoCache>,
) -> anyhow::Result<Partition<S>> {
    let metadata_path = partition_directory.child(PARTITION_METADATA_FILE);
    let metadata_bytes = object_store.get(&metadata_path).await?.bytes().await?;
    let metadata: PartitionMetadata = serde_json::from_slice(&metadata_bytes)?;
    let state = load_partition_state(
        object_store.clone(),
        partition_directory.clone(),
        metadata.name.clone(),
    )
    .await?;

    Ok(Partition::new(
        object_store,
        metadata.name.clone(),
        partition_directory,
        metadata,
        state,
        io_cache,
    ))
}

pub(crate) async fn load_partition_state<S: object_store::ObjectStore + Clone>(
    object_store: S,
    partition_directory: object_store::path::Path,
    partition_name: impl Into<String>,
) -> anyhow::Result<PartitionState> {
    let partition_name = partition_name.into();
    let entries_batch = load_partition_entries(&object_store, &partition_directory).await?;

    let entry_keys = entries_batch.column(0).as_string::<i32>();
    let dataset_indexes = entries_batch.column(1).as_primitive::<UInt32Type>();
    let deletion_flags = entries_batch.column(2).as_boolean();

    let entry_keys = entry_keys
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            entry.map(ToString::to_string).ok_or_else(|| {
                anyhow::anyhow!(
                    "null entry key at dataset index {index} in partition {}",
                    partition_name
                )
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let dataset_indexes = dataset_indexes.values().to_vec();
    let deletion_flags = deletion_flags
        .iter()
        .map(|flag| flag.unwrap_or(false))
        .collect::<Vec<_>>();

    anyhow::ensure!(
        entry_keys.len() == dataset_indexes.len() && entry_keys.len() == deletion_flags.len(),
        "entries file is inconsistent for partition {}",
        partition_name
    );

    Ok(PartitionState {
        entry_keys,
        dataset_indexes,
        deletion_flags,
    })
}

pub(crate) fn column_name_to_path(
    partition_directory: object_store::path::Path,
    column_name: &str,
) -> object_store::path::Path {
    if column_name.starts_with('.') {
        let reserved_name = column_name.trim_start_matches('.');
        partition_directory
            .child("columns")
            .child(format!("__{}", reserved_name))
    } else if column_name.contains('.') {
        let parts: Vec<&str> = column_name.split('.').collect();
        let mut path = partition_directory.child("columns");
        for part in parts {
            path = path.child(part);
        }
        path
    } else {
        partition_directory.child("columns").child(column_name)
    }
}
