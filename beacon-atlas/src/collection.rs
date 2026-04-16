use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::Cache;
use crate::consts::{COLLECTION_METADATA_FILE, PARTITION_METADATA_FILE};
use crate::partition::Partition;
use crate::partition::ops::write::PartitionWriter;
use crate::partition::state::try_load_partition_state;
use crate::schema::{AtlasSchema, AtlasSuperTypingMode};
use futures::StreamExt;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionMetadata {
    pub name: String,
    pub description: Option<String>,
    #[serde(default = "default_collection_version")]
    pub version: String,
    #[serde(default = "default_endianness")]
    pub endianness: String,
    #[serde(default = "default_super_typing_mode")]
    pub super_typing_mode: AtlasSuperTypingMode,
    #[serde(default)]
    pub partitions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AtlasCollectionState {
    metadata: CollectionMetadata,
    partition_schemas: HashMap<String, AtlasSchema>,
}

impl AtlasCollectionState {
    pub fn metadata(&self) -> &CollectionMetadata {
        &self.metadata
    }

    pub fn partition_schemas(&self) -> impl Iterator<Item = &AtlasSchema> {
        self.partition_schemas.values()
    }

    pub fn partition_schema_map(&self) -> &HashMap<String, AtlasSchema> {
        &self.partition_schemas
    }
}

#[derive(Debug, Clone)]
pub struct AtlasCollection<
    S: object_store::ObjectStore + Clone = Arc<dyn object_store::ObjectStore>,
> {
    object_store: S,
    collection_directory: object_store::path::Path,
    state: Option<AtlasCollectionState>,
    cache: Arc<Cache>,
}

/// Scoped partition writer tied to a mutable collection reference.
///
/// Call `finish()` to persist the partition and refresh collection state.
/// If dropped before `finish()`, collection state is marked stale and must be
/// reloaded before accessing snapshot-derived data.
pub struct CollectionPartitionWriter<'a, S: object_store::ObjectStore + Clone> {
    collection: &'a mut AtlasCollection<S>,
    partition_name: String,
    writer: Option<PartitionWriter<S>>,
    finished: bool,
}

impl<'a, S: object_store::ObjectStore + Clone> CollectionPartitionWriter<'a, S> {
    fn new(
        collection: &'a mut AtlasCollection<S>,
        partition_name: String,
        writer: PartitionWriter<S>,
    ) -> Self {
        Self {
            collection,
            partition_name,
            writer: Some(writer),
            finished: false,
        }
    }

    pub fn writer_mut(&mut self) -> anyhow::Result<&mut PartitionWriter<S>> {
        self.writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("partition writer is no longer available"))
    }

    pub async fn finish(mut self) -> anyhow::Result<crate::partition::Partition<S>> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| anyhow::anyhow!("partition writer is no longer available"))?;
        let partition = writer.finish(self.collection.cache.clone()).await?;

        let state = self
            .collection
            .state
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("collection state is not loaded"))?;
        if !state
            .metadata
            .partitions
            .iter()
            .any(|name| name == &self.partition_name)
        {
            state.metadata.partitions.push(self.partition_name.clone());
            save_collection_metadata(
                self.collection.object_store.clone(),
                self.collection.collection_directory.clone(),
                &state.metadata,
            )
            .await?;
        }

        self.collection.load().await?;
        self.finished = true;
        Ok(partition)
    }
}

impl<'a, S: object_store::ObjectStore + Clone> Drop for CollectionPartitionWriter<'a, S> {
    fn drop(&mut self) {
        if !self.finished {
            // Drop cannot be async; mark state stale and let callers reload explicitly.
            self.collection.state = None;
        }
    }
}

impl<S: object_store::ObjectStore + Clone> AtlasCollection<S> {
    pub async fn create(
        object_store: S,
        collection_directory: object_store::path::Path,
        name: impl Into<String>,
        description: Option<String>,
        super_typing_mode: AtlasSuperTypingMode,
    ) -> anyhow::Result<Self> {
        let metadata = CollectionMetadata {
            name: name.into(),
            description,
            version: default_collection_version(),
            endianness: default_endianness(),
            super_typing_mode,
            partitions: Vec::new(),
        };

        save_collection_metadata(
            object_store.clone(),
            collection_directory.clone(),
            &metadata,
        )
        .await?;

        let mut collection = Self::new(object_store, collection_directory);

        collection.state = Some(AtlasCollectionState {
            metadata,
            partition_schemas: HashMap::new(),
        });

        Ok(collection)
    }

    pub fn new(object_store: S, collection_directory: object_store::path::Path) -> Self {
        Self {
            object_store,
            collection_directory,
            state: None,
            cache: Arc::new(Cache::new(256, 256)),
        }
    }

    pub async fn open(
        object_store: S,
        collection_directory: object_store::path::Path,
    ) -> anyhow::Result<Self> {
        let mut collection = Self::new(object_store, collection_directory);
        collection.load().await?;
        Ok(collection)
    }

    pub fn object_store(&self) -> &S {
        &self.object_store
    }

    pub fn super_typing_mode(&self) -> AtlasSuperTypingMode {
        self.state
            .as_ref()
            .map(|state| state.metadata.super_typing_mode)
            .unwrap_or_default()
    }

    pub fn collection_path(&self) -> &object_store::path::Path {
        &self.collection_directory
    }

    pub fn cache_ref(&self) -> Arc<Cache> {
        self.cache.clone()
    }

    pub async fn load(&mut self) -> anyhow::Result<()> {
        let state = load_collection_state(
            self.object_store.clone(),
            self.collection_directory.clone(),
            self.cache.clone(),
        )
        .await?;

        self.state = Some(state);
        Ok(())
    }

    pub fn collection_schema(&self) -> anyhow::Result<AtlasSchema> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("collection state is not loaded"))?;

        if state.partition_schemas.is_empty() {
            return Ok(AtlasSchema::empty());
        }

        let schemas: Vec<_> = state.partition_schemas.values().cloned().collect();
        AtlasSchema::merge_all_with_mode(&schemas, state.metadata.super_typing_mode)
    }

    pub async fn update_state(&mut self) -> anyhow::Result<()> {
        self.load().await
    }

    pub async fn create_partition_writer(
        &mut self,
        name: impl Into<String>,
        description: Option<&str>,
    ) -> anyhow::Result<PartitionWriter<S>> {
        if self.state.is_none() {
            self.load().await?;
        }

        let name = name.into();
        anyhow::ensure!(!name.is_empty(), "partition name cannot be empty");

        let state = self
            .state
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("collection state is not loaded"))?;

        anyhow::ensure!(
            !state
                .metadata
                .partitions
                .iter()
                .any(|partition| partition == &name),
            "partition '{}' already exists",
            name
        );

        let partition_directory = self
            .collection_directory
            .child("partitions")
            .child(name.clone());
        let writer = PartitionWriter::new(
            self.object_store.clone(),
            partition_directory.clone(),
            &name,
            description,
        )?;

        Ok(writer)
    }

    pub async fn create_partition(
        &mut self,
        name: impl Into<String>,
        description: Option<&str>,
    ) -> anyhow::Result<CollectionPartitionWriter<'_, S>> {
        let name = name.into();
        let writer = self
            .create_partition_writer(name.clone(), description)
            .await?;
        Ok(CollectionPartitionWriter::new(self, name, writer))
    }

    pub fn snapshot(&self) -> anyhow::Result<&AtlasCollectionState> {
        self.state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("collection state is not loaded"))
    }

    /// Load and return a partition handle by name.
    ///
    /// This validates that the partition exists in collection metadata and then
    /// returns a fully loaded [`crate::partition::Partition`] handle that can be
    /// explored (schema, dataset indexes, deletion flags, entry keys, etc.).
    pub async fn get_partition(
        &mut self,
        partition_name: &str,
    ) -> anyhow::Result<crate::partition::Partition<S>> {
        if self.state.is_none() {
            self.load().await?;
        }

        let snapshot = self.snapshot()?;
        anyhow::ensure!(
            snapshot
                .metadata()
                .partitions
                .iter()
                .any(|name| name == partition_name),
            "partition '{}' does not exist in collection '{}'",
            partition_name,
            snapshot.metadata().name
        );

        let partition_directory = self
            .collection_directory
            .clone()
            .child("partitions")
            .child(partition_name);

        Partition::try_open(
            self.object_store.clone(),
            partition_name.to_string(),
            partition_directory,
            self.cache.clone(),
        )
    }

    /// Remove a partition from collection metadata and delete its persisted files.
    pub async fn remove_partition(&mut self, partition_name: &str) -> anyhow::Result<()> {
        if self.state.is_none() {
            self.load().await?;
        }

        let state = self
            .state
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("collection state is not loaded"))?;

        if !state
            .metadata
            .partitions
            .iter()
            .any(|name| name == partition_name)
        {
            return Ok(());
        }

        let partition_directory = self
            .collection_directory
            .clone()
            .child("partitions")
            .child(partition_name);

        let mut objects = self.object_store.list(Some(&partition_directory));
        while let Some(next_object) = objects.next().await {
            let object_meta = next_object?;
            if let Err(error) = self.object_store.delete(&object_meta.location).await
                && !matches!(error, object_store::Error::NotFound { .. })
            {
                return Err(error.into());
            }
        }

        state
            .metadata
            .partitions
            .retain(|name| name != partition_name);

        save_collection_metadata(
            self.object_store.clone(),
            self.collection_directory.clone(),
            &state.metadata,
        )
        .await?;

        self.load().await?;
        Ok(())
    }
}

fn default_super_typing_mode() -> AtlasSuperTypingMode {
    AtlasSuperTypingMode::General
}

fn default_collection_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn default_endianness() -> String {
    if cfg!(target_endian = "little") {
        "little".to_string()
    } else {
        "big".to_string()
    }
}

pub(crate) async fn load_collection_state<S: object_store::ObjectStore + Clone>(
    object_store: S,
    collection_directory: object_store::path::Path,
    cache: Arc<Cache>,
) -> anyhow::Result<AtlasCollectionState> {
    let metadata_path = collection_directory.child(COLLECTION_METADATA_FILE);
    let metadata_bytes = object_store.get(&metadata_path).await?.bytes().await?;
    let metadata: CollectionMetadata = serde_json::from_slice(&metadata_bytes)?;
    load_collection_state_with_metadata(object_store, collection_directory, cache, metadata).await
}

async fn load_collection_state_with_metadata<S: object_store::ObjectStore + Clone>(
    object_store: S,
    collection_directory: object_store::path::Path,
    _cache: Arc<Cache>,
    metadata: CollectionMetadata,
) -> anyhow::Result<AtlasCollectionState> {
    let mut partition_schemas = HashMap::with_capacity(metadata.partitions.len());

    for partition_name in &metadata.partitions {
        let partition_metadata_path = collection_directory
            .clone()
            .child("partitions")
            .child(partition_name.clone())
            .child(PARTITION_METADATA_FILE);

        match try_load_partition_state(&object_store, &partition_metadata_path).await {
            Ok(partition_state) => {
                partition_schemas.insert(partition_name.clone(), partition_state.schema());
            }
            Err(error) if is_object_not_found_error(&error) => {
                // Partition metadata can be temporarily absent when a writer was dropped before finish.
                continue;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(AtlasCollectionState {
        metadata,
        partition_schemas,
    })
}

fn is_object_not_found_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<object_store::Error>()
            .is_some_and(|object_store_error| {
                matches!(object_store_error, object_store::Error::NotFound { .. })
            })
    })
}

async fn save_collection_metadata<S: object_store::ObjectStore + Clone>(
    object_store: S,
    collection_directory: object_store::path::Path,
    metadata: &CollectionMetadata,
) -> anyhow::Result<()> {
    let metadata_path = collection_directory.child(COLLECTION_METADATA_FILE);
    object_store
        .put(&metadata_path, serde_json::to_vec(metadata)?.into())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;

    use crate::{column::Column, schema::AtlasSuperTypingMode};

    use super::{AtlasCollection, default_endianness};

    #[tokio::test]
    async fn create_collection_writes_metadata_and_loads_state() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let collection_path = Path::from("collections/example");

        let collection = AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "example",
            Some("demo collection".to_string()),
            AtlasSuperTypingMode::General,
        )
        .await?;

        let snapshot = collection.snapshot()?;
        assert_eq!(snapshot.metadata().name, "example");
        assert_eq!(
            snapshot.metadata().description.as_deref(),
            Some("demo collection")
        );
        assert_eq!(
            snapshot.metadata().super_typing_mode,
            AtlasSuperTypingMode::General
        );
        assert_eq!(snapshot.metadata().version, env!("CARGO_PKG_VERSION"));
        assert_eq!(snapshot.metadata().endianness, default_endianness());
        assert!(snapshot.metadata().partitions.is_empty());

        let opened = AtlasCollection::open(store, collection_path).await?;
        assert_eq!(opened.snapshot()?.metadata().name, "example");
        assert_eq!(opened.super_typing_mode(), AtlasSuperTypingMode::General);
        assert_eq!(
            opened.snapshot()?.metadata().version,
            env!("CARGO_PKG_VERSION")
        );
        assert_eq!(
            opened.snapshot()?.metadata().endianness,
            default_endianness()
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_partition_updates_collection_metadata() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let collection_path = Path::from("collections/example");

        let mut collection = AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "example",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        let writer = collection
            .create_partition("part-00000", Some("first partition"))
            .await?;
        let partition = writer.finish().await?;

        assert_eq!(partition.name(), "part-00000");
        assert_eq!(
            partition.state().partition_description().as_deref(),
            Some("first partition")
        );
        assert_eq!(
            collection.snapshot()?.metadata().partitions,
            vec!["part-00000".to_string()]
        );

        let reopened = AtlasCollection::open(store, collection_path).await?;
        assert_eq!(
            reopened.snapshot()?.metadata().partitions,
            vec!["part-00000".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_partition_finishes_and_reloads_collection_state() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let collection_path = Path::from("collections/example");

        let mut collection = AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "example",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        let mut scoped_writer = collection
            .create_partition("part-00001", Some("scoped writer"))
            .await?;

        scoped_writer
            .writer_mut()?
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10_i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;

        let partition = scoped_writer.finish().await?;

        assert_eq!(partition.name(), "part-00001");
        assert_eq!(
            collection.snapshot()?.metadata().partitions,
            vec!["part-00001".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn dropping_create_partition_marks_collection_state_stale() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let collection_path = Path::from("collections/example");

        let mut collection = AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "example",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        {
            let _scoped_writer = collection
                .create_partition("part-00002", Some("not finalized"))
                .await?;
        }

        assert!(collection.snapshot().is_err());

        collection.load().await?;
        assert!(collection.snapshot()?.metadata().partitions.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn remove_partition_updates_collection_metadata() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let collection_path = Path::from("collections/remove-partition");

        let mut collection = AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "example",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        let writer = collection
            .create_partition("part-00003", Some("to delete"))
            .await?;
        writer.finish().await?;

        collection.remove_partition("part-00003").await?;
        assert!(collection.snapshot()?.metadata().partitions.is_empty());

        let reopened = AtlasCollection::open(store, collection_path).await?;
        assert!(reopened.snapshot()?.metadata().partitions.is_empty());

        Ok(())
    }
}
