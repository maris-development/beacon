use std::sync::Arc;

use crate::consts::COLLECTION_METADATA_FILE;
use crate::schema::AtlasSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionMetadata {
    pub name: String,
    pub description: Option<String>,
    pub schema: AtlasSchema,
    #[serde(default)]
    pub partitions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AtlasCollectionState {
    metadata: CollectionMetadata,
}

impl AtlasCollectionState {
    pub fn metadata(&self) -> &CollectionMetadata {
        &self.metadata
    }

    pub(crate) fn partitions(&self) -> &[String] {
        &self.metadata.partitions
    }

    pub(crate) fn resolve_partition_name(
        &self,
        requested_partition_name: Option<&str>,
        operation_name: &str,
    ) -> anyhow::Result<String> {
        if let Some(partition_name) = requested_partition_name {
            anyhow::ensure!(
                self.metadata
                    .partitions
                    .iter()
                    .any(|name| name == partition_name),
                "partition '{partition_name}' not found for {operation_name} operation"
            );
            return Ok(partition_name.to_string());
        }

        match self.metadata.partitions.as_slice() {
            [partition_name] => Ok(partition_name.clone()),
            [] => anyhow::bail!("collection has no partitions available for {operation_name}"),
            _ => anyhow::bail!(
                "partition name is required for {operation_name} because the collection has multiple partitions"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AtlasCollection<S: object_store::ObjectStore + Clone> {
    object_store: S,
    collection_directory: object_store::path::Path,
    state: Option<AtlasCollectionState>,
}

impl<S: object_store::ObjectStore + Clone> AtlasCollection<S> {
    pub fn new(object_store: S, collection_directory: object_store::path::Path) -> Self {
        Self {
            object_store,
            collection_directory,
            state: None,
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

    pub fn collection_path(&self) -> &object_store::path::Path {
        &self.collection_directory
    }

    pub async fn load(&mut self) -> anyhow::Result<()> {
        self.state = Some(
            load_collection_state(self.object_store.clone(), self.collection_directory.clone())
                .await?,
        );
        Ok(())
    }

    pub fn arrow_schema(&self) -> anyhow::Result<Arc<arrow::datatypes::Schema>> {
        let schema = self
            .state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("collection state is not loaded"))?
            .metadata()
            .schema
            .to_arrow_schema();

        Ok(Arc::new(schema))
    }

    pub async fn update_state(&mut self) -> anyhow::Result<()> {
        self.load().await
    }

    pub fn snapshot(&self) -> anyhow::Result<&AtlasCollectionState> {
        self.state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("collection state is not loaded"))
    }

    pub(crate) fn with_state(mut self, state: AtlasCollectionState) -> Self {
        self.state = Some(state);
        self
    }

    pub(crate) fn into_parts(self) -> (S, object_store::path::Path, Option<AtlasCollectionState>) {
        (self.object_store, self.collection_directory, self.state)
    }
}

pub(crate) async fn load_collection_state<S: object_store::ObjectStore + Clone>(
    object_store: S,
    collection_directory: object_store::path::Path,
) -> anyhow::Result<AtlasCollectionState> {
    let metadata_path = collection_directory.child(COLLECTION_METADATA_FILE);
    let metadata_bytes = object_store.get(&metadata_path).await?.bytes().await?;
    let metadata: CollectionMetadata = serde_json::from_slice(&metadata_bytes)?;
    load_collection_state_with_metadata(object_store, collection_directory, metadata).await
}

async fn load_collection_state_with_metadata<S: object_store::ObjectStore + Clone>(
    _object_store: S,
    _collection_directory: object_store::path::Path,
    metadata: CollectionMetadata,
) -> anyhow::Result<AtlasCollectionState> {
    Ok(AtlasCollectionState { metadata })
}
