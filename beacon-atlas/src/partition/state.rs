use chrono::{DateTime, Utc};
use object_store::ObjectStore;

use crate::{
    partition::entries::PartitionEntry,
    schema::{_type::AtlasDataType, AtlasColumn, AtlasSchema},
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionState {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) schema: AtlasSchema,
    pub(crate) entries: Vec<PartitionEntry>,
    pub(crate) creation_date: DateTime<Utc>,
    pub(crate) version: semver::Version,
}

impl PartitionState {
    pub(crate) fn new(name: impl Into<String>, description: Option<impl Into<String>>) -> Self {
        const VERSION_STR: &str = env!("CARGO_PKG_VERSION");
        let semver =
            semver::Version::parse(VERSION_STR).expect("Failed to parse cargo version as semver.");
        Self {
            creation_date: chrono::Utc::now(),
            description: description.map(|d| d.into()),
            name: name.into(),
            version: semver,
            entries: vec![],
            schema: AtlasSchema::new(vec![]),
        }
    }

    pub(crate) fn add_column(
        &mut self,
        name: impl Into<String>,
        datatype: AtlasDataType,
    ) -> anyhow::Result<()> {
        let name = name.into();
        if self.schema.columns.iter().any(|c| &c.name == &name) {
            anyhow::bail!("Column: {} already exists in the partition schema.", name)
        } else {
            self.schema.columns.push(AtlasColumn::new(name, datatype));
            Ok(())
        }
    }

    pub(crate) fn partition_name(&self) -> String {
        self.name.clone()
    }

    pub(crate) fn partition_description(&self) -> Option<String> {
        self.description.clone()
    }

    pub(crate) fn schema(&self) -> AtlasSchema {
        self.schema.clone()
    }

    /// Keeps only the non-deleted entries.
    /// Physical entries show all, even non-deleted entries as the are
    /// physically still there (unless vaccuumed)
    pub(crate) fn entries(&self) -> Vec<PartitionEntry> {
        // Zip into non deleted partitions
        self.entries.iter().filter(|e| e.deleted).cloned().collect()
    }

    pub(crate) fn physical_entries(&self) -> &[PartitionEntry] {
        &self.entries
    }

    pub(crate) fn partition_creation_date(&self) -> DateTime<Utc> {
        self.creation_date
    }

    pub(crate) fn semver(&self) -> semver::Version {
        self.version.clone()
    }
}

pub async fn try_write_partition_state<S: ObjectStore + ?Sized>(
    store: &S,
    path: &object_store::path::Path,
    state: &PartitionState,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(state)?;
    store.put(path, bytes.into()).await?;
    Ok(())
}

pub async fn try_load_partition_state<S: ObjectStore + ?Sized>(
    store: &S,
    path: &object_store::path::Path,
) -> anyhow::Result<PartitionState> {
    let bytes = store.get(path).await?.bytes().await?;
    let state: PartitionState = serde_json::from_slice(&bytes)?;
    Ok(state)
}
