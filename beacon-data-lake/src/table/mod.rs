use std::sync::Arc;

use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use futures::StreamExt;
use object_store::{ObjectStore, PutPayload, path::Path};

use crate::table::{_type::TableType, error::TableError};

pub mod _type;
pub mod empty;
pub mod error;
pub mod geospatial;
pub mod logical;
pub mod merged;
pub mod preset;
pub mod table_formats;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum TableFormat {
    #[serde(untagged)]
    Legacy(Table),
    #[serde(untagged)]
    DefinitionBased(Arc<dyn TableDefinition>),
}

impl TableFormat {
    pub async fn try_open(
        store: Arc<dyn ObjectStore>,
        table_json: object_store::path::Path,
    ) -> anyhow::Result<Self> {
        // Read the table config
        let json_bytes = store
            .get(&table_json)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read table config: {:?}", e))?
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read table config bytes: {:?}", e))?;

        // Try parse as legacy table format first
        if let Ok(table) = serde_json::from_slice::<Table>(&json_bytes) {
            return Ok(Self::Legacy(table));
        } // If that fails, try parse as definition-based format
        let definition =
            serde_json::from_slice::<Arc<dyn TableDefinition>>(&json_bytes).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse table config as definition-based format: {:?}",
                    e
                )
            })?;

        Ok(Self::DefinitionBased(definition))
    }

    pub fn table_name(&self) -> &str {
        match self {
            Self::Legacy(table) => table.table_name(),
            Self::DefinitionBased(definition) => definition.table_name(),
        }
    }
}

/// Represents a table configuration along with its associated provider.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Table {
    #[serde(skip)]
    pub table_directory: Vec<object_store::path::PathPart<'static>>,
    /// The name of the table.
    pub table_name: String,
    /// The type of the table which determines its behavior.
    pub table_type: TableType,
    #[serde(default)]
    pub description: Option<String>,
}

impl Table {
    pub async fn save(
        &mut self,
        table_object_store: Arc<dyn ObjectStore>,
        mut table_directory: Vec<object_store::path::PathPart<'static>>,
    ) {
        // Create table.json in the specified directory
        table_directory.push("table.json".into());
        let table_directory_path: object_store::path::Path =
            Path::from_iter(table_directory.clone());

        //  Write self as json to 'table.json'
        let json = serde_json::to_string_pretty(&self).unwrap();
        let payload = PutPayload::from_bytes(json.into_bytes().into());
        table_object_store
            .put(&table_directory_path, payload)
            .await
            .unwrap();

        self.table_directory = table_directory;
    }

    pub async fn table_provider(
        &self,
        session_ctx: Arc<SessionContext>,
        data_directory_store_url: ObjectStoreUrl,
        table_directory_store_url: ObjectStoreUrl,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        self.table_type
            .table_provider(
                session_ctx,
                table_directory_store_url,
                data_directory_store_url,
            )
            .await
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub async fn delete_table(
        &self,
        session_ctx: Arc<SessionContext>,
        table_directory_store_url: ObjectStoreUrl,
    ) {
        let full_path = Path::from(self.table_name.clone());
        // Delete the table directory
        let object_store = session_ctx
            .runtime_env()
            .object_store(&table_directory_store_url)
            .unwrap();
        let mut table_files = object_store.list(Some(&full_path));
        while let Some(res) = table_files.next().await {
            match res {
                Ok(object_meta) => {
                    let path = object_meta.location.clone();
                    if let Err(e) = object_store.delete(&path).await {
                        tracing::error!("Failed to delete table file {:?}: {:?}", path, e);
                    }
                    tracing::debug!("Deleted table file: {:?}", path);
                }
                Err(e) => {
                    tracing::error!("Failed to list table files for deletion: {:?}", e);
                }
            }
        }
        tracing::info!("Deleted table directory: {:?}", full_path);
    }
}
