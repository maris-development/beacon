use std::sync::Arc;

use datafusion::{catalog::TableProvider, prelude::SessionContext};
use geoarrow::table;
use object_store::{ObjectStore, PutPayload};

use crate::table::{_type::TableType, error::TableError};

pub mod _type;
pub mod error;
pub mod logical;
pub mod preset;
pub mod table_formats;

/// Represents a table configuration along with its associated provider.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Table {
    #[serde(skip)]
    pub object_path: object_store::path::Path,
    /// The name of the table.
    pub table_name: String,
    /// The type of the table which determines its behavior.
    pub table_type: TableType,
    #[serde(default)]
    pub description: Option<String>,
}

impl Table {
    pub async fn new(
        table_directory: object_store::path::Path,
        object_store: Arc<dyn ObjectStore>,
        table_name: String,
        table_type: impl Into<TableType>,
        description: Option<String>,
    ) -> Self {
        let table = Self {
            object_path: table_directory.clone(),
            table_name,
            description,
            table_type: table_type.into(),
        };
        //  Write self as json to 'table.json'
        let json = serde_json::to_string(&table).unwrap();
        let payload = PutPayload::from_bytes(json.into_bytes().into());
        object_store
            .put(&table_directory.child("table.json"), payload)
            .await
            .unwrap();

        table
    }

    pub async fn open(
        store: Arc<dyn ObjectStore>,
        table_directory: object_store::path::Path,
    ) -> Result<Self, TableError> {
        // Read the table config
        let config_json_path = table_directory.child("table.json");
        let json_bytes = store
            .get(&config_json_path)
            .await
            .map_err(TableError::FailedToReadTableConfig)?
            .bytes()
            .await
            .map_err(TableError::FailedToReadTableConfig)?;

        let mut table: Table =
            serde_json::from_slice(&json_bytes).map_err(TableError::InvalidTableConfig)?;
        table.object_path = table_directory;

        Ok(table)
    }

    pub async fn table_provider(
        &self,
        session_ctx: Arc<SessionContext>,
        object_store: Arc<dyn ObjectStore>,
        data_directory: object_store::path::Path,
        table_directory: object_store::path::Path,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        self.table_type
            .table_provider(session_ctx, object_store, table_directory, data_directory)
            .await
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}
