use std::sync::Arc;

use object_store::ObjectStore;

use crate::table::{self, _type::TableType, error::TableError};

pub mod _type;
pub mod error;

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
    pub fn new(
        path: object_store::path::Path,
        table_name: String,
        table_type: impl Into<TableType>,
        description: Option<String>,
    ) -> Self {
        Self {
            object_path: path,
            table_name,
            description,
            table_type: table_type.into(),
        }
    }

    pub async fn open(
        store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
    ) -> Result<Self, TableError> {
        // Read the table config
        let config_json_path = path.child("table.json");
        let json_bytes = store
            .get(&config_json_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let mut table: Table =
            serde_json::from_slice(&json_bytes).map_err(TableError::InvalidTableConfig)?;
        table.object_path = path;

        Ok(table)
    }
}
