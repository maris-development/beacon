use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use object_store::{
    ObjectStore, PutPayload,
    path::{Path, PathPart},
};

use crate::table::{_type::TableType, error::TableError};

pub mod _type;
pub mod empty;
pub mod error;
pub mod geospatial;
pub mod logical;
pub mod preset;
pub mod table_formats;

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

    pub async fn open(
        store: Arc<dyn ObjectStore>,
        mut table_directory: Vec<PathPart<'static>>,
    ) -> Result<Self, TableError> {
        table_directory.push("table.json".into());
        let table_directory_path: object_store::path::Path =
            Path::from_iter(table_directory.clone());

        // Read the table config
        let json_bytes = store
            .get(&table_directory_path)
            .await
            .map_err(TableError::FailedToReadTableConfig)?
            .bytes()
            .await
            .map_err(TableError::FailedToReadTableConfig)?;

        let mut table: Table =
            serde_json::from_slice(&json_bytes).map_err(TableError::InvalidTableConfig)?;
        table.table_directory = table_directory;

        Ok(table)
    }

    pub async fn table_provider(
        &self,
        session_ctx: Arc<SessionContext>,
        data_directory_store_url: ObjectStoreUrl,
        data_directory_prefix: object_store::path::Path,
        table_directory_store_url: ObjectStoreUrl,
        table_directory_prefix: object_store::path::Path,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        self.table_type
            .table_provider(
                session_ctx,
                table_directory_store_url,
                table_directory_prefix,
                data_directory_store_url,
                data_directory_prefix,
            )
            .await
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}
