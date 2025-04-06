use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use datafusion::{
    catalog::TableProvider,
    execution::{session_state, SessionState},
    prelude::SessionContext,
};

use crate::{error::TableError, LogicalTableProvider};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Table {
    pub table_name: String,
    pub table_type: TableType,
}

impl From<Arc<dyn LogicalTableProvider>> for Table {
    fn from(logical_table: Arc<dyn LogicalTableProvider>) -> Self {
        Table {
            table_name: logical_table.table_name().to_string(),
            table_type: TableType::Logical(logical_table),
        }
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TableType {
    Logical(Arc<dyn LogicalTableProvider>),
}

impl Table {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, TableError> {
        // Ensure the directory exists
        if !dir.as_ref().exists() {
            tracing::error!("Failed to open table: {}", dir.as_ref().display());
            return Err(TableError::TableDoesNotExist(
                dir.as_ref().to_string_lossy().to_string(),
            ));
        }

        // Read the table configuration file
        let table_config_path = dir.as_ref().join("table.json");
        let table_config_file = std::fs::File::open(&table_config_path).map_err(|e| {
            TableError::TableConfigFileError(e, dir.as_ref().to_string_lossy().to_string())
        })?;

        let table: Table = serde_json::from_reader(table_config_file).map_err(|e| {
            TableError::TableConfigFileParseError(e, dir.as_ref().to_string_lossy().to_string())
        })?;
        tracing::debug!("Opened table: {}", table.table_name);

        Ok(table)
    }

    pub async fn create(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), TableError> {
        //Ensure the directory exists
        if !table_directory.exists() {
            //Create the directory
            return Err(TableError::BaseTableDirectoryDoesNotExist);
        }

        //Create the table directory
        let directory = table_directory.join(&self.table_name);
        if !directory.exists() {
            tokio::fs::create_dir_all(&directory).await.map_err(|e| {
                TableError::FailedToCreateTableDirectory(e, directory.to_string_lossy().to_string())
            })?;
        }

        //Write the table (self) as a json file to the directory
        let table_config_path = directory.join("table.json");
        std::fs::File::create(&table_config_path)
            .map_err(|e| TableError::TableConfigWriteError(e))?;

        // Serialize the table to JSON and write it to the file
        let table_json = serde_json::to_string_pretty(self)
            .map_err(|e| TableError::TableConfigSerializationError(e))?;

        std::fs::write(&table_config_path, table_json)
            .map_err(|e| TableError::TableConfigWriteError(e))?;

        match &self.table_type {
            TableType::Logical(logical_table_provider) => Ok(logical_table_provider
                .create(directory, session_ctx)
                .await?),
        }
    }

    pub async fn table_provider(
        &self,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        match &self.table_type {
            TableType::Logical(logical_table) => {
                Ok(logical_table.table_provider(session_ctx).await?)
            }
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}
