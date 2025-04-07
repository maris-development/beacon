use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use datafusion::{catalog::TableProvider, prelude::SessionContext};

use crate::{error::TableError, LogicalTableProvider};

/// Represents a table configuration along with its associated provider.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Table {
    /// The name of the table.
    pub table_name: String,
    /// The type of the table which determines its behavior.
    pub table_type: TableType,
}

impl From<Arc<dyn LogicalTableProvider>> for Table {
    /// Creates a `Table` from a logical table provider.
    ///
    /// # Arguments
    ///
    /// * `logical_table` - An `Arc` to a logical table provider.
    ///
    /// # Returns
    ///
    /// A new instance of `Table` with the provided logical table provider.
    fn from(logical_table: Arc<dyn LogicalTableProvider>) -> Self {
        Table {
            table_name: logical_table.table_name().to_string(),
            table_type: TableType::Logical(logical_table),
        }
    }
}

/// Enum representing different types of tables.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TableType {
    /// A logical table with its associated provider.
    Logical(Arc<dyn LogicalTableProvider>),
}

impl Table {
    /// Opens an existing table configuration from the specified directory.
    ///
    /// This function checks whether the provided directory exists and attempts to read and deserialize
    /// the table configuration stored in a `table.json` file.
    ///
    /// # Arguments
    ///
    /// * `dir` - A path-like reference to the directory containing the table configuration.
    ///
    /// # Errors
    ///
    /// Returns a `TableError` if the directory does not exist, or if there is an issue reading or parsing
    /// the configuration file.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, TableError> {
        // Ensure the directory exists.
        if !dir.as_ref().exists() {
            tracing::error!("Failed to open table: {}", dir.as_ref().display());
            return Err(TableError::TableDoesNotExist(
                dir.as_ref().to_string_lossy().to_string(),
            ));
        }

        // Construct the path for the table configuration file.
        let table_config_path = dir.as_ref().join("table.json");
        let table_config_file = std::fs::File::open(&table_config_path).map_err(|e| {
            TableError::TableConfigFileError(e, dir.as_ref().to_string_lossy().to_string())
        })?;

        // Deserialize the table from the JSON configuration file.
        let table: Table = serde_json::from_reader(table_config_file).map_err(|e| {
            TableError::TableConfigFileParseError(e, dir.as_ref().to_string_lossy().to_string())
        })?;
        tracing::debug!("Opened table: {}", table.table_name);

        Ok(table)
    }

    /// Creates a new table by writing its configuration to disk and delegating further initialization
    /// to its logical table provider.
    ///
    /// This function ensures that the base directory exists, creates a specific directory for the table,
    /// writes the table configuration to a JSON file, and then calls the logical table provider's create
    /// method to initialize any additional required resources.
    ///
    /// # Arguments
    ///
    /// * `table_directory` - The base directory where the table should be created.
    /// * `session_ctx` - A shared session context used for DataFusion operations.
    ///
    /// # Errors
    ///
    /// Returns a `TableError` if the base directory does not exist, or if any file system or serialization
    /// operation fails.
    pub async fn create(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), TableError> {
        // Ensure the base table directory exists.
        if !table_directory.exists() {
            return Err(TableError::BaseTableDirectoryDoesNotExist);
        }

        // Create the specific table directory.
        let directory = table_directory.join(&self.table_name);
        if !directory.exists() {
            tokio::fs::create_dir_all(&directory).await.map_err(|e| {
                TableError::FailedToCreateTableDirectory(e, directory.to_string_lossy().to_string())
            })?;
        }

        // Write the table configuration as a JSON file to the directory.
        let table_config_path = directory.join("table.json");
        std::fs::File::create(&table_config_path)
            .map_err(|e| TableError::TableConfigWriteError(e))?;

        // Serialize the table into pretty JSON format.
        let table_json = serde_json::to_string_pretty(self)
            .map_err(|e| TableError::TableConfigSerializationError(e))?;
        std::fs::write(&table_config_path, table_json)
            .map_err(|e| TableError::TableConfigWriteError(e))?;

        // Initialize the table using its logical table provider.
        match &self.table_type {
            TableType::Logical(logical_table_provider) => Ok(logical_table_provider
                .create(directory, session_ctx)
                .await?),
        }
    }

    /// Obtains a `TableProvider` for executing queries against this table.
    ///
    /// This method leverages the underlying logical table provider to produce a DataFusion-compatible
    /// table provider.
    ///
    /// # Arguments
    ///
    /// * `session_ctx` - A shared session context to be used during provider creation.
    ///
    /// # Errors
    ///
    /// Returns a `TableError` if there is an issue retrieving the table provider.
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

    /// Returns the name of the table.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}
