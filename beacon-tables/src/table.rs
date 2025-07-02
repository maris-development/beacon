use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use datafusion::{catalog::TableProvider, prelude::SessionContext};

use crate::{
    error::TableError, physical_table::PhysicalTableProvider, preset_table::PresetTable,
    table_extension::TableExtension, LogicalTableProvider,
};

#[derive(Debug)]
pub struct TableInfo {
    pub table: Table,
    pub table_directory: PathBuf,
}

/// Represents a table configuration along with its associated provider.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Table {
    /// The name of the table.
    pub table_name: String,
    /// The type of the table which determines its behavior.
    pub table_type: TableType,
    /// A vector of table extensions that provide additional functionality.
    #[serde(default)]
    pub table_extensions: Vec<Arc<dyn TableExtension>>,
}

impl Table {
    /// Creates a new instance of `Table`.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table.
    /// * `table_type` - The type of the table (logical or physical).
    /// * `table_extensions` - A vector of table extensions associated with the table.
    ///
    /// # Returns
    ///
    /// A new instance of `Table`.
    pub fn new(
        table_name: String,
        table_type: impl Into<TableType>,
        table_extensions: Vec<Arc<dyn TableExtension>>,
    ) -> Self {
        Table {
            table_name,
            table_type: table_type.into(),
            table_extensions,
        }
    }
}

/// Enum representing different types of tables.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TableType {
    /// A logical table with its associated provider.
    Logical(Arc<dyn LogicalTableProvider>),
    Physical(Arc<dyn PhysicalTableProvider>),
    PresetTable(PresetTable),
}

impl From<Arc<dyn LogicalTableProvider>> for TableType {
    fn from(logical_table_provider: Arc<dyn LogicalTableProvider>) -> Self {
        TableType::Logical(logical_table_provider)
    }
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
        self,
        base_table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<TableInfo, TableError> {
        // Ensure the base table directory exists.
        if !base_table_directory.exists() {
            return Err(TableError::BaseTableDirectoryDoesNotExist);
        }

        // Create the specific table directory.
        let table_directory = base_table_directory.join(&self.table_name);
        if !table_directory.exists() {
            tokio::fs::create_dir_all(&table_directory)
                .await
                .map_err(|e| {
                    TableError::FailedToCreateTableDirectory(
                        e,
                        table_directory.to_string_lossy().to_string(),
                    )
                })?;
        }

        // Write the table configuration as a JSON file to the directory.
        let table_config_path = table_directory.join("table.json");
        std::fs::File::create(&table_config_path)
            .map_err(|e| TableError::TableConfigWriteError(e))?;

        // Serialize the table into pretty JSON format.
        let table_json = serde_json::to_string_pretty(&self)
            .map_err(|e| TableError::TableConfigSerializationError(e))?;
        std::fs::write(&table_config_path, table_json)
            .map_err(|e| TableError::TableConfigWriteError(e))?;

        // Initialize the table using its logical table provider.
        match &self.table_type {
            TableType::Logical(logical_table_provider) => {
                logical_table_provider
                    .create(table_directory.clone(), session_ctx)
                    .await?
            }
            TableType::Physical(physical_table_provider) => physical_table_provider
                .create(table_directory.clone(), session_ctx)
                .await
                .unwrap(),
            TableType::PresetTable(preset_table) => {
                preset_table
                    .create(table_directory.clone(), session_ctx)
                    .await?
            }
        };

        // Return the table information.
        Ok(TableInfo {
            table: self,
            table_directory,
        })
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
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        match &self.table_type {
            TableType::Logical(logical_table) => {
                Ok(logical_table.table_provider(session_ctx).await?)
            }
            TableType::Physical(physical_table) => Ok(physical_table
                .table_provider(table_directory, session_ctx)
                .await?),
            TableType::PresetTable(preset_table) => Ok(preset_table
                .table_provider(table_directory, session_ctx)
                .await?),
        }
    }

    /// Returns the name of the table.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}
