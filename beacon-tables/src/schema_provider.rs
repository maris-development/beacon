use std::{any::Any, fmt::Debug, path::PathBuf, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    common::not_impl_err,
    error::DataFusionError,
    prelude::SessionContext,
};

use crate::{
    empty_table::EmptyTable,
    error::TableError,
    table::{Table, TableInfo},
    LogicalTableProvider,
};

/// A schema provider for Beacon which manages a collection of tables stored on disk.
///
/// This provider integrates with DataFusion's [`SchemaProvider`] trait and exposes
/// methods for adding, deleting, and retrieving tables.
pub struct BeaconSchemaProvider {
    session_ctx: Arc<SessionContext>,
    tables_map: Arc<parking_lot::Mutex<indexmap::IndexMap<String, TableInfo>>>,
    root_dir_path: PathBuf,
}

impl Debug for BeaconSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BeaconSchemaProvider")
            .field("tables_map", &self.tables_map)
            .field("root_dir_path", &self.root_dir_path)
            .finish()
    }
}

impl BeaconSchemaProvider {
    /// Creates a new instance of [`BeaconSchemaProvider`].
    ///
    /// This function initializes the tables directory, loads existing tables from disk,
    /// and ensures that a default table exists.
    ///
    /// # Parameters
    /// - `session_ctx`: A shared session context used for table operations.
    ///
    /// # Errors
    /// Returns a [`TableError`] if the base directory cannot be created or if there is
    /// an error loading existing tables.
    pub async fn new(session_ctx: Arc<SessionContext>) -> Result<Self, TableError> {
        // Construct the path to the tables directory.
        let root_dir_path =
            beacon_config::DATA_DIR.join(beacon_config::TABLES_DIR_PREFIX.to_string());

        // Create the tables directory if it does not exist.
        tokio::fs::create_dir_all(&root_dir_path)
            .await
            .map_err(|e| TableError::FailedToCreateBaseTableDirectory(e))?;

        // Load all existing tables from the tables directory.
        let mut tables_map = indexmap::IndexMap::new();
        let tables = Self::load_tables(&root_dir_path).await?;
        for table in tables {
            tracing::debug!("Loaded table: {}", table.table.table_name());
            tables_map.insert(table.table.table_name().to_string(), table);
        }

        let provider = Self {
            session_ctx,
            tables_map: Arc::new(parking_lot::Mutex::new(tables_map)),
            root_dir_path,
        };

        // If the default table does not exist, create an empty default logical table.
        if !provider.table_exist(beacon_config::CONFIG.default_table.as_str()) {
            let empty_table = EmptyTable::new(beacon_config::CONFIG.default_table.clone());
            let empty_table_provider =
                Table::from(Arc::new(empty_table) as Arc<dyn LogicalTableProvider>);

            provider.add_table(empty_table_provider).await?;
        }

        Ok(provider)
    }

    /// Loads tables from the given directory.
    ///
    /// This function reads the directory entries & attempts to open each directory as a table.
    ///
    /// # Parameters
    /// - `base_path`: The path where tables are stored.
    ///
    /// # Errors
    /// Returns a [`TableError`] if reading the directory or loading a table fails.
    async fn load_tables(base_path: &std::path::Path) -> Result<Vec<TableInfo>, TableError> {
        let mut tables = Vec::new();

        let mut dir = tokio::fs::read_dir(base_path)
            .await
            .map_err(|e| TableError::TableConfigLoadReadError(e))?;

        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(|e| TableError::TableConfigLoadReadError(e))?
        {
            let path = entry.path();

            // Use async metadata to check if the path is a directory.
            if let Ok(metadata) = tokio::fs::metadata(&path).await {
                if metadata.is_dir() {
                    let table = Table::open(path.clone())?;
                    let table_info = TableInfo {
                        table: table,
                        table_directory: path,
                    };
                    tables.push(table_info);
                }
            }
        }
        Ok(tables)
    }

    /// Adds a new table to the schema provider.
    ///
    /// This function attempts to create the table's directory and adds the table to the managed map.
    ///
    /// # Parameters
    /// - `table`: The table to add.
    ///
    /// # Errors
    /// Returns a [`TableError`] if the table already exists or if creation fails.
    pub async fn add_table(&self, table: Table) -> Result<(), TableError> {
        let mut locked_tables = self.tables_map.lock();
        if !locked_tables.contains_key(table.table_name()) {
            drop(locked_tables); // Drop the lock before creating the table otherwise it might deadlock on the creation of the table.
                                 // Create the directory with the name of the table in the tables directory.
            let table_info = table
                .create(self.root_dir_path.clone(), self.session_ctx.clone())
                .await?;
            tracing::debug!("Created table: {}", table_info.table.table_name());

            // Re-lock the tables map to insert the new table.
            locked_tables = self.tables_map.lock();
            locked_tables.insert(table_info.table.table_name().to_string(), table_info);

            Ok(())
        } else {
            tracing::warn!("Table already exists: {}", table.table_name());
            Err(TableError::TableAlreadyExists(
                table.table_name().to_string(),
            ))
        }
    }

    /// Deletes a table from the schema provider.
    ///
    /// This function removes the table's directory from disk and deletes the entry from the map.
    ///
    /// # Parameters
    /// - `table_name`: The name of the table to delete.
    ///
    /// # Errors
    /// Returns a [`TableError`] if the table does not exist or if there is an error during deletion.
    pub async fn delete_table(&self, table_name: &str) -> Result<(), TableError> {
        let mut locked_tables = self.tables_map.lock();
        if let Some(_table) = locked_tables.shift_remove(table_name) {
            tracing::debug!("Deleting table: {}", table_name);
            let table_path = self.root_dir_path.join(table_name);
            tokio::fs::remove_dir_all(table_path).await.map_err(|e| {
                TableError::TableDeletionError(Box::new(TableError::TableIOError(e)))
            })?;
            Ok(())
        } else {
            tracing::warn!("Table does not exist: {}", table_name);
            Err(TableError::TableDoesNotExist(table_name.to_string()))
        }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for BeaconSchemaProvider {
    /// Returns the owner of the schema.
    ///
    /// Currently, Beacon does not specify an owner so this value is always `None`.
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Provides access to the inner object as [`Any`], allowing the schema provider
    /// to be downcast to a concrete type.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves a list of available table names managed by this schema provider.
    fn table_names(&self) -> Vec<String> {
        self.tables_map.lock().keys().cloned().collect()
    }

    /// Retrieves a specific table by name, if it exists.
    ///
    /// # Parameters
    /// - `name`: The name of the table to retrieve.
    ///
    /// # Returns
    /// An optional [`Arc<dyn TableProvider>`] wrapped in a `Result`. Returns `None` if the table does not exist.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.tables_map.lock().get(name) {
            let table_directory = table.table_directory.clone();
            let table = table
                .table
                .table_provider(table_directory, self.session_ctx.clone())
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get table provider for table {}: {}",
                        name, e
                    ))
                })?;
            Ok(Some(table))
        } else {
            Ok(None)
        }
    }

    /// Registers a new table with the given name.
    ///
    /// Beacon does not support registering tables via this function, so an error is returned.
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!("Beacon does not support registering tables.")
    }

    /// Deregisters a table with the given name.
    ///
    /// Beacon does not support deregistering tables via this function, so an error is returned.
    #[allow(unused_variables)]
    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!("Beacon does not support deregistering tables.")
    }

    /// Checks if a table exists in the schema provider.
    ///
    /// # Parameters
    /// - `name`: The name of the table to check for existence.
    ///
    /// # Returns
    /// `true` if the table exists, `false` otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.tables_map.lock().contains_key(name)
    }
}
