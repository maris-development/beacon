use std::{any::Any, fmt::Debug, path::PathBuf, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    common::not_impl_err,
    error::DataFusionError,
    prelude::SessionContext,
};

use crate::{empty_table::EmptyTable, error::TableError, table::Table, LogicalTableProvider};

pub struct BeaconSchemaProvider {
    session_ctx: Arc<SessionContext>,
    tables_map: Arc<parking_lot::Mutex<indexmap::IndexMap<String, Table>>>,
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
    pub async fn new(session_ctx: Arc<SessionContext>) -> Result<Self, TableError> {
        //Create tables dir if it doesnt exists
        let root_dir_path =
            beacon_config::DATA_DIR.join(beacon_config::TABLES_DIR_PREFIX.to_string());

        tokio::fs::create_dir_all(&root_dir_path)
            .await
            .map_err(|e| TableError::FailedToCreateBaseTableDirectory(e))?;

        // //Read all tables. Each .json file in the tables directory is a table.
        let mut tables_map = indexmap::IndexMap::new();
        let tables = Self::load_tables(&root_dir_path).await?;
        for table in tables {
            tracing::debug!("Loaded table: {}", table.table_name());
            tables_map.insert(table.table_name().to_string(), table);
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

    async fn load_tables(base_path: &std::path::Path) -> Result<Vec<Table>, TableError> {
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
                    tables.push(table);
                }
            }
        }
        Ok(tables)
    }

    pub async fn add_table(&self, table: Table) -> Result<(), TableError> {
        let mut locked_tables = self.tables_map.lock();
        if !locked_tables.contains_key(table.table_name()) {
            //Create the dir with the name of the table in the tables directory
            table
                .create(self.root_dir_path.clone(), self.session_ctx.clone())
                .await?;
            tracing::debug!("Created table: {}", table.table_name());
            locked_tables.insert(table.table_name().to_string(), table);

            Ok(())
        } else {
            tracing::warn!("Table already exists: {}", table.table_name());
            Err(TableError::TableAlreadyExists(
                table.table_name().to_string(),
            ))
        }
    }

    pub async fn delete_table(&self, table_name: &str) -> Result<(), TableError> {
        let mut locked_tables = self.tables_map.lock();
        if let Some(table) = locked_tables.shift_remove(table_name) {
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
    /// Returns the owner of the Schema, default is None. This value is reported
    /// as part of `information_tables.schemata
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        self.tables_map.lock().keys().cloned().collect()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.tables_map.lock().get(name) {
            let table = table
                .table_provider(self.session_ctx.clone())
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

    /// If supported by the implementation, adds a new table named `name` to
    /// this schema.
    ///
    /// If a table of the same name was already registered, returns "Table
    /// already exists" error.
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!("Beacon does not support registering tables.")
    }

    /// If supported by the implementation, removes the `name` table from this
    /// schema and returns the previously registered [`TableProvider`], if any.
    ///
    /// If no `name` table exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!("Beacon does not support deregistering tables.")
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.tables_map.lock().contains_key(name)
    }
}
