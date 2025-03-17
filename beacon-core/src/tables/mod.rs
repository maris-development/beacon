use std::{any::Any, path::PathBuf, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    common::not_impl_err,
    error::DataFusionError,
    execution::SessionState,
};
use table::BeaconTable;

pub mod empty_table;
pub mod glob_table;
pub mod table;

#[derive(Debug)]
pub struct BeaconSchemaProvider {
    session_state: Arc<SessionState>,
    tables_map: Arc<parking_lot::Mutex<indexmap::IndexMap<String, Arc<dyn BeaconTable>>>>,
    root_dir_path: PathBuf,
}

impl BeaconSchemaProvider {
    pub async fn new(session_state: Arc<SessionState>) -> anyhow::Result<Self> {
        //Create tables dir if it doesnt exists
        let root_dir_path =
            beacon_config::DATA_DIR.join(beacon_config::TABLES_DIR_PREFIX.to_string());

        tokio::fs::create_dir_all(&root_dir_path).await?;

        //Read all tables. Each .json file in the tables directory is a table.
        let mut tables_map = indexmap::IndexMap::new();
        let tables = Self::load_tables(&root_dir_path).await?;
        for table in tables {
            tracing::debug!("Loaded table: {}", table.table_name());
            tables_map.insert(table.table_name().to_string(), table);
        }

        let provider = Self {
            session_state,
            tables_map: Arc::new(parking_lot::Mutex::new(tables_map)),
            root_dir_path,
        };

        if !provider.table_exist(beacon_config::CONFIG.default_table.as_str()) {
            provider
                .add_table(Arc::new(empty_table::EmptyDefaultTable::new(
                    beacon_config::CONFIG.default_table.clone(),
                )))
                .await?;
        }

        Ok(provider)
    }

    async fn load_tables(
        base_path: &std::path::Path,
    ) -> std::io::Result<Vec<Arc<dyn BeaconTable>>> {
        let mut tables = Vec::new();

        let mut dir = tokio::fs::read_dir(base_path).await.unwrap();

        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();

            // Use async metadata to check if the path is a directory.
            if let Ok(metadata) = tokio::fs::metadata(&path).await {
                if metadata.is_dir() {
                    let json_path = path.join("table.json");
                    // Check if the JSON file exists.
                    if tokio::fs::metadata(&json_path).await.is_ok() {
                        let json_data = tokio::fs::read_to_string(&json_path).await?;
                        let table: Arc<dyn BeaconTable> = serde_json::from_str(&json_data)
                            .expect("Failed to deserialize DataTable");
                        tables.push(table);
                    }
                }
            }
        }
        Ok(tables)
    }

    pub async fn add_table(&self, table: Arc<dyn BeaconTable>) -> anyhow::Result<()> {
        let mut locked_tables = self.tables_map.lock();
        if !locked_tables.contains_key(table.table_name()) {
            //Create the dir with the name of the table in the tables directory
            let table_directory = self.root_dir_path.join(table.table_name());
            tokio::fs::create_dir_all(&table_directory).await?;
            //Now write the table to a json file inside the tables directory

            let table_json_path = table_directory.join("table.json");
            let table_json = serde_json::to_string(&table)?;
            tokio::fs::write(table_json_path, table_json).await?;
            locked_tables.insert(table.table_name().to_string(), table);

            Ok(())
        } else {
            anyhow::bail!("Table with name {} already exists", table.table_name());
        }
    }

    pub async fn delete_table(&self, table_name: &str) -> anyhow::Result<()> {
        let mut locked_tables = self.tables_map.lock();
        if let Some(table) = locked_tables.shift_remove(table_name) {
            let table_path = self.root_dir_path.join(table_name);
            tokio::fs::remove_dir_all(table_path).await?;
            Ok(())
        } else {
            anyhow::bail!("Table with name {} does not exist", table_name);
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
        let state_cloned = self.session_state.clone();
        if let Some(table) = self.tables_map.lock().get(name).cloned() {
            let table = table.as_table(state_cloned).await;
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
