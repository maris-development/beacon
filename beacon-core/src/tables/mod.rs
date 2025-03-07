use std::{any::Any, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    common::not_impl_err,
    error::DataFusionError,
    execution::SessionState,
};
use table::BeaconTable;

pub mod glob_table;
pub mod indexed_table;
pub mod table;

#[derive(Debug)]
pub struct BeaconSchemaProvider {
    session_state: Arc<SessionState>,
    tables: Arc<tokio::sync::Mutex<indexmap::IndexMap<String, Arc<dyn BeaconTable>>>>,
}

impl BeaconSchemaProvider {
    pub async fn new(session_state: Arc<SessionState>) -> anyhow::Result<Self> {
        //Read all tables. Each .json file in the tables directory is a table.
        let tables = Arc::new(tokio::sync::Mutex::new(indexmap::IndexMap::new()));
        while let Ok(entry) = tokio::fs::read_dir("tables").await?.next_entry().await {
            if let Some(path) = entry {
                if path.file_type().await?.is_file() {
                    let json_string = tokio::fs::read_to_string(path.path()).await?;
                    let table: Arc<dyn BeaconTable> = serde_json::from_str(&json_string)?;
                    tables
                        .lock()
                        .await
                        .insert(table.table_name().to_string(), table);
                }
            }
        }

        Ok(Self {
            session_state,
            tables: Arc::new(tokio::sync::Mutex::new(indexmap::IndexMap::new())),
        })
    }

    pub async fn add_table(&self, table: Arc<dyn BeaconTable>) {
        self.tables
            .lock()
            .await
            .insert(table.table_name().to_string(), table);
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
        self.tables.blocking_lock().keys().cloned().collect()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let state_cloned = self.session_state.clone();
        if let Some(table) = self.tables.lock().await.get(name).cloned() {
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
        not_impl_err!("schema provider does not support registering tables")
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
        not_impl_err!("schema provider does not support deregistering tables")
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.tables.blocking_lock().contains_key(name)
    }
}
