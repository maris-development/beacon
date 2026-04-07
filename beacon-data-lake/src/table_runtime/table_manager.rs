use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{SchemaProvider, TableProvider}, error::DataFusionError,
    execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use object_store::{ObjectStore, path::PathPart};

use crate::table::{Table, TableFormat, empty::EmptyTable, error::TableError};

use super::{
    lifecycle::TableLifecycleService, loading, ordering,
    provider_factory::TableProviderFactory,
    schema_persistence::SchemaPersistenceService,
};

#[derive(Clone)]
struct TableRegistryEntry {
    table: Option<TableFormat>,
    provider: Arc<dyn TableProvider>,
}

pub struct TableManager {
    runtime_handle: tokio::runtime::Handle,
    data_directory_store_url: ObjectStoreUrl,
    table_directory_store_url: ObjectStoreUrl,
    session_context: Arc<SessionContext>,
    registry: parking_lot::Mutex<HashMap<String, TableRegistryEntry>>,
}

impl std::fmt::Debug for TableManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableManager")
            .field("data_directory_store_url", &self.data_directory_store_url)
            .field("table_directory_store_url", &self.table_directory_store_url)
            .field("table_count", &self.registry.lock().len())
            .finish()
    }
}

#[async_trait::async_trait]
impl SchemaProvider for TableManager {
    fn table_exist(&self, name: &str) -> bool {
        self.table_exist(name)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.table_provider(name))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        TableManager::register_table(self, name, table)
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        TableManager::deregister_table(self, name)
    }
}

impl TableManager {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_context: Arc<SessionContext>,
        data_directory_store_url: ObjectStoreUrl,
        table_directory_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            runtime_handle,
            data_directory_store_url,
            table_directory_store_url,
            session_context,
            registry: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    fn table_lifecycle_service(&self) -> TableLifecycleService {
        TableLifecycleService::new(
            self.session_context.clone(),
            self.data_directory_store_url.clone(),
            self.table_directory_store_url.clone(),
        )
    }

    fn schema_persistence_service(&self) -> SchemaPersistenceService {
        SchemaPersistenceService::new(
            self.session_context.clone(),
            self.table_directory_store_url.clone(),
        )
    }

    async fn register_tables(&self, tables_to_register: Vec<TableFormat>) {
        let provider_factory = TableProviderFactory::new(
            self.session_context.clone(),
            self.data_directory_store_url.clone(),
            self.table_directory_store_url.clone(),
        );

        for table in tables_to_register {
            let table_name = table.table_name().to_string();
            let table_kind = match &table {
                TableFormat::Legacy(_) => "legacy",
                TableFormat::DefinitionBased(_) => "definition-based",
            };

            match provider_factory.build(&table).await {
                Ok((name, provider)) => {
                    self.registry.lock().insert(
                        name.clone(),
                        TableRegistryEntry {
                            table: Some(table),
                            provider,
                        },
                    );
                    tracing::info!("Registered {} table '{}'", table_kind, name);
                }
                Err(error) => {
                    tracing::error!(
                        "Failed to build provider for {} table '{}': {}. Skipping registration of this table.",
                        table_kind,
                        table_name,
                        error
                    );
                }
            }
        }
    }

    async fn init_tables_impl(
        tables_object_store_url: ObjectStoreUrl,
        session_context: Arc<SessionContext>,
    ) -> anyhow::Result<Vec<TableFormat>> {
        tracing::info!("Initializing tables from object store");
        let tables_object_store = session_context
            .runtime_env()
            .object_store(&tables_object_store_url)
            .map_err(|error| anyhow::anyhow!("Failed to get tables object store: {}", error))?;

        let discovered_tables = Self::load_tables_from_object_store(tables_object_store).await;
        Ok(discovered_tables)
    }

    async fn load_tables_from_object_store(tables_object_store: Arc<dyn ObjectStore>) -> Vec<TableFormat> {
        loading::load_tables_from_object_store(tables_object_store).await
    }

    async fn order_tables(tables: &HashMap<String, TableFormat>) -> Vec<TableFormat> {
        ordering::order_tables(tables).await
    }

    async fn ensure_default_table(&self) -> anyhow::Result<()> {
        if self.table_exist("default") {
            return Ok(());
        }

        let table = Table {
            table_directory: vec![],
            table_name: "default".to_string(),
            table_type: crate::table::_type::TableType::Empty(EmptyTable::new()),
            description: Some("Default Table.".to_string()),
        };

        self.create_table(table)
            .await
            .map_err(|error| anyhow::anyhow!("Failed to create default table: {}", error))
    }

    pub async fn init_tables(&self) -> anyhow::Result<()> {
        let table_formats =
            Self::init_tables_impl(self.table_directory_store_url.clone(), self.session_context.clone())
                .await?;

        let table_map = table_formats
            .into_iter()
            .map(|table| (table.table_name().to_string(), table))
            .collect::<HashMap<_, _>>();
        let ordered_table_formats = Self::order_tables(&table_map).await;

        self.registry.lock().clear();
        self.register_tables(ordered_table_formats).await;

        self.ensure_default_table().await
    }

    pub fn table_exist(&self, name: &str) -> bool {
        self.registry.lock().contains_key(name)
    }

    pub fn table_names(&self) -> Vec<String> {
        self.registry.lock().keys().cloned().collect()
    }

    pub fn table_provider(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.registry.lock().get(name).map(|entry| entry.provider.clone())
    }

    pub fn list_table_schema(&self, table_name: &str) -> Option<SchemaRef> {
        self.registry
            .lock()
            .get(table_name)
            .map(|entry| entry.provider.schema())
    }

    pub fn list_table(&self, table_name: &str) -> Option<TableFormat> {
        self.registry
            .lock()
            .get(table_name)
            .and_then(|entry| entry.table.clone())
    }

    pub async fn update_table(&self, table: Table) -> Result<(), TableError> {
        self.deregister_table(&table.table_name)?;
        self.create_table(table).await
    }

    pub async fn create_table(&self, mut table: Table) -> Result<(), TableError> {
        if self.registry.lock().contains_key(&table.table_name) {
            return Err(TableError::TableAlreadyExists(table.table_name));
        }

        let table_provider = table
            .table_provider(
                self.session_context.clone(),
                self.data_directory_store_url.clone(),
                self.table_directory_store_url.clone(),
            )
            .await?;

        let table_object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.table_directory_store_url)
            .unwrap();

        let table_directory: Vec<PathPart<'static>> =
            vec![PathPart::from(table.table_name.clone())];
        table.save(table_object_store, table_directory).await;

        self.registry.lock().insert(
            table.table_name.clone(),
            TableRegistryEntry {
                table: Some(TableFormat::Legacy(table)),
                provider: table_provider,
            },
        );

        Ok(())
    }

    pub async fn refresh_table(&self, table_name: &str) -> anyhow::Result<()> {
        let table = self
            .registry
            .lock()
            .get(table_name)
            .and_then(|entry| entry.table.clone())
            .ok_or(anyhow::anyhow!("Table not found: {}", table_name))?;

        let lifecycle = self.table_lifecycle_service();
        let refreshed_table_provider = lifecycle.refresh_provider(&table).await?;

        let mut registry = self.registry.lock();
        if let Some(entry) = registry.get_mut(table_name) {
            entry.provider = refreshed_table_provider;
        }

        Ok(())
    }

    pub async fn apply_operation(
        &self,
        table_name: &str,
        op: serde_json::Value,
    ) -> anyhow::Result<()> {
        let table = self
            .registry
            .lock()
            .get(table_name)
            .and_then(|entry| entry.table.clone())
            .ok_or(anyhow::anyhow!("Table not found: {}", table_name))?;

        let lifecycle = self.table_lifecycle_service();
        lifecycle.apply_operation(&table, op).await
    }

    pub fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let handle = self.runtime_handle.clone();
        let persistence = self.schema_persistence_service();
        let persist_name = name.clone();
        let persist_table = table.clone();
        let cloned_table = table.clone();
        tokio::task::block_in_place(|| {
            handle.block_on(async move {
                persistence
                    .persist_provider_definition(&persist_name, persist_table.as_ref())
                    .await
            })
        })?;

        self.registry.lock().insert(
            name,
            TableRegistryEntry {
                table: None,
                provider: table,
            },
        );

        Ok(Some(cloned_table))
    }

    pub fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let handle = self.runtime_handle.clone();
        let persistence = self.schema_persistence_service();
        tokio::task::block_in_place(|| {
            handle.block_on(async move { persistence.remove_persisted_table(name).await })
        })?;

        Ok(self.registry.lock().remove(name).map(|entry| entry.provider))
    }
}