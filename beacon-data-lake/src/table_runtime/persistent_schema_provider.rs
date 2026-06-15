//! The `beacon.public` schema provider.
//!
//! This is a thin wrapper around DataFusion's native [`MemorySchemaProvider`]:
//! the in-memory catalog (register/lookup/deregister) is delegated to it
//! verbatim. The wrapper exists only to add one side effect — persisting and
//! removing each table's `tables://<name>/table.json` definition as it is
//! registered or deregistered — so that the catalog survives restarts (it is
//! rebuilt at startup by [`crate::init_tables`]).

use std::{any::Any, sync::Arc};

use arrow::datatypes::Schema;
use datafusion::{
    catalog::{MemorySchemaProvider, SchemaProvider, TableProvider},
    datasource::empty::EmptyTable,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};

use super::schema_persistence::SchemaPersistenceService;

/// Schema provider for `beacon.public` that persists table definitions on
/// registration and removes them on deregistration.
pub struct PersistentSchemaProvider {
    inner: Arc<MemorySchemaProvider>,
    runtime_handle: tokio::runtime::Handle,
    session_context: Arc<SessionContext>,
    table_directory_store_url: ObjectStoreUrl,
}

impl std::fmt::Debug for PersistentSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentSchemaProvider")
            .field("table_directory_store_url", &self.table_directory_store_url)
            .field("table_count", &self.inner.table_names().len())
            .finish()
    }
}

impl PersistentSchemaProvider {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_context: Arc<SessionContext>,
        table_directory_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            inner: Arc::new(MemorySchemaProvider::new()),
            runtime_handle,
            session_context,
            table_directory_store_url,
        }
    }

    fn schema_persistence_service(&self) -> SchemaPersistenceService {
        SchemaPersistenceService::new(
            self.session_context.clone(),
            self.table_directory_store_url.clone(),
        )
    }

    /// Register a provider that was loaded from a persisted definition, without
    /// re-persisting it. Used by [`crate::init_tables`] during startup recovery.
    pub fn insert_loaded(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    /// Register the in-memory `default` table backed by an empty provider.
    ///
    /// The default table is not persisted; it is recreated on every startup so
    /// queries against the configured default table always resolve.
    pub fn ensure_default_table(&self) {
        if self.inner.table_exist("default") {
            return;
        }
        let provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        let _ = self.inner.register_table("default".to_string(), provider);
    }
}

#[async_trait::async_trait]
impl SchemaProvider for PersistentSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.inner.table(name).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let handle = self.runtime_handle.clone();
        let persistence = self.schema_persistence_service();
        let persist_name = name.clone();
        let persist_table = table.clone();
        tokio::task::block_in_place(|| {
            handle.block_on(async move {
                persistence
                    .persist_provider_definition(&persist_name, persist_table.as_ref())
                    .await
            })
        })?;

        // DataFusion's `MemorySchemaProvider` refuses to overwrite an existing
        // entry, but beacon registers a fresh provider over an existing name to
        // swap it (materialized-view refresh, Iceberg replace/alter). Drop any
        // prior entry first so registration overwrites, returning the old one.
        let previous = self.inner.deregister_table(&name)?;
        self.inner.register_table(name, table)?;
        Ok(previous)
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let handle = self.runtime_handle.clone();
        let persistence = self.schema_persistence_service();
        let remove_name = name.to_string();
        tokio::task::block_in_place(|| {
            handle.block_on(async move { persistence.remove_persisted_table(&remove_name).await })
        })?;

        self.inner.deregister_table(name)
    }
}
