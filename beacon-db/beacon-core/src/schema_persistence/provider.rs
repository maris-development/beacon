//! The `beacon.public` schema provider.
//!
//! This is a thin wrapper around DataFusion's native [`MemorySchemaProvider`]:
//! the in-memory catalog (register/lookup/deregister) is delegated to it
//! verbatim. The wrapper exists only to add one side effect — persisting and
//! removing each table's `db://<name>/table.json` definition as it is
//! registered or deregistered — so that the catalog survives restarts (it is
//! rebuilt at startup by [`super::init_tables`]).

use std::{
    any::Any,
    sync::{Arc, Weak},
};

use arrow::datatypes::Schema;
use datafusion::{
    catalog::{MemorySchemaProvider, SchemaProvider, TableProvider},
    datasource::empty::EmptyTable,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};

use beacon_datafusion_ext::table_ext::INTERNAL_TABLE_PREFIX;

use super::service::SchemaPersistenceService;

/// Schema provider for `beacon.public` that persists table definitions on
/// registration and removes them on deregistration.
pub struct PersistentSchemaProvider {
    inner: Arc<MemorySchemaProvider>,
    runtime_handle: tokio::runtime::Handle,
    /// A [`Weak`] back-reference: this provider is registered *in* the session
    /// context's catalog, so a strong reference would form a cycle that leaks the
    /// context (and the tables store it registers, holding its exclusive lock)
    /// for the process lifetime. Every use upgrades; during a live runtime the
    /// context always outlives this provider.
    session_context: Weak<SessionContext>,
    db_store_url: ObjectStoreUrl,
}

impl std::fmt::Debug for PersistentSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentSchemaProvider")
            .field("table_directory_store_url", &self.db_store_url)
            .field("table_count", &self.inner.table_names().len())
            .finish()
    }
}

impl PersistentSchemaProvider {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_context: Arc<SessionContext>,
        db_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            inner: Arc::new(MemorySchemaProvider::new()),
            runtime_handle,
            session_context: Arc::downgrade(&session_context),
            db_store_url,
        }
    }

    /// The persistence side-effect service, or `None` if the session context has
    /// been torn down (in which case there is no tables store to persist to). In
    /// practice the context always outlives register/deregister on a live runtime.
    fn schema_persistence_service(&self) -> Option<SchemaPersistenceService> {
        Some(SchemaPersistenceService::new(
            self.session_context.upgrade()?,
            self.db_store_url.clone(),
        ))
    }

    /// Run a persistence side effect to completion from the sync `SchemaProvider`
    /// trait methods. A no-op when the session context is gone. Requires a
    /// multi-threaded runtime (`block_in_place`).
    fn run_persistence<F, Fut>(&self, side_effect: F) -> datafusion::error::Result<()>
    where
        F: FnOnce(SchemaPersistenceService) -> Fut,
        Fut: std::future::Future<Output = datafusion::error::Result<()>>,
    {
        let Some(persistence) = self.schema_persistence_service() else {
            return Ok(());
        };
        let handle = self.runtime_handle.clone();
        tokio::task::block_in_place(|| handle.block_on(side_effect(persistence)))
    }

    /// Register a provider that was loaded from a persisted definition, without
    /// re-persisting it. Used by [`super::init_tables`] during startup recovery.
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
        // Beacon's internal auth tables are hidden from user-facing listings
        // (this backs both `SHOW TABLES` and `information_schema`); they hold
        // password hashes and are super-user-only. `table`/`table_exist` still
        // resolve them so the auth write path and super-users reach them.
        self.inner
            .table_names()
            .into_iter()
            .filter(|name| !name.starts_with(INTERNAL_TABLE_PREFIX))
            .collect()
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
        self.run_persistence(|persistence| {
            let persist_name = name.clone();
            let persist_table = table.clone();
            async move {
                persistence
                    .persist_provider_definition(&persist_name, persist_table.as_ref())
                    .await
            }
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
        self.run_persistence(|persistence| {
            let remove_name = name.to_string();
            async move { persistence.remove_persisted_table(&remove_name).await }
        })?;

        self.inner.deregister_table(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::ViewTable;
    use futures::StreamExt;
    use object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt};
    use url::Url;

    // The persistence side effect runs the async store I/O via `block_in_place`,
    // which requires a multi-threaded runtime.
    fn fixture() -> (PersistentSchemaProvider, Arc<SessionContext>, Arc<InMemory>) {
        let session_context = Arc::new(SessionContext::new());
        let tables_store = Arc::new(InMemory::new());
        let tables_url = ObjectStoreUrl::parse("db://").expect("db url should parse");
        session_context.register_object_store(
            &Url::parse(tables_url.as_str()).expect("db url should be valid"),
            tables_store.clone(),
        );
        let provider = PersistentSchemaProvider::new(
            tokio::runtime::Handle::current(),
            session_context.clone(),
            tables_url,
        );
        (provider, session_context, tables_store)
    }

    async fn view(session_context: &SessionContext, sql: &str) -> Arc<dyn TableProvider> {
        let plan = session_context
            .state()
            .create_logical_plan(sql)
            .await
            .expect("logical plan should be created");
        Arc::new(ViewTable::new(plan, Some(sql.to_string())))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_persists_definition_and_registers_in_catalog() {
        let (provider, ctx, store) = fixture();

        let previous = provider
            .register_table("v".to_string(), view(&ctx, "SELECT 1 AS x").await)
            .expect("registration should succeed");

        assert!(
            previous.is_none(),
            "first registration has no previous table"
        );
        assert!(provider.table_exist("v"));
        assert!(
            store.get(&Path::from("v/table.json")).await.is_ok(),
            "the definition should be persisted to the tables store"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_overwrites_existing_name() {
        // DataFusion's `MemorySchemaProvider` errors when a name already exists;
        // the wrapper must overwrite instead so materialized-view refresh and
        // Iceberg replace/alter can swap a fresh provider under the same name.
        let (provider, ctx, _store) = fixture();
        provider
            .register_table("v".to_string(), view(&ctx, "SELECT 1 AS x").await)
            .expect("first registration should succeed");

        let previous = provider
            .register_table("v".to_string(), view(&ctx, "SELECT 2 AS y").await)
            .expect("re-registering an existing name should overwrite, not error");

        assert!(
            previous.is_some(),
            "overwrite returns the replaced provider"
        );
        let table = provider
            .table("v")
            .await
            .expect("lookup should succeed")
            .expect("table should be present");
        assert_eq!(
            table.schema().field(0).name(),
            "y",
            "the catalog should resolve the newly registered provider"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deregister_removes_from_catalog_and_store() {
        let (provider, ctx, store) = fixture();
        provider
            .register_table("v".to_string(), view(&ctx, "SELECT 1 AS x").await)
            .expect("registration should succeed");

        let removed = provider
            .deregister_table("v")
            .expect("deregistration should succeed");

        assert!(removed.is_some(), "deregister returns the removed provider");
        assert!(!provider.table_exist("v"));
        let mut listing = store.list(Some(&Path::from("v")));
        assert!(
            listing.next().await.is_none(),
            "the persisted definition should be removed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_default_table_is_idempotent() {
        let (provider, _ctx, _store) = fixture();
        assert!(!provider.table_exist("default"));

        provider.ensure_default_table();
        provider.ensure_default_table();

        assert!(provider.table_exist("default"));
        assert_eq!(
            provider
                .table_names()
                .iter()
                .filter(|name| name.as_str() == "default")
                .count(),
            1,
            "the default table should be registered exactly once"
        );
    }
}
