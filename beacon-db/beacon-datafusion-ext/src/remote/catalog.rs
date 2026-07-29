//! A lazy [`CatalogProvider`] over a whole remote Beacon instance.
//!
//! Attaching a remote enumerates its schemas and tables once (over Flight SQL, via the remote's
//! `information_schema`), then exposes each remote table as a federated [`TableProvider`] built on
//! demand. Because every table shares one endpoint (compute context), the federation optimizer
//! pushes joins and aggregates *between* remote tables down to the remote, not just single scans.
//!
//! The listing is a snapshot taken at attach time; re-attach to pick up tables created on the
//! remote afterward. Each table's schema and provider are resolved lazily on first access and
//! cached.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion_federation::sql::{RemoteTableRef, SQLFederationProvider, SQLTable, SQLTableSource};
use datafusion_federation::FederatedTableProviderAdaptor;
use parking_lot::Mutex;

use super::connection::RemoteConnection;
use super::definition::{BeaconRemoteSqlTable, RemoteTableDefinition};
use super::executor::BeaconFlightSqlExecutor;

/// The whole remote instance, presented as a DataFusion catalog.
pub struct RemoteCatalogProvider {
    schemas: BTreeMap<String, Arc<RemoteSchemaProvider>>,
}

impl std::fmt::Debug for RemoteCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteCatalogProvider")
            .field("schemas", &self.schemas.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl RemoteCatalogProvider {
    /// Connect to the remote, enumerate its schemas and tables once, and build the catalog.
    pub async fn connect(connection: RemoteConnection) -> anyhow::Result<Self> {
        let listing = enumerate_remote(&connection).await?;
        Ok(Self::from_listing(connection, listing))
    }

    /// Build a catalog from a pre-fetched `{schema -> [table, …]}` listing. Split out so the
    /// catalog structure can be unit-tested without a live remote.
    pub fn from_listing(
        connection: RemoteConnection,
        listing: BTreeMap<String, Vec<String>>,
    ) -> Self {
        let schemas = listing
            .into_iter()
            .map(|(schema, tables)| {
                let provider =
                    Arc::new(RemoteSchemaProvider::new(connection.clone(), schema.clone(), tables));
                (schema, provider)
            })
            .collect();
        Self { schemas }
    }
}

impl CatalogProvider for RemoteCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|schema| Arc::clone(schema) as Arc<dyn SchemaProvider>)
    }
}

/// One remote schema. Table names come from the attach-time snapshot; each table's provider is
/// built (and its schema fetched) lazily on first access, then cached.
struct RemoteSchemaProvider {
    connection: RemoteConnection,
    schema: String,
    tables: Vec<String>,
    cache: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl RemoteSchemaProvider {
    fn new(connection: RemoteConnection, schema: String, tables: Vec<String>) -> Self {
        Self {
            connection,
            schema,
            tables,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

impl std::fmt::Debug for RemoteSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteSchemaProvider")
            .field("schema", &self.schema)
            .field("tables", &self.tables)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for RemoteSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.clone()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.iter().any(|table| table == name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if !self.table_exist(name) {
            return Ok(None);
        }
        if let Some(cached) = self.cache.lock().get(name).cloned() {
            return Ok(Some(cached));
        }
        let remote_table = format!("{}.{}", self.schema, name);
        let provider = build_federated_provider(self.connection.clone(), remote_table)
            .await
            .map_err(|e| DataFusionError::External(format!("remote beacon catalog: {e}").into()))?;
        self.cache
            .lock()
            .insert(name.to_string(), Arc::clone(&provider));
        Ok(Some(provider))
    }
}

/// Build a federated provider for one remote table (`schema.table`), resolving its schema from the
/// remote. Mirrors [`RemoteTableDefinition::build_provider`], minus the persistence bookkeeping —
/// attached catalog tables live only for the session.
async fn build_federated_provider(
    connection: RemoteConnection,
    remote_table: String,
) -> anyhow::Result<Arc<dyn TableProvider>> {
    let schema = BeaconFlightSqlExecutor::fetch_schema(&connection, &remote_table)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve remote table `{remote_table}`: {e}"))?;

    let executor = Arc::new(BeaconFlightSqlExecutor::new(connection.clone()));
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let table_ref = RemoteTableRef::try_from(remote_table.as_str())?;

    let definition = RemoteTableDefinition {
        name: remote_table.clone(),
        url: connection.url.clone(),
        remote_table,
        schema: Arc::clone(&schema),
    };
    let sql_table: Arc<dyn SQLTable> =
        Arc::new(BeaconRemoteSqlTable::new(definition, table_ref, schema));
    let source = Arc::new(SQLTableSource::new_with_table(provider, sql_table));
    Ok(Arc::new(FederatedTableProviderAdaptor::new(source)))
}

/// Enumerate the remote's user schemas and tables via its `information_schema` (over Flight SQL).
///
/// `information_schema` itself is excluded (it is DataFusion's own reflection schema, not user
/// data); everything else the remote exposes — `public`, beacon's `system`, and any other schema —
/// is mirrored.
async fn enumerate_remote(
    connection: &RemoteConnection,
) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
    let batches = connection
        .collect_query(
            "SELECT table_schema, table_name FROM information_schema.tables \
             WHERE table_schema <> 'information_schema' \
             ORDER BY table_schema, table_name",
        )
        .await?;

    let mut listing: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for batch in &batches {
        let schemas = string_column(batch, 0)?;
        let tables = string_column(batch, 1)?;
        for row in 0..batch.num_rows() {
            if schemas.is_null(row) || tables.is_null(row) {
                continue;
            }
            listing
                .entry(schemas.value(row).to_string())
                .or_default()
                .push(tables.value(row).to_string());
        }
    }
    Ok(listing)
}

/// Downcast a result column to a `Utf8` array, erroring clearly if the remote returned another
/// type for an `information_schema` column (it should not).
fn string_column(batch: &arrow::record_batch::RecordBatch, index: usize) -> anyhow::Result<&StringArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            anyhow::anyhow!("remote information_schema column {index} was not Utf8 as expected")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::catalog::CatalogProvider;

    fn listing() -> BTreeMap<String, Vec<String>> {
        BTreeMap::from([
            ("public".to_string(), vec!["obs".to_string(), "argo".to_string()]),
            ("system".to_string(), vec!["functions".to_string()]),
        ])
    }

    #[test]
    fn catalog_mirrors_the_listing_structure() {
        let catalog = RemoteCatalogProvider::from_listing(
            RemoteConnection::new("http://remote:50051".to_string()),
            listing(),
        );

        let mut schemas = catalog.schema_names();
        schemas.sort();
        assert_eq!(schemas, vec!["public", "system"]);

        let public = catalog.schema("public").expect("public schema exists");
        let mut tables = public.table_names();
        tables.sort();
        assert_eq!(tables, vec!["argo", "obs"]);
        assert!(public.table_exist("obs"));
        assert!(!public.table_exist("missing"));

        assert!(catalog.schema("nope").is_none());
    }
}
