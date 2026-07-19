//! Serializable definition and SQL-table wrapper for a federated remote table.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_federation::sql::{RemoteTableRef, SQLFederationProvider, SQLTable, SQLTableSource};

use crate::table_ext::TableDefinition;

use super::connection::RemoteConnection;
use super::executor::BeaconFlightSqlExecutor;

/// Persisted configuration for a federated remote-Beacon table.
///
/// Stored as `table.json` and reloaded at startup like every other
/// [`TableDefinition`]. No credentials are stored: remote tables connect
/// anonymously, so the remote instance must allow anonymous Flight SQL access.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RemoteTableDefinition {
    /// Local logical table name.
    pub name: String,
    /// gRPC endpoint of the remote Flight SQL server, e.g. `http://host:50051`.
    pub url: String,
    /// Table name on the remote instance.
    pub remote_table: String,
    /// Pinned output schema. An empty schema means "fetch from the remote when
    /// building the provider" (and the resolved schema is then pinned).
    pub schema: SchemaRef,
}

impl RemoteTableDefinition {
    fn connection(&self) -> RemoteConnection {
        RemoteConnection::new(self.url.clone())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "remote_table")]
impl TableDefinition for RemoteTableDefinition {
    async fn build_provider(
        &self,
        _context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let connection = self.connection();

        // Resolve the schema up front: `TableProvider::schema()` is sync, so it
        // must be pinned before the provider serves planning.
        let schema = if self.schema.fields().is_empty() {
            BeaconFlightSqlExecutor::fetch_schema(&connection, &self.remote_table).await?
        } else {
            self.schema.clone()
        };

        let executor = Arc::new(BeaconFlightSqlExecutor::new(connection));
        let provider = Arc::new(SQLFederationProvider::new(executor));
        let table_ref = RemoteTableRef::try_from(self.remote_table.as_str())?;

        // Pin the resolved schema into the definition the SQL table carries, so a
        // catalog round-trip persists the concrete schema (not the empty marker).
        let mut pinned = self.clone();
        pinned.schema = schema.clone();

        let sql_table: Arc<dyn SQLTable> =
            Arc::new(BeaconRemoteSqlTable::new(pinned, table_ref, schema));
        let source = Arc::new(SQLTableSource::new_with_table(provider, sql_table));

        Ok(Arc::new(FederatedTableProviderAdaptor::new(source)))
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}

/// A [`SQLTable`] that additionally carries the [`RemoteTableDefinition`], so
/// catalog persistence can recover it from the registered provider (see
/// [`super::remote_table_definition`]).
#[derive(Debug)]
pub struct BeaconRemoteSqlTable {
    definition: RemoteTableDefinition,
    table_ref: RemoteTableRef,
    schema: SchemaRef,
}

impl BeaconRemoteSqlTable {
    pub fn new(
        definition: RemoteTableDefinition,
        table_ref: RemoteTableRef,
        schema: SchemaRef,
    ) -> Self {
        Self {
            definition,
            table_ref,
            schema,
        }
    }

    pub fn definition(&self) -> &RemoteTableDefinition {
        &self.definition
    }
}

impl SQLTable for BeaconRemoteSqlTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_reference(&self) -> TableReference {
        self.table_ref.table_ref().clone()
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Convenience: an empty pinned schema marker meaning "infer from the remote".
pub fn unresolved_schema() -> SchemaRef {
    Arc::new(Schema::empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    /// A remote-table definition round-trips through the typetag `TableDefinition`
    /// trait, preserving its `remote_table` tag, endpoint, credentials, and schema.
    fn remote_table_definition_serde_round_trip() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, true),
        ]));
        let definition: Arc<dyn TableDefinition> = Arc::new(RemoteTableDefinition {
            name: "remote_obs".to_string(),
            url: "http://127.0.0.1:50051".to_string(),
            remote_table: "obs".to_string(),
            schema: schema.clone(),
        });

        let json = serde_json::to_value(&definition).expect("definition should serialize");
        assert_eq!(json["definition_type"], "remote_table");
        assert_eq!(json["url"], "http://127.0.0.1:50051");
        assert_eq!(json["remote_table"], "obs");
        // No credentials are ever persisted for a remote table.
        assert!(json.get("username").is_none());
        assert!(json.get("password").is_none());

        let restored: Arc<dyn TableDefinition> =
            serde_json::from_value(json).expect("definition should deserialize");
        assert_eq!(restored.table_name(), "remote_obs");
    }
}
