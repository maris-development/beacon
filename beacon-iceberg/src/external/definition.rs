//! Serializable definition for a `CREATE EXTERNAL TABLE ... STORED AS ICEBERG`
//! table, persisted to `table.json` and reloaded at startup like every other
//! [`TableDefinition`].

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

use super::loader::load_external_iceberg_table;
use super::provider::ExternalIcebergTable;

/// Persisted configuration for an external Iceberg table.
///
/// An Iceberg table's schema lives in its metadata, so (unlike the listing
/// `ExternalTableDefinition`) no schema is stored here — it is resolved from the
/// table's metadata when the provider is built.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalIcebergTableDefinition {
    /// Logical table name.
    pub name: String,
    /// Iceberg table location, relative to the datasets store (optionally with a
    /// `datasets://` scheme), e.g. `datasets://db/orders`.
    pub location: String,
}

impl ExternalIcebergTableDefinition {
    pub fn new(name: impl Into<String>, location: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            location: location.into(),
        }
    }
}

#[async_trait]
#[typetag::serde(name = "iceberg_external")]
impl TableDefinition for ExternalIcebergTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        _data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        // The runtime threads its config through the session as an extension (see
        // `Runtime::init_ctx`); recover the storage config to back the table.
        // Config is runtime-owned — no process-global accessor — so this works
        // even with multiple runtimes in one process.
        let storage = context
            .state()
            .config()
            .get_extension::<beacon_config::Config>()
            .context("beacon Config extension is not registered on the session")?
            .storage
            .clone();

        let table = load_external_iceberg_table(&storage, &self.location).await?;
        Ok(Arc::new(ExternalIcebergTable::new(self.clone(), table)))
    }

    fn table_name(&self) -> &str {
        &self.name
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_serde_round_trip() {
        let definition: Arc<dyn TableDefinition> =
            Arc::new(ExternalIcebergTableDefinition::new("orders", "datasets://db/orders"));

        let json = serde_json::to_value(&definition).expect("definition should serialize");
        assert_eq!(json["definition_type"], "iceberg_external");
        assert_eq!(json["location"], "datasets://db/orders");

        let restored: Arc<dyn TableDefinition> =
            serde_json::from_value(json).expect("definition should deserialize");
        assert_eq!(restored.table_name(), "orders");
    }
}
