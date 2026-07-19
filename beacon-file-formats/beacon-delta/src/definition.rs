//! Serializable definition for a `CREATE EXTERNAL TABLE ... STORED AS DELTA`
//! table, persisted to `table.json` and reloaded at startup like every other
//! [`TableDefinition`].

use std::collections::HashMap;
use std::sync::Arc;

use beacon_datafusion_ext::{listing_factory::ListingFactory, table_ext::TableDefinition};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;

use crate::provider::{open_delta_provider, TimeTravel};

/// Persisted configuration for a Delta Lake external table.
///
/// A Delta table's schema lives in its transaction log, so (unlike the listing
/// `ExternalTableDefinition`) no schema is stored here — it is resolved from the
/// log when the provider is built.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DeltaTableDefinition {
    /// Logical table name.
    pub name: String,
    /// Location of the Delta table, e.g. `file:///argo/delta-tbl` or `https://s3.amazonaws.com/bucket-name/path/to/root` or 'test/delta-tbl'.
    pub location: String,
    /// Table OPTIONS, including `version` / `timestamp` for time travel.
    pub options: HashMap<String, String>,
    /// Original `CREATE EXTERNAL TABLE` SQL, if available.
    pub definition: Option<String>,
}

#[async_trait::async_trait]
#[typetag::serde(name = "delta_table")]
impl TableDefinition for DeltaTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let state = context.state();
        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .expect("Delta table requires a ListingFactory extension");
        let store_url = listing_factory
            .parse_to_store(&state, &self.location)
            .ok_or(anyhow::anyhow!(
                "Delta table requires a resolvable object store for location {}",
                self.location
            ))?;
        // The datasets store is resolved from the session's object-store registry by
        // `store_url` inside `open_delta_provider`; nothing extra is needed here.
        let time_travel = TimeTravel::from_options(&self.options)?;
        let store = state.runtime_env().object_store(store_url)?;

        let provider = open_delta_provider(
            context.clone(),
            store.clone(),
            &self.location,
            time_travel.clone(),
        )
        .await?;

        // Wrap so the catalog can recover this definition from the registered
        // provider when persisting/reloading `table.json`. The store + time-travel
        // target are kept so reads/writes re-open at the latest version.
        Ok(Arc::new(crate::wrapper::BeaconDeltaTable::new(
            provider,
            self.clone(),
            time_travel,
        )))
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// The definition round-trips through the typetag `TableDefinition` trait,
    /// preserving its `delta_table` tag so `table.json` reloads correctly.
    fn delta_table_definition_serde_round_trip() {
        let definition: Arc<dyn TableDefinition> = Arc::new(DeltaTableDefinition {
            name: "argo_delta".to_string(),
            location: "datasets://argo/delta-tbl".to_string(),
            options: HashMap::from([("version".to_string(), "2".to_string())]),
            definition: Some("CREATE EXTERNAL TABLE argo_delta STORED AS DELTA ...".to_string()),
        });

        let json = serde_json::to_value(&definition).expect("definition should serialize");
        assert_eq!(json["definition_type"], "delta_table");
        assert_eq!(json["location"], "datasets://argo/delta-tbl");

        let restored: Arc<dyn TableDefinition> =
            serde_json::from_value(json).expect("definition should deserialize");
        assert_eq!(restored.table_name(), "argo_delta");
    }
}
