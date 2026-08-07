//! Serializable definition for a `CREATE EXTERNAL TABLE ... STORED AS ICEBERG`
//! table, persisted to `table.json` and reloaded at startup like every other
//! [`TableDefinition`].

use std::collections::HashMap;
use std::sync::Arc;

use beacon_datafusion_ext::{listing_factory::ListingFactory, table_ext::TableDefinition};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;

use crate::provider::{location_to_prefix, open_iceberg_table, snapshot_id_from_options};
use crate::wrapper::BeaconIcebergTable;

/// Persisted configuration for an Iceberg external table.
///
/// An Iceberg table's schema lives in its metadata file, so (unlike the listing
/// `ExternalTableDefinition`) no schema is stored here — it is read from the
/// table each time the provider is built, and re-read as the table changes.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IcebergTableDefinition {
    /// Logical table name.
    pub name: String,
    /// Location of the Iceberg table *directory*, e.g. `datasets://argo/obs` or
    /// `s3://bucket/warehouse/obs`. A relative path resolves under the datasets
    /// root.
    pub location: String,
    /// Table OPTIONS, including `snapshot_id` for time travel.
    pub options: HashMap<String, String>,
    /// Original `CREATE EXTERNAL TABLE` SQL, if available.
    pub definition: Option<String>,
}

#[async_trait::async_trait]
#[typetag::serde(name = "iceberg_table")]
impl TableDefinition for IcebergTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let state = context.state();
        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .expect("Iceberg table requires a ListingFactory extension");
        let store_url = listing_factory
            .parse_to_store(&state, &self.location)
            .ok_or(anyhow::anyhow!(
                "Iceberg table requires a resolvable object store for location {}",
                self.location
            ))?;
        let store = state.runtime_env().object_store(store_url)?;

        let snapshot_id = snapshot_id_from_options(&self.options)?;
        let prefix = location_to_prefix(&self.location)?;
        let opened = open_iceberg_table(store.clone(), &self.location, None, snapshot_id).await?;

        // Wrap so the catalog can recover this definition from the registered
        // provider when persisting/reloading `table.json`, and so later queries
        // follow the table as another system writes it.
        Ok(Arc::new(BeaconIcebergTable::new(
            store,
            prefix,
            self.clone(),
            snapshot_id,
            opened,
            tokio::runtime::Handle::current(),
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
    /// preserving its `iceberg_table` tag so `table.json` reloads correctly.
    fn iceberg_table_definition_serde_round_trip() {
        let definition: Arc<dyn TableDefinition> = Arc::new(IcebergTableDefinition {
            name: "argo_iceberg".to_string(),
            location: "datasets://argo/obs".to_string(),
            options: HashMap::from([("snapshot_id".to_string(), "12".to_string())]),
            definition: Some(
                "CREATE EXTERNAL TABLE argo_iceberg STORED AS ICEBERG ...".to_string(),
            ),
        });

        let json = serde_json::to_value(&definition).expect("definition should serialize");
        assert_eq!(json["definition_type"], "iceberg_table");
        assert_eq!(json["location"], "datasets://argo/obs");

        let restored: Arc<dyn TableDefinition> =
            serde_json::from_value(json).expect("definition should deserialize");
        assert_eq!(restored.table_name(), "argo_iceberg");
    }
}
