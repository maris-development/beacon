//! Serializable definition for a `CREATE EXTERNAL TABLE ... STORED AS ICECHUNK`
//! table, persisted to `table.json` and reloaded at startup like every other
//! [`TableDefinition`].

use std::collections::HashMap;
use std::sync::Arc;

use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;

use crate::provider::IcechunkTable;

/// Persisted configuration for an Icechunk external table.
///
/// An Icechunk repository carries its own metadata, so (unlike the listing
/// `ExternalTableDefinition`) no schema is stored here — it is read from the
/// repository when the provider is built.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IcechunkTableDefinition {
    /// Logical table name.
    pub name: String,
    /// Location of the repository, e.g. `argo/repo` (relative to the datasets
    /// store) or `s3://bucket/argo/repo`.
    pub location: String,
    /// Table OPTIONS: `branch` / `tag` / `snapshot` select the version,
    /// `read_dimensions` narrows the variables that are read.
    pub options: HashMap<String, String>,
    /// Original `CREATE EXTERNAL TABLE` SQL, if available.
    pub definition: Option<String>,
}

#[async_trait::async_trait]
#[typetag::serde(name = "icechunk_table")]
impl TableDefinition for IcechunkTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let table = IcechunkTable::try_new(&context.state(), self.clone()).await?;
        Ok(Arc::new(table))
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The definition round-trips through the typetag `TableDefinition` trait,
    /// preserving its `icechunk_table` tag so `table.json` reloads correctly.
    #[test]
    fn icechunk_table_definition_serde_round_trip() {
        let definition: Arc<dyn TableDefinition> = Arc::new(IcechunkTableDefinition {
            name: "argo_icechunk".to_string(),
            location: "argo/repo".to_string(),
            options: HashMap::from([("branch".to_string(), "dev".to_string())]),
            definition: Some(
                "CREATE EXTERNAL TABLE argo_icechunk STORED AS ICECHUNK ...".to_string(),
            ),
        });

        let json = serde_json::to_value(&definition).expect("definition should serialize");
        assert_eq!(json["definition_type"], "icechunk_table");
        assert_eq!(json["location"], "argo/repo");
        assert_eq!(json["options"]["branch"], "dev");

        let restored: Arc<dyn TableDefinition> =
            serde_json::from_value(json).expect("definition should deserialize");
        assert_eq!(restored.table_name(), "argo_icechunk");
    }
}
