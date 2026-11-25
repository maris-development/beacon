//! The persisted `CREATE EXTERNAL TABLE ... STORED AS ERDDAP` definition.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;

use crate::info::DatasetInfo;
use crate::provider::ErddapTable;

/// What create time learned about the dataset. Persisted, so a restart needs no network.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ResolvedDataset {
    /// The parsed `info/<id>/index.json`.
    pub info: DatasetInfo,
    /// The table schema derived from `info`.
    pub schema: SchemaRef,
}

/// The persisted configuration of an ERDDAP external table.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ErddapTableDefinition {
    /// The logical table name.
    pub name: String,
    /// The dataset URL as given in `LOCATION`.
    pub location: String,
    /// The table `OPTIONS`, as DataFusion passes them.
    pub options: HashMap<String, String>,
    /// Original `CREATE EXTERNAL TABLE` SQL, if available.
    pub definition: Option<String>,
    /// `None` until the provider is first built.
    pub resolved: Option<ResolvedDataset>,
}

#[async_trait::async_trait]
#[typetag::serde(name = "erddap_table")]
impl TableDefinition for ErddapTableDefinition {
    async fn build_provider(
        &self,
        _context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(ErddapTable::try_new(self.clone()).await?))
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_with_its_tag() {
        let info = DatasetInfo::parse(&crate::fixture::test_file("tabledap_info.json")).unwrap();
        let schema = info.tabledap_schema().unwrap();
        let definition: Arc<dyn TableDefinition> = Arc::new(ErddapTableDefinition {
            name: "bottles".into(),
            location: "https://h/erddap/tabledap/x".into(),
            options: HashMap::new(),
            definition: None,
            resolved: Some(ResolvedDataset {
                info,
                schema: schema.clone(),
            }),
        });
        let json = serde_json::to_value(&definition).unwrap();
        assert_eq!(json["definition_type"], "erddap_table");
        let restored: Arc<dyn TableDefinition> = serde_json::from_value(json).unwrap();
        assert_eq!(restored.table_name(), "bottles");
        let json = serde_json::to_value(&restored).unwrap();
        let restored: ErddapTableDefinition = serde_json::from_value(json).unwrap();
        assert_eq!(restored.resolved.unwrap().schema, schema);
    }
}
