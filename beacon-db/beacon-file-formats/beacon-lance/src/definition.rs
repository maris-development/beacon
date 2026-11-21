//! Persisted definition for a Lance table (managed or external).
//!
//! Serialized to `table.json` (via the typetag `TableDefinition` trait). Unlike
//! Iceberg there is no catalog: the dataset `location` is the source of truth,
//! so it is persisted directly. A managed table stores a `db://` URI into the
//! redb object store. An external table stores its `LOCATION` as the user wrote
//! it; `build_provider` resolves it with [`resolve_external_lance_uri`]. On
//! startup, `build_provider` reopens the dataset through the runtime's
//! warehouse; the Lance manifest — not the JSON — remains authoritative for the
//! schema.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::prelude::SessionContext;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::provider::LanceTable;
use crate::warehouse::LanceWarehouse;
use crate::{MissingDataset, open_external_lance_table};

/// Persisted configuration for a Lance table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceTableDefinition {
    /// Logical table name.
    pub name: String,
    /// Namespace the table lives under (e.g. `["beacon"]`).
    pub namespace: Vec<String>,
    /// The `db://` URI of a managed dataset, or the `LOCATION` of an external
    /// dataset as the user wrote it.
    pub location: String,
    /// The `CREATE TABLE` statement that made the table, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    /// `true` when the dataset lives outside Beacon and `DROP` keeps its files.
    #[serde(default)]
    pub external: bool,
    /// The `OPTIONS` of the `CREATE EXTERNAL TABLE` statement.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

impl LanceTableDefinition {
    pub fn new(
        name: impl Into<String>,
        namespace: Vec<String>,
        location: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            namespace,
            location: location.into(),
            definition: None,
            external: false,
            options: HashMap::new(),
        }
    }

    /// The dataset version from `options["version"]`, or `None` for the latest version.
    ///
    /// # Errors
    ///
    /// Returns an error for a version that is not a `u64`, or for an unknown option key.
    pub fn pinned_version(&self) -> anyhow::Result<Option<u64>> {
        let mut version = None;
        for (key, value) in &self.options {
            match key.as_str() {
                "version" => {
                    let parsed = value.trim().parse::<u64>().map_err(|_| {
                        anyhow::anyhow!(
                            "Invalid 'version' option '{value}' for a Lance table: \
                             expected a non-negative integer"
                        )
                    })?;
                    version = Some(parsed);
                }
                other => anyhow::bail!("Unknown option '{other}' for a Lance table"),
            }
        }
        Ok(version)
    }
}

fn warehouse(context: &SessionContext) -> anyhow::Result<Arc<LanceWarehouse>> {
    context
        .state()
        .config()
        .get_extension::<LanceWarehouse>()
        .ok_or_else(|| anyhow::anyhow!("Lance warehouse is unavailable in this session"))
}

/// Resolve the `LOCATION` of an external Lance table to the URI that Lance opens.
///
/// A relative path is in the datasets store, the same as for the other external
/// tables. A path with a scheme (`s3://bucket/a.lance`) uses the object store
/// that the session has for its scheme and authority, also when the session has
/// a datasets store. The store is registered in the Lance warehouse, so Lance
/// uses the same object store and credentials as DataFusion.
///
/// # Errors
///
/// Returns an error for a location with a glob character, and when the location
/// does not resolve to an object store.
pub fn resolve_external_lance_uri(
    context: &SessionContext,
    location: &str,
) -> anyhow::Result<String> {
    if location.contains(['*', '?', '[', ']']) {
        anyhow::bail!("A Lance location cannot contain a glob: '{location}'");
    }
    let state = context.state();
    let (store_url, path) = match schemed_url(location) {
        Some(url) => {
            let mut store_url = url.clone();
            store_url.set_path("");
            let path = Path::from_url_path(url.path())
                .map_err(|e| anyhow::anyhow!("Invalid Lance location '{location}': {e}"))?;
            anyhow::ensure!(
                path.parts().next().is_some(),
                "A Lance location must name a dataset: '{location}'"
            );
            (store_url, path)
        }
        None => {
            let factory = state
                .config()
                .get_extension::<ListingFactory>()
                .ok_or_else(|| anyhow::anyhow!("Listing factory is unavailable in this session"))?;
            let url = factory
                .parse_listing_table_url(&state, location)
                .map_err(|e| anyhow::anyhow!("Invalid Lance location '{location}': {e}"))?;
            let object_store = url.object_store();
            let store_url: &Url = object_store.as_ref();
            (store_url.clone(), url.prefix().clone())
        }
    };
    let store = state
        .runtime_env()
        .object_store_registry
        .get_store(&store_url)
        .map_err(|e| anyhow::anyhow!("No object store is registered for '{location}': {e}"))?;
    warehouse(context)?.register_store(&store_url, store);
    Ok(LanceWarehouse::external_uri(&store_url, &path))
}

/// The URL of a location with a scheme, or `None` for a path.
fn schemed_url(location: &str) -> Option<Url> {
    let url = Url::parse(location).ok()?;
    // A one-letter scheme is a Windows drive (`C:/data`), not a store.
    (url.scheme().len() > 1 && location.contains("://")).then_some(url)
}

#[async_trait]
#[typetag::serde(name = "lance")]
impl TableDefinition for LanceTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let warehouse = warehouse(&context)?;
        if self.external {
            let uri = resolve_external_lance_uri(&context, &self.location)?;
            let table = open_external_lance_table(warehouse, self.clone(), uri, None)
                .await
                .map_err(|error| match error.downcast_ref::<MissingDataset>() {
                    Some(_) => anyhow::anyhow!(
                        "Lance dataset '{}' of table '{}' is missing",
                        self.location,
                        self.name
                    ),
                    None => error,
                })?;
            return Ok(Arc::new(table));
        }
        let table = LanceTable::open(self.clone(), warehouse).await?;
        Ok(Arc::new(table))
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
    use std::collections::HashMap;

    use beacon_datafusion_ext::table_ext::TableDefinition;

    use super::LanceTableDefinition;

    /// Round-trip `json` through the `TableDefinition` typetag and back to the concrete type.
    fn through_typetag(json: &str) -> LanceTableDefinition {
        let boxed: Box<dyn TableDefinition> = serde_json::from_str(json).unwrap();
        let mut value = serde_json::to_value(&boxed).unwrap();
        let tag = value.as_object_mut().unwrap().remove("definition_type");
        assert_eq!(tag, Some(serde_json::json!("lance")));
        serde_json::from_value(value).unwrap()
    }

    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn old_definition_json_loads_as_managed() {
        let definition = through_typetag(
            r#"{"definition_type":"lance","name":"t","namespace":["beacon"],"location":"db:///lance/beacon/t.lance"}"#,
        );
        assert!(!definition.external);
        assert!(definition.options.is_empty());
        assert_eq!(definition.location, "db:///lance/beacon/t.lance");
    }

    #[test]
    fn external_definition_round_trips() {
        let mut definition =
            LanceTableDefinition::new("t", vec!["beacon".into()], "s3://b/t.lance");
        definition.external = true;
        definition.options = options(&[("version", "3")]);

        let boxed: Box<dyn TableDefinition> = Box::new(definition);
        let json = serde_json::to_string(&boxed).unwrap();
        let back = through_typetag(&json);
        assert!(back.external);
        assert_eq!(back.options, options(&[("version", "3")]));
    }

    #[test]
    fn managed_definition_omits_empty_options() {
        let definition = LanceTableDefinition::new("t", vec!["beacon".into()], "db:///t.lance");
        let value = serde_json::to_value(&definition).unwrap();
        assert!(value.get("options").is_none());
    }

    /// A registered table whose dataset another tool deleted names the table.
    #[tokio::test]
    async fn missing_dataset_of_a_registered_table() {
        use std::sync::Arc;

        use datafusion::prelude::{SessionConfig, SessionContext};

        let dir = tempfile::tempdir().unwrap();
        let store =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let warehouse = Arc::new(crate::LanceWarehouse::new(store));
        let context = Arc::new(SessionContext::new_with_config(
            SessionConfig::new().with_extension(warehouse),
        ));
        let location = url::Url::from_directory_path(dir.path())
            .unwrap()
            .join("gone.lance")
            .unwrap()
            .to_string();
        let mut definition = LanceTableDefinition::new("t", vec![], location.clone());
        definition.external = true;

        let error = definition.build_provider(context).await.err().unwrap();
        assert_eq!(
            error.to_string(),
            format!("Lance dataset '{location}' of table 't' is missing")
        );
    }

    #[test]
    fn pinned_version_parses_and_rejects() {
        let mut definition = LanceTableDefinition::new("t", vec![], "db:///t.lance");

        definition.options = options(&[("version", "3")]);
        assert_eq!(definition.pinned_version().unwrap(), Some(3));

        definition.options = options(&[]);
        assert_eq!(definition.pinned_version().unwrap(), None);

        definition.options = options(&[("version", "x")]);
        assert!(definition.pinned_version().is_err());

        definition.options = options(&[("foo", "1")]);
        assert_eq!(
            definition.pinned_version().unwrap_err().to_string(),
            "Unknown option 'foo' for a Lance table"
        );
    }
}
