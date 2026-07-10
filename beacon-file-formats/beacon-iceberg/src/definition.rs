//! Persisted definition for a beacon-managed Iceberg table.
//!
//! Serialized to `table.json` (via the typetag `TableDefinition` trait) as a thin
//! pointer into the Iceberg catalog: just the namespace + table name. On startup,
//! `build_provider` reloads the live table from the shared catalog so the Iceberg
//! metadata — not the JSON — remains the source of truth for the schema.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use serde::{Deserialize, Serialize};

use crate::catalog::get_catalog;
use crate::provider::IcebergTable;

/// Persisted configuration for an Iceberg-backed managed table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTableDefinition {
    /// Logical table name (also the Iceberg table name).
    pub name: String,
    /// Iceberg namespace the table lives under (e.g. `["beacon"]`).
    pub namespace: Vec<String>,
}

impl IcebergTableDefinition {
    pub fn new(name: impl Into<String>, namespace: Vec<String>) -> Self {
        Self {
            name: name.into(),
            namespace,
        }
    }

    /// The Iceberg catalog identifier for this table.
    pub fn identifier(&self) -> Identifier {
        Identifier::new(&self.namespace, &self.name)
    }
}

#[async_trait]
#[typetag::serde(name = "iceberg")]
impl TableDefinition for IcebergTableDefinition {
    async fn build_provider(
        &self,
        _context: Arc<SessionContext>,
        _data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let catalog = get_catalog()?;
        let identifier = self.identifier();
        let tabular = catalog
            .clone()
            .load_tabular(&identifier)
            .await
            .map_err(|error| {
                anyhow::anyhow!("Failed to load Iceberg table '{}': {error}", self.name)
            })?;

        let table = match tabular {
            Tabular::Table(table) => table,
            _ => anyhow::bail!(
                "Iceberg identifier '{}' does not refer to a table",
                self.name
            ),
        };

        Ok(Arc::new(IcebergTable::new(
            self.clone(),
            DataFusionTable::from(table),
        )))
    }

    fn table_name(&self) -> &str {
        &self.name
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}
