//! Persisted definition for a beacon-managed Lance table.
//!
//! Serialized to `table.json` (via the typetag `TableDefinition` trait). Unlike
//! Iceberg there is no catalog: the dataset `location` (a `db://` URI
//! into the redb object store) is the source of truth, so it is persisted
//! directly. On startup, `build_provider` reopens the dataset at that URI through
//! the runtime's warehouse; the Lance manifest — not the JSON — remains
//! authoritative for the schema.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

use crate::provider::LanceTable;
use crate::warehouse::LanceWarehouse;

/// Persisted configuration for a Lance-backed managed table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceTableDefinition {
    /// Logical table name.
    pub name: String,
    /// Namespace the table lives under (e.g. `["beacon"]`).
    pub namespace: Vec<String>,
    /// The dataset's `db://` URI into the redb object store.
    pub location: String,
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
        }
    }
}

#[async_trait]
#[typetag::serde(name = "lance")]
impl TableDefinition for LanceTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let warehouse = context
            .state()
            .config()
            .get_extension::<LanceWarehouse>()
            .ok_or_else(|| anyhow::anyhow!("Lance warehouse is unavailable in this session"))?;
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
