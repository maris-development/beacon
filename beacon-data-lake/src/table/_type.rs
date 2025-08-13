use std::sync::Arc;

use datafusion::catalog::TableProvider;
use object_store::ObjectStore;

use crate::table::{error::TableError, logical::LogicalTable, preset::PresetTable};

/// Enum representing different types of tables.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TableType {
    Logical(LogicalTable),
    Preset(PresetTable),
}

impl TableType {
    pub fn table_provider(
        &self,
        object_store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        todo!()
    }
}
