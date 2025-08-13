use std::sync::Arc;

use datafusion::{catalog::TableProvider, prelude::SessionContext};
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
    pub async fn table_provider(
        &self,
        session_ctx: Arc<SessionContext>,
        object_store: Arc<dyn ObjectStore>,
        table_directory: object_store::path::Path,
        data_directory: object_store::path::Path,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        match self {
            TableType::Logical(logical_table) => {
                Box::pin(logical_table.table_provider(&data_directory, session_ctx)).await
            }
            TableType::Preset(preset_table) => {
                Box::pin(preset_table.table_provider(
                    table_directory,
                    data_directory,
                    object_store,
                    session_ctx,
                ))
                .await
            }
        }
    }
}
