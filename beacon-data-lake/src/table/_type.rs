use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
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
        table_directory_store_url: ObjectStoreUrl,
        table_directory_prefix: object_store::path::Path,
        data_directory_store_url: ObjectStoreUrl,
        data_directory_prefix: object_store::path::Path,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        match self {
            TableType::Logical(logical_table) => {
                Box::pin(logical_table.table_provider(
                    &data_directory_store_url,
                    &data_directory_prefix,
                    session_ctx,
                ))
                .await
            }
            TableType::Preset(preset_table) => {
                Box::pin(preset_table.table_provider(
                    table_directory_store_url,
                    table_directory_prefix,
                    data_directory_store_url,
                    data_directory_prefix,
                    session_ctx,
                ))
                .await
            }
        }
    }
}
