use std::{any::Any, path::PathBuf, sync::Arc};

use arrow::datatypes::{DataType, TimeUnit};
use beacon_common::rename_table_provider::RenameTableProvider;
use datafusion::{catalog::TableProvider, prelude::SessionContext};

use super::{TableExtension, TableExtensionError, TableExtensionResult};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct TemporalExtension {
    pub table_time_column: String,
}

const TIME_COLUMN_NAME: &str = "time";

#[typetag::serde(name = "temporal")]
impl TableExtension for TemporalExtension {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_provider(
        &self,
        _table_directory: PathBuf,
        _session_ctx: Arc<SessionContext>,
        origin_table_provider: Arc<dyn TableProvider>,
    ) -> TableExtensionResult<Arc<dyn TableProvider>> {
        let origin_schema = origin_table_provider.schema();
        let renamed_schema = RenameTableProvider::rename_field(
            &origin_schema,
            &self.table_time_column,
            TIME_COLUMN_NAME,
        )
        .map_err(|e| TableExtensionError::from(format!("Failed to rename field: {}", e)))?;

        let arc_schema = Arc::new(renamed_schema);
        let renamed_table_provider = RenameTableProvider::new(origin_table_provider, arc_schema)
            .map_err(|e| {
                TableExtensionError::from(format!("Failed to create renamed table provider: {}", e))
            })?;

        Ok(Arc::new(renamed_table_provider))
    }
}
