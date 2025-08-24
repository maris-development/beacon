use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use object_store::path::Path;

use crate::table::{
    error::TableError,
    preset::{PresetColumnMapping, PresetTable},
};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GeoSpatialTable {
    #[serde(flatten)]
    preset_table: PresetTable,
    longitude_column: PresetColumnMapping,
    latitude_column: PresetColumnMapping,
}

impl GeoSpatialTable {
    pub async fn create(
        &self,
        table_directory: object_store::path::Path,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), TableError> {
        self.preset_table.create(table_directory, session_ctx).await
    }
    pub(crate) async fn table_provider(
        &self,
        table_directory_store_url: ObjectStoreUrl,
        table_directory_prefix: Path,
        data_directory_store_url: ObjectStoreUrl,
        data_directory_prefix: Path,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        todo!()
    }
}
