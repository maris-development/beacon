use std::{any::Any, path::PathBuf, sync::Arc};

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{catalog::TableProvider, prelude::SessionContext};

use super::{TableExtension, TableExtensionError, TableExtensionResult};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct GeoSpatialExtension {
    pub table_longitude_column: String,
    pub table_latitude_column: String,
    pub table_ext: String,
}

const LONGITUDE_COLUMN_NAME: &str = "longitude";
const LATITUDE_COLUMN_NAME: &str = "longitude";

#[typetag::serde(name = "geo_spatial")]
impl TableExtension for GeoSpatialExtension {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_ext(&self) -> &str {
        &self.table_ext
    }

    fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
        origin_table_provider: Arc<dyn TableProvider>,
    ) -> TableExtensionResult<Arc<dyn TableProvider>> {
        
    }
}
