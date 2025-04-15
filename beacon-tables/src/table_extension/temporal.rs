use std::{any::Any, path::PathBuf, sync::Arc};

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{catalog::TableProvider, prelude::SessionContext};

use super::{TableExtension, TableExtensionError, TableExtensionResult};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct TemporalExtension {
    pub table_time_column: String,
    pub table_ext: String,
}

const TIME_COLUMN_NAME: &str = "time";
const TIME_COLUMN_ARROW_DTYPE: DataType = DataType::Timestamp(TimeUnit::Millisecond, None);

#[typetag::serde(name = "temporal")]
impl TableExtension for TemporalExtension {
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
        todo!()
    }
}
