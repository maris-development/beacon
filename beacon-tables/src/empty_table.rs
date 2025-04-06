use std::{path::PathBuf, sync::Arc};

use arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;

use crate::LogicalTableProvider;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EmptyTable {
    table_name: String,
}

impl EmptyTable {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}

#[typetag::serde(name = "empty")]
#[async_trait::async_trait]
impl LogicalTableProvider for EmptyTable {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    async fn create(
        &self,
        _directory: PathBuf,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<(), crate::LogicalTableError> {
        Ok(())
    }

    async fn table_provider(
        &self,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, crate::LogicalTableError> {
        Ok(Arc::new(datafusion::datasource::empty::EmptyTable::new(
            Arc::new(Schema::empty()),
        )))
    }
}
