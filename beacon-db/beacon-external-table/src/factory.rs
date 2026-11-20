//! The `CREATE EXTERNAL TABLE` entry point.

use std::sync::Arc;

use datafusion::{
    catalog::{Session, TableProvider, TableProviderFactory},
    common::Result,
    logical_expr::CreateExternalTable,
};

use crate::ExternalTableBuilder;

/// Creates an [`ExternalTable`](crate::ExternalTable) for a `CREATE EXTERNAL TABLE` statement.
#[derive(Debug, Default)]
pub struct ExternalTableFactory;

#[async_trait::async_trait]
impl TableProviderFactory for ExternalTableFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let table = ExternalTableBuilder::from_create(cmd)?.build(state).await?;
        Ok(Arc::new(table))
    }
}
