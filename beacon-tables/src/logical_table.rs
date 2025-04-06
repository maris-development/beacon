use std::{path::PathBuf, sync::Arc};

use beacon_sources::DataSource;
use datafusion::{
    catalog::TableProvider, datasource::listing::ListingTableUrl, error::DataFusionError,
    execution::SessionState, prelude::SessionContext,
};

use crate::{file_format::FileFormat, LogicalTableProvider};

#[derive(Debug, thiserror::Error)]
pub enum LogicalTableError {
    #[error("Failed to create table provider from logical table: {0}")]
    TableProviderCreationError(#[from] anyhow::Error),
    #[error("Failed to parse table URL: {0}")]
    TableUrlParseError(#[from] DataFusionError),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct LogicalTable {
    table_name: String,
    paths: Vec<String>,
    #[serde(flatten)]
    file_format: Arc<dyn FileFormat>,
}

#[typetag::serde(name = "logical_table")]
#[async_trait::async_trait]
impl LogicalTableProvider for LogicalTable {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    async fn create(
        &self,
        directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), LogicalTableError> {
        Ok(())
    }

    async fn table_provider(
        &self,
        session_context: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, LogicalTableError> {
        let session_state = session_context.state();
        let mut table_urls = vec![];

        for path in &self.paths {
            let table_url = ListingTableUrl::parse(format!(
                "/{}/{}",
                beacon_config::DATASETS_DIR_PREFIX.to_string(),
                path
            ))
            .map_err(LogicalTableError::TableUrlParseError)?;
            table_urls.push(table_url);
        }

        let source = DataSource::new(&session_state, self.file_format.file_format(), table_urls)
            .await
            .map_err(|e| LogicalTableError::TableProviderCreationError(e.into()))?;

        Ok(Arc::new(source))
    }
}
