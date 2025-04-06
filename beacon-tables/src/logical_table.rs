use std::{path::PathBuf, sync::Arc};

use beacon_sources::DataSource;
use datafusion::{
    catalog::TableProvider, datasource::listing::ListingTableUrl, error::DataFusionError,
    prelude::SessionContext,
};

use crate::{file_format::FileFormat, LogicalTableProvider};

/// Errors that can occur while working with a logical table.
#[derive(Debug, thiserror::Error)]
pub enum LogicalTableError {
    /// An error occurred while creating the table provider from the logical table.
    #[error("Failed to create table provider from logical table: {0}")]
    TableProviderCreationError(#[from] anyhow::Error),
    /// An error occurred while parsing the table URL.
    #[error("Failed to parse table URL: {0}")]
    TableUrlParseError(#[from] DataFusionError),
}

/// A representation of a logical table configuration.
///
/// This structure holds the name of the table, the file paths that
/// represent the underlying data sources, and the file format.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct LogicalTable {
    table_name: String,
    /// List of data paths for the table.
    paths: Vec<String>,
    /// The file format for this logical table.
    #[serde(flatten)]
    file_format: Arc<dyn FileFormat>,
}

#[typetag::serde(name = "logical_table")]
#[async_trait::async_trait]
impl LogicalTableProvider for LogicalTable {
    /// Returns the name of the table.
    fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Creates the logical table.
    ///
    /// # Parameters
    /// - `_directory`: The directory containing the table data.
    /// - `_session_ctx`: The session context to use when creating the table.
    ///
    /// # Returns
    /// This method currently returns `Ok(())`.
    async fn create(
        &self,
        _directory: PathBuf,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<(), LogicalTableError> {
        Ok(())
    }

    /// Creates a `TableProvider` instance for the logical table.
    ///
    /// # Parameters
    /// - `session_context`: The session context used to retrieve the session state.
    ///
    /// # Returns
    /// Returns an `Arc` to a `TableProvider` if successful.
    async fn table_provider(
        &self,
        session_context: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, LogicalTableError> {
        // Retrieve the session state from the session context.
        let session_state = session_context.state();
        let mut table_urls = Vec::new();

        // Construct table URLs based on the paths provided in the logical table.
        for path in &self.paths {
            let table_url = ListingTableUrl::parse(format!(
                "/{}/{}",
                beacon_config::DATASETS_DIR_PREFIX.to_string(),
                path
            ))
            .map_err(LogicalTableError::TableUrlParseError)?;
            table_urls.push(table_url);
        }

        // Create the data source with the given file format and table URLs.
        let source = DataSource::new(&session_state, self.file_format.file_format(), table_urls)
            .await
            .map_err(|e| LogicalTableError::TableProviderCreationError(e.into()))?;

        Ok(Arc::new(source))
    }
}
