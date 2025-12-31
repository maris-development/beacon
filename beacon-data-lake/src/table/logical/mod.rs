use std::sync::Arc;

use beacon_common::listing_url::parse_listing_table_url;
use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

use crate::{
    files::collection::FileCollection,
    table::{error::TableError, table_formats::TableFileFormat},
};

/// A representation of a logical table configuration.
///
/// This structure holds the name of the table, the file paths that
/// represent the underlying data sources, and the file format.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct LogicalTable {
    #[serde(alias = "paths")]
    pub glob_paths: Vec<String>,
    /// The file format for this logical table.
    #[serde(flatten)]
    pub file_format: Arc<dyn TableFileFormat>,
}

impl LogicalTable {
    /// Creates a `TableProvider` instance for the logical table.
    ///
    /// # Parameters
    /// - `session_context`: The session context used to retrieve the session state.
    ///
    /// # Returns
    /// Returns an `Arc` to a `TableProvider` if successful.
    pub async fn table_provider(
        &self,
        data_directory_store_url: &ObjectStoreUrl,
        session_context: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        // Retrieve the session state from the session context.
        let session_state = session_context.state();

        let mut table_urls = Vec::new();

        // Construct table URLs based on the paths provided in the logical table.
        for glob_path in &self.glob_paths {
            table_urls.push(parse_listing_table_url(
                data_directory_store_url,
                glob_path,
            )?);
        }

        tracing::debug!(
            "Creating file collection for logical table with URLs: {:?}",
            table_urls
        );

        let file_ext = self.file_format.file_ext();
        let file_format_factory = session_state.get_file_format_factory(&file_ext).ok_or(
            TableError::GenericTableError(format!(
                "Encountered unsupported file format: {} while creating table.",
                file_ext
            )),
        )?;

        // Create the data source with the given file format and table URLs.
        let source = FileCollection::new(
            &session_state,
            self.file_format
                .file_format()
                .unwrap_or(file_format_factory.default()),
            table_urls,
        )
        .await
        .map_err(|e| {
            TableError::GenericTableError(format!("Failed to create file collection: {}", e))
        })?;

        Ok(Arc::new(source))
    }
}
