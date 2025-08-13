use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, datasource::listing::ListingTableUrl, prelude::SessionContext,
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
    glob_paths: Vec<String>,
    /// The file format for this logical table.
    #[serde(flatten)]
    file_format: Arc<dyn TableFileFormat>,
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
        base_path: &object_store::path::Path,
        session_context: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        // Retrieve the session state from the session context.
        let session_state = session_context.state();

        let mut table_urls = Vec::new();

        // Construct table URLs based on the paths provided in the logical table.
        for path in &self.glob_paths {
            let table_url =
                ListingTableUrl::parse(base_path.child(path.as_str())).map_err(|e| {
                    TableError::GenericTableError(format!("Failed to parse table URL: {}", e))
                })?;
            table_urls.push(table_url);
        }

        // Create the data source with the given file format and table URLs.
        let source =
            FileCollection::new(&session_state, self.file_format.file_format(), table_urls)
                .await
                .map_err(|e| {
                    TableError::GenericTableError(format!(
                        "Failed to create file collection: {}",
                        e
                    ))
                })?;

        Ok(Arc::new(source))
    }
}
