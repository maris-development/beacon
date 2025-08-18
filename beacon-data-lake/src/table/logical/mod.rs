use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, datasource::listing::ListingTableUrl,
    execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use object_store::path::PathPart;
use url::Url;

use crate::{
    files::collection::FileCollection,
    table::{error::TableError, table_formats::TableFileFormat},
    util::split_glob,
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
        data_directory_store_url: &ObjectStoreUrl,
        data_directory_prefix: &object_store::path::Path,
        session_context: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        // Retrieve the session state from the session context.
        let session_state = session_context.state();

        let mut table_urls = Vec::new();
        let parts: Vec<PathPart<'static>> = data_directory_prefix
            .parts()
            .map(|part| part.as_ref().to_string().into())
            .collect::<Vec<_>>();

        // Construct table URLs based on the paths provided in the logical table.
        for path in &self.glob_paths {
            // Check if path contains glob expression
            if let Some((base, pattern)) = split_glob(path) {
                let pattern = glob::Pattern::new(&pattern).map_err(|e| {
                    TableError::GenericTableError(format!("Failed to parse glob pattern: {}", e))
                })?;
                let mut full_path = format!(
                    "{}{}",
                    data_directory_store_url,
                    object_store::path::Path::from_iter(parts.clone().into_iter()),
                );
                if base.components().next().is_some() {
                    full_path.push_str(format!("/{}", base.as_os_str().to_string_lossy()).as_str());
                }
                if !full_path.ends_with('/') {
                    full_path.push('/');
                }

                let url = Url::parse(&full_path).map_err(|e| {
                    TableError::GenericTableError(format!("Failed to parse URL: {}", e))
                })?;

                let table_url = ListingTableUrl::try_new(url, Some(pattern)).map_err(|e| {
                    TableError::GenericTableError(format!("Failed to create table URL: {}", e))
                })?;

                table_urls.push(table_url);
            } else {
                let mut full_path = format!(
                    "{}{}",
                    data_directory_store_url,
                    object_store::path::Path::from_iter(parts.clone().into_iter()),
                );

                // If full path ends with '/' then just append the path, otherwise append without a '/'
                if full_path.ends_with('/') {
                    full_path.push_str(path);
                } else {
                    full_path.push_str(format!("/{}", path).as_str());
                }

                let url = Url::parse(&full_path).map_err(|e| {
                    TableError::GenericTableError(format!("Failed to parse URL: {}", e))
                })?;

                let table_url = ListingTableUrl::try_new(url, None).map_err(|e| {
                    TableError::GenericTableError(format!("Failed to create table URL: {}", e))
                })?;

                table_urls.push(table_url);
            }
        }

        tracing::debug!(
            "Creating file collection for logical table with URLs: {:?}",
            table_urls
        );

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
