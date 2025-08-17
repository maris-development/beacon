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

        println!(
            "Object Store Before GLOB: {:?}",
            session_state
                .runtime_env()
                .object_store(data_directory_store_url)
        );
        // Construct table URLs based on the paths provided in the logical table.
        for path in &self.glob_paths {
            let mut glob_path_parts = parts.clone();
            glob_path_parts.push(path.as_str().into());

            // println!("Constructing table URL with parts: {:?}", glob_path_parts);
            // let full_path = format!(
            //     "{}{}",
            //     data_directory_store_url,
            //     object_store::path::Path::from_iter(glob_path_parts)
            // );
            // println!("Full path for logical table: {}", full_path);
            // ListingTableUrl::
            let url = Url::parse("file:///datasets/bgc/").unwrap();
            // ListingTableUrl::try_new(url, glob)
            let table_url =
                ListingTableUrl::try_new(url, Some(glob::Pattern::new("*.parquet").unwrap()))
                    .map_err(|e| {
                        TableError::GenericTableError(format!("Failed to parse table URL: {}", e))
                    })?;
            table_urls.push(table_url);
        }

        println!(
            "Creating file collection for logical table with URLs: {:?}",
            table_urls
        );

        println!(
            "Object Store Before FileCollection: {:?}",
            session_state
                .runtime_env()
                .object_store(data_directory_store_url)
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
