use std::{fmt::Debug, sync::Arc};

use datafusion::{
    common::GetExt,
    datasource::file_format::{FileFormat, FileFormatFactory},
};
use object_store::ObjectStore;
use zarrs_object_store::AsyncObjectStore;

pub struct ZarrFormatFactory {
    zarr_object_store: AsyncObjectStore<Arc<dyn ObjectStore>>,
}

impl Debug for ZarrFormatFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZarrFormatFactory").finish()
    }
}

impl ZarrFormatFactory {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            zarr_object_store: AsyncObjectStore::new(object_store),
        }
    }
}

impl GetExt for ZarrFormatFactory {
    fn get_ext(&self) -> String {
        "zarr".to_string()
    }
}

impl FileFormatFactory for ZarrFormatFactory {
    fn create(
        &self,
        state: &dyn datafusion::catalog::Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn FileFormat>> {
        todo!()
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct ZarrFormat {
    zarr_object_store: AsyncObjectStore<Arc<dyn ObjectStore>>,
}

// impl FileFormat for ZarrFormat {
//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }
// }

#[cfg(test)]
mod tests {
    use datafusion::datasource::listing::ListingTableUrl;
    use futures::StreamExt;

    use super::*;

    #[test]
    fn test_name() {
        let test_path = object_store::path::Path::parse("bucket/key.zarr").unwrap();

        println!("Path: {:?}", test_path);
    }

    #[tokio::test]
    async fn test_listing_table() {
        let object_store =
            object_store::local::LocalFileSystem::new_with_prefix("./test_files").unwrap();

        let url = ListingTableUrl::try_new(
            "file:///".try_into().unwrap(),
            Some(glob::Pattern::new("gridded-example.zarr").unwrap()),
        )
        .unwrap();
        println!("URL: {:?}", url);

        let session = datafusion::execution::context::SessionContext::new();
        let state = session.state();
        let mut files = url
            .list_all_files(&state, &object_store, "zarr.json")
            .await
            .unwrap();

        while let Some(file) = files.next().await {
            println!("File: {:?}", file);
        }

        let url =
            ListingTableUrl::try_new("file:///gridded-example.zarr/".try_into().unwrap(), None)
                .unwrap();
        println!("URL: {:?}", url);

        let session = datafusion::execution::context::SessionContext::new();
        let state = session.state();
        let mut files = url
            .list_all_files(&state, &object_store, "zarr.json")
            .await
            .unwrap();

        while let Some(file) = files.next().await {
            println!("File: {:?}", file);
        }
    }
}
