use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};
use object_store::{ObjectStore, local::LocalFileSystem};

pub mod files;
pub mod table;
pub mod util;

#[derive(Debug)]
pub struct Config {
    read_only: bool,
}

#[derive(Debug)]
pub struct DataLake {
    engine: Arc<dyn ObjectStore>,
    local_temp_fs: Arc<LocalFileSystem>,
    config: Config,

    // Map of tables
    tables: parking_lot::Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl DataLake {
    pub fn new(engine: Arc<dyn ObjectStore>, config: Config) -> Self {
        // Create tmp object store for storing temp files.
        let local_temp_fs = Arc::new(
            LocalFileSystem::new_with_prefix("./tmp")
                .expect("Failed to create local temp file system. Is the tmp dir set correctly?"),
        );

        let tables = parking_lot::Mutex::new(HashMap::new());

        Self {
            engine,
            local_temp_fs,
            config,
            tables,
        }
    }

    async fn init_tables(
        object_store: Arc<dyn ObjectStore>,
        table_directory: object_store::path::Path,
        data_directory: object_store::path::Path,
    ) -> HashMap<String, Arc<dyn TableProvider>> {
        let mut tables = HashMap::new();
        // Iterate through the table directory for each 'table.json'
        // for entry in object_store.list(&table_directory).await.unwrap() {
        //     if entry.name().ends_with("table.json") {
        //         let table_name = entry.name().strip_suffix("table.json").unwrap().to_string();
        //         let table_path = table_directory.join(entry.name());
        //         let table = Arc::new(TableProvider::new(table_path));
        //         tables.insert(table_name, table);
        //     }
        // }

        tables
    }
}

#[async_trait::async_trait]
impl SchemaProvider for DataLake {
    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.lock();
        tables.contains_key(name)
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.lock();
        tables.keys().cloned().collect()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let tables = self.tables.lock();
        Ok(tables.get(name).cloned())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::{parse_url, path::PathPart};
    use url::Url;

    use super::*;

    #[tokio::test]
    async fn test_name() {
        let fs = Arc::new(LocalFileSystem::new_with_prefix("./data").unwrap());
        let mut list_files = fs.list(None);

        // object_store::path::Path::from_url_path(path)

        let url = Url::parse("s3://bucket/path").unwrap();
        let (store, path) = parse_url(&url).unwrap();
        // assert_eq!(path.as_ref(), "path");
        println!("Path: {:?}", path);

        let object_store_url = ObjectStoreUrl::parse("s3://").unwrap();
        println!("Object Store URL: {:?}", object_store_url);

        // let part: PathPart<'_> = PathPart::parse("foo/bar").unwrap();
        // println!("{:?}", part);

        // while let Some(file) = list_files.next().await {
        //     match file {
        //         Ok(file) => {
        //             println!("Found file: {:?}", file);

        //             let parts = file.location.parts().collect::<Vec<_>>();
        //             println!("File parts: {:?}", parts);
        //         }
        //         Err(e) => eprintln!("Error listing files: {}", e),
        //     }
        // }
    }
}
