use std::{any::Any, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};
use object_store::{ObjectStore, local::LocalFileSystem};

pub mod table;

#[derive(Debug)]
pub struct Config {
    read_only: bool,
}

#[derive(Debug)]
pub struct DataLake {
    engine: Arc<dyn ObjectStore>,
    local_temp_fs: Arc<LocalFileSystem>,
    config: Config,
}

impl DataLake {
    pub fn new(engine: Arc<dyn ObjectStore>, config: Config) -> Self {
        // Create tmp object store for storing temp files.
        let local_temp_fs = Arc::new(
            LocalFileSystem::new_with_prefix("./tmp")
                .expect("Failed to create local temp file system. Is the tmp dir set correctly?"),
        );

        Self {
            engine,
            local_temp_fs,
            config,
        }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for DataLake {
    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        false
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        todo!()
    }
}
