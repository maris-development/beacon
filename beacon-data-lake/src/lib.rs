use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use beacon_formats::netcdf::object_resolver::{NetCDFObjectResolver, NetCDFSinkResolver};
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    datasource::listing::ListingTableUrl,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};
use futures::StreamExt;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, path::PathPart};
use url::Url;

use crate::{
    files::{collection::FileCollection, temp_output_file::TempOutputFile},
    table::{Table, empty::EmptyTable, error::TableError},
    util::split_glob,
};

pub mod files;
pub mod table;
pub mod util;

pub mod prelude {
    pub use super::DataLake;
    pub use super::files::*;
}

#[derive(Debug)]
pub struct Config {
    read_only: bool,
}

pub struct DataLake {
    data_directory_store_url: ObjectStoreUrl,
    data_directory_prefix: object_store::path::Path,

    table_directory_store_url: ObjectStoreUrl,
    table_directory_prefix: object_store::path::Path,

    tmp_directory_object_store: Arc<LocalFileSystem>,
    tmp_directory_store_url: ObjectStoreUrl,
    tmp_directory_prefix: object_store::path::Path,
    /// The session context used for executing queries and managing the session state.
    session_context: Arc<SessionContext>,

    config: Config,
    // Map of tables
    tables: parking_lot::Mutex<HashMap<String, Table>>,
    table_providers: parking_lot::Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl Debug for DataLake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataLake")
            .field("data_directory_store_url", &self.data_directory_store_url)
            .field("data_directory_prefix", &self.data_directory_prefix)
            .field("table_directory_store_url", &self.table_directory_store_url)
            .field("table_directory_prefix", &self.table_directory_prefix)
            .field("config", &self.config)
            .field("tables", &self.tables)
            .finish()
    }
}

impl DataLake {
    pub fn try_create_listing_url(
        &self,
        path: String,
    ) -> datafusion::error::Result<ListingTableUrl> {
        let parts = self.data_directory_prefix.parts().collect::<Vec<_>>();
        if let Some((base, pattern)) = split_glob(&path) {
            let pattern = glob::Pattern::new(&pattern).unwrap();
            let mut full_path = format!(
                "{}{}",
                self.data_directory_store_url,
                object_store::path::Path::from_iter(parts.clone().into_iter()),
            );
            if base.components().next().is_some() {
                full_path.push_str(format!("/{}", base.as_os_str().to_string_lossy()).as_str());
            }
            if !full_path.ends_with('/') {
                full_path.push('/');
            }

            let url = Url::parse(&full_path).unwrap();

            let table_url = ListingTableUrl::try_new(url, Some(pattern))?;

            Ok(table_url)
        } else {
            let mut full_path = format!(
                "{}{}",
                self.data_directory_store_url,
                object_store::path::Path::from_iter(parts.clone().into_iter()),
            );

            // If full path ends with '/' then just append the path, otherwise append without a '/'
            if full_path.ends_with('/') {
                full_path.push_str(&path);
            } else {
                full_path.push_str(format!("/{}", path).as_str());
            }

            let url = Url::parse(&full_path).unwrap();

            let table_url = ListingTableUrl::try_new(url, None)?;

            Ok(table_url)
        }
    }

    pub fn netcdf_object_resolver() -> Arc<NetCDFObjectResolver> {
        if beacon_config::CONFIG.s3_data_lake {
            let endpoint = beacon_config::CONFIG
                .s3_endpoint
                .clone()
                .expect("S3 endpoint not set");
            let bucket = beacon_config::CONFIG
                .s3_bucket
                .clone()
                .expect("S3 bucket not set");
            Arc::new(NetCDFObjectResolver::new(endpoint, Some(bucket), None))
        } else {
            let base_path = PathBuf::from("./data");

            std::fs::create_dir_all(&base_path).expect("Failed to create datasets directory");

            let absolute_path = base_path.canonicalize().unwrap();

            // Create directories if they do not exist
            std::fs::create_dir_all(&absolute_path).expect("Failed to create datasets directory");
            tracing::debug!(
                "Using local NetCDF datasets path: {}",
                absolute_path.display()
            );
            Arc::new(NetCDFObjectResolver::new(
                "file://".to_string(),
                None,
                Some(absolute_path.to_string_lossy().to_string()),
            ))
        }
    }

    pub fn netcdf_sink_resolver() -> Arc<NetCDFSinkResolver> {
        let base_path = PathBuf::from("./data");
        std::fs::create_dir_all(&base_path).expect("Failed to create datasets directory");
        let absolute_path = base_path.canonicalize().unwrap();

        tracing::debug!("Using local NetCDF sink path: {}", absolute_path.display());

        // Create directories if they do not exist
        Arc::new(NetCDFSinkResolver::new(absolute_path))
    }

    pub fn try_create_temp_output_file(&self, extension: &str) -> TempOutputFile {
        TempOutputFile::new(self, extension)
    }

    pub async fn new(session_context: Arc<SessionContext>) -> Self {
        // Create tmp object store for storing temp files.
        let (datasets_url, datasets_prefix) =
            Self::datasets_url_with_prefix(session_context.clone());
        let (tables_url, tables_prefix) = Self::tables_url_with_prefix(session_context.clone());
        let (tmp_directory_object_store, tmp_directory_store_url, tmp_directory_prefix) =
            Self::tmp_url_with_prefix(session_context.clone());

        let config = Self::read_config();

        let mut table_providers = HashMap::new();
        let mut tables = HashMap::new();

        Self::init_tables(
            tables_url.clone(),
            tables_prefix.clone(),
            datasets_url.clone(),
            datasets_prefix.clone(),
            session_context.clone(),
            &mut table_providers,
            &mut tables,
        )
        .await;

        let data_lake = Self {
            data_directory_store_url: datasets_url,
            data_directory_prefix: datasets_prefix,
            table_directory_store_url: tables_url,
            table_directory_prefix: tables_prefix,
            tmp_directory_object_store,
            tmp_directory_store_url,
            tmp_directory_prefix,
            session_context,
            config,
            table_providers: parking_lot::Mutex::new(table_providers),
            tables: parking_lot::Mutex::new(tables),
        };

        if !data_lake.table_exist("default") {
            let default_table_type = EmptyTable::new();
            let table = Table {
                table_directory: vec![],
                table_name: "default".to_string(),
                table_type: table::_type::TableType::Empty(default_table_type),
                description: Some("Default Table.".to_string()),
            };
            data_lake
                .create_table(table)
                .await
                .expect("Failed to create default table.");
        }

        data_lake
    }

    fn read_config() -> Config {
        // Read the config from environment variables or a config file
        Config {
            read_only: false, // Example value, replace with actual logic
        }
    }

    fn tmp_url_with_prefix(
        context: Arc<SessionContext>,
    ) -> (
        Arc<LocalFileSystem>,
        ObjectStoreUrl,
        object_store::path::Path,
    ) {
        let base_path = PathBuf::from("./data");
        let tmp_directory = base_path.join("tmp");

        // Create directories if they do not exist
        std::fs::create_dir_all(&tmp_directory).expect("Failed to create tmp directory");

        // Configure the object store using LOCAL FS
        let tmp_url = ObjectStoreUrl::parse("file://").expect("Failed to parse file URL");
        let tmp_fs = LocalFileSystem::new_with_prefix("./data").unwrap();
        let tmp_fs_arc = Arc::new(tmp_fs);
        context.register_object_store(tmp_url.as_ref(), tmp_fs_arc.clone());

        let path_prefix = object_store::path::Path::from("tmp/");

        (tmp_fs_arc, tmp_url, path_prefix)
    }

    fn tables_url_with_prefix(
        context: Arc<SessionContext>,
    ) -> (ObjectStoreUrl, object_store::path::Path) {
        let base_path = PathBuf::from("./data");
        let table_directory = base_path.join("tables");

        // Create directories if they do not exist
        std::fs::create_dir_all(&table_directory).expect("Failed to create tables directory");

        // Configure the object store using LOCAL FS
        let table_url = ObjectStoreUrl::parse("file://").expect("Failed to parse file URL");
        let table_fs = LocalFileSystem::new_with_prefix("./data").unwrap();

        context.register_object_store(table_url.as_ref(), Arc::new(table_fs));

        let path_prefix = object_store::path::Path::from("tables/");

        (table_url, path_prefix)
    }

    fn datasets_url_with_prefix(
        context: Arc<SessionContext>,
    ) -> (ObjectStoreUrl, object_store::path::Path) {
        if beacon_config::CONFIG.s3_data_lake {
            // Fetch the s3 settings from the config
            let mut s3_object_store_builder = AmazonS3Builder::new()
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false);

            let endpoint = beacon_config::CONFIG
                .s3_endpoint
                .clone()
                .expect("S3 endpoint not set");
            s3_object_store_builder = s3_object_store_builder.with_endpoint(endpoint);
            let bucket = beacon_config::CONFIG
                .s3_bucket
                .clone()
                .expect("S3 bucket not set");
            s3_object_store_builder = s3_object_store_builder.with_bucket_name(bucket.clone());

            if let Some(region) = beacon_config::CONFIG.s3_region.clone() {
                s3_object_store_builder = s3_object_store_builder.with_region(region);
            }

            if let Some(access_key_id) = beacon_config::CONFIG.s3_access_key_id.clone() {
                s3_object_store_builder = s3_object_store_builder.with_access_key_id(access_key_id);
            }

            if let Some(secret_access_key) = beacon_config::CONFIG.s3_secret_access_key.clone() {
                s3_object_store_builder =
                    s3_object_store_builder.with_secret_access_key(secret_access_key);
            } else {
                s3_object_store_builder = s3_object_store_builder.with_skip_signature(true);
            }

            let s3_object_store = s3_object_store_builder.build().unwrap();

            let object_store_url =
                ObjectStoreUrl::parse("http://datasets").expect("Failed to parse S3 URL");
            context.register_object_store(object_store_url.as_ref(), Arc::new(s3_object_store));

            (object_store_url, object_store::path::Path::from(""))
        } else {
            let base_path = PathBuf::from("./data");
            let dataset_directory = base_path.join("datasets");

            // Create directories if they do not exist
            std::fs::create_dir_all(&dataset_directory)
                .expect("Failed to create datasets directory");

            // Configure the object store using LOCAL FS
            let dataset_url = ObjectStoreUrl::parse("file://").expect("Failed to parse file URL");
            let dataset_fs = LocalFileSystem::new_with_prefix("./data").unwrap();

            context.register_object_store(dataset_url.as_ref(), Arc::new(dataset_fs));
            let path_prefix = object_store::path::Path::from("datasets/");
            (dataset_url, path_prefix)
        }
    }

    async fn init_tables(
        tables_object_store_url: ObjectStoreUrl,
        tables_prefix: object_store::path::Path,
        data_directory_store_url: ObjectStoreUrl,
        data_directory_prefix: object_store::path::Path,
        session_context: Arc<SessionContext>,
        table_providers: &mut HashMap<String, Arc<dyn TableProvider>>,
        tables: &mut HashMap<String, Table>,
    ) {
        tracing::info!("Initializing tables from object store");
        let tables_object_store = session_context
            .runtime_env()
            .object_store(&tables_object_store_url)
            .unwrap();

        let mut entry_stream = tables_object_store.list(Some(&tables_prefix));
        while let Some(entry) = entry_stream.next().await {
            tracing::info!("Found table entry: {:?}", entry);
            if let Ok(entry) = entry
                && entry.location.to_string().ends_with("table.json")
            {
                // Extract the table name from the path
                let mut table_directory: Vec<PathPart<'static>> = entry
                    .location
                    .parts()
                    .map(|part| part.as_ref().to_string().into())
                    .collect();
                // Pop the last part which is "table.json"
                table_directory.pop();

                // Open the table
                match Table::open(tables_object_store.clone(), table_directory).await {
                    Ok(table) => {
                        let provider = table
                            .table_provider(
                                session_context.clone(),
                                data_directory_store_url.clone(),
                                data_directory_prefix.clone(),
                                tables_object_store_url.clone(),
                                tables_prefix.clone(),
                            )
                            .await;

                        if let Ok(provider) = provider {
                            table_providers.insert(table.table_name.clone(), provider);
                            tables.insert(table.table_name.clone(), table);
                        } else {
                            tracing::error!(
                                "Failed to create table provider for {}: {}",
                                table.table_name,
                                provider.unwrap_err()
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to open table: {}", e);
                    }
                }
            }
        }
    }

    pub async fn list_datasets(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
        pattern: Option<String>,
    ) -> datafusion::error::Result<Vec<String>> {
        let state = self.session_context.state();
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(self.data_directory_store_url.clone())?;

        let listing_url =
            self.try_create_listing_url(pattern.unwrap_or_else(|| "*".to_string()))?;

        let mut datasets = Vec::new();
        let mut entry_stream = listing_url
            .list_all_files(&state, &object_store, "")
            .await?;

        while let Some(entry) = entry_stream.next().await {
            if let Ok(entry) = entry {
                datasets.push(entry.location.to_string());
            }
        }

        datasets.sort();

        if let Some(offset) = offset {
            datasets = datasets.into_iter().skip(offset).collect();
        }

        if let Some(limit) = limit {
            datasets = datasets.into_iter().take(limit).collect();
        }

        // For each dataset, remove the prefix if the file starts with that prefix
        let mut prefix = self.data_directory_prefix.clone().to_string();
        if !prefix.is_empty() {
            prefix.push('/');
        }

        datasets = datasets
            .into_iter()
            .map(|d| {
                if d.starts_with(&prefix) {
                    d.strip_prefix(&prefix).unwrap().to_string()
                } else {
                    d
                }
            })
            .collect();

        Ok(datasets)
    }

    pub async fn list_dataset_schema(
        &self,
        file_pattern: &str,
    ) -> datafusion::error::Result<SchemaRef> {
        let session_state = self.session_context.state();
        let extension = match Path::new(file_pattern).extension() {
            Some(ext) => ext.to_string_lossy().to_string(),
            None => {
                return Err(DataFusionError::Plan(format!(
                    "No file extension found for {}. No file type information available.",
                    file_pattern
                )));
            }
        };

        let listing_url = self.try_create_listing_url(file_pattern.to_string())?;

        let file_format_factory = session_state
            .get_file_format_factory(&extension)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("No file format reader found for {}", extension))
            })?;

        let file_format = file_format_factory.create(&session_state, &HashMap::new())?;

        let file_collection =
            FileCollection::new(&session_state, file_format, vec![listing_url]).await?;

        Ok(file_collection.schema())
    }

    pub fn list_table_schema(&self, table_name: &str) -> Option<SchemaRef> {
        let table_providers = self.table_providers.lock();
        table_providers.get(table_name).map(|t| t.schema())
    }

    pub fn list_table(&self, table_name: &str) -> Option<Table> {
        let tables = self.tables.lock();
        tables.get(table_name).cloned()
    }

    pub async fn update_table(&self, mut table: Table) -> Result<(), TableError> {
        self.remove_table(&table.table_name);
        self.create_table(table).await
    }

    pub async fn create_table(&self, mut table: Table) -> Result<(), TableError> {
        let mut tables = self.tables.lock();
        if tables.contains_key(&table.table_name) {
            return Err(TableError::TableAlreadyExists(table.table_name));
        }

        let table_object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.table_directory_store_url)
            .unwrap();

        let mut table_directory: Vec<PathPart<'static>> = self
            .table_directory_prefix
            .clone()
            .parts()
            .map(|part| part.as_ref().to_string().into())
            .collect::<Vec<_>>();
        table_directory.push(PathPart::from(table.table_name.clone()));
        drop(tables); // Release the lock before saving the table as to not deadlock across the async call
        table.save(table_object_store, table_directory).await;
        let table_provider = table
            .table_provider(
                self.session_context.clone(),
                self.data_directory_store_url.clone(),
                self.data_directory_prefix.clone(),
                self.table_directory_store_url.clone(),
                self.table_directory_prefix.clone(),
            )
            .await?;
        // Re-acquire the lock to insert the table
        tables = self.tables.lock();
        let mut table_providers = self.table_providers.lock();
        table_providers.insert(table.table_name.clone(), table_provider);
        tables.insert(table.table_name.clone(), table);
        Ok(())
    }

    pub fn remove_table(&self, table_name: &str) -> bool {
        let mut tables = self.tables.lock();
        if tables.remove(table_name).is_some() {
            let mut table_providers = self.table_providers.lock();
            table_providers.remove(table_name);
            true
        } else {
            false
        }
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
        let table_providers = self.table_providers.lock();
        Ok(table_providers.get(name).cloned())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        datasource::listing::ListingTableUrl, execution::object_store::ObjectStoreUrl,
    };
    use futures::StreamExt;
    use object_store::{aws::AmazonS3, parse_url, path::PathPart};
    use url::Url;

    use super::*;

    #[tokio::test]
    async fn test_object_store() {
        let aws = Arc::new(
            AmazonS3Builder::new()
                .with_endpoint("http://localhost:8000")
                .with_bucket_name("era5")
                .with_allow_http(true)
                .with_skip_signature(true)
                .build()
                .unwrap(),
        ) as Arc<dyn ObjectStore>;

        let object_store_url = ObjectStoreUrl::parse("http://datasets").unwrap();

        let session = SessionContext::new();
        session.register_object_store(object_store_url.as_ref(), aws.clone());

        let mut files = aws.list(None);
        while let Some(file) = files.next().await {
            match file {
                Ok(file) => println!("Found file: {:?}", file),
                Err(e) => eprintln!("Error listing files: {}", e),
            }
        }

        let pattern = glob::Pattern::new("*.parquet").unwrap();
        let listing_url =
            ListingTableUrl::try_new(Url::parse("http://datasets/").unwrap(), Some(pattern))
                .unwrap();

        println!("Listing URL: {:?}", listing_url);

        let state = session.state();
        let mut stream = listing_url.list_all_files(&state, &aws, "").await.unwrap();

        while let Some(file) = stream.next().await {
            println!("Found file: {:?}", file);
        }
    }

    #[tokio::test]
    async fn test_data_lake_initialization() {
        let session_context = Arc::new(SessionContext::new());
        let data_lake = DataLake::new(session_context.clone()).await;

        println!("Data Lake Initialization {:?}", data_lake);

        // List tables
        // let tables = data_lake.table_names();
        // println!("Tables in Data Lake: {:?}", tables);

        // let store = data_lake
        //     .session_context
        //     .runtime_env()
        //     .object_store(&data_lake.data_directory_store_url)
        //     .unwrap();

        // println!(
        //     "Data Directory Store URL: {:?}, Prefix: {:?}",
        //     data_lake.data_directory_store_url, data_lake.data_directory_prefix
        // );
        // println!("Store: {:?}", store);

        // let mut entry_stream = store.list(None);
        // while let Some(entry) = entry_stream.next().await {
        //     println!("Found entry: {:?}", entry);
        // }

        panic!("")
    }

    #[tokio::test]
    async fn test_name() {
        // let fs = Arc::new(LocalFileSystem::new_with_prefix("./data").unwrap());
        // let mut list_files = fs.list(None);

        // // object_store::path::Path::from_url_path(path)

        // let url = Url::parse("s3://bucket/path").unwrap();
        // let (store, path) = parse_url(&url).unwrap();
        // // assert_eq!(path.as_ref(), "path");
        // println!("Path: {:?}", path);

        let object_store_url = ObjectStoreUrl::parse("s3://example-bucket").unwrap();
        println!("Object Store URL: {:?}", object_store_url);

        let local_path = object_store::path::Path::parse("data/*.json").unwrap();
        let full_path = format!("{}{}", object_store_url, local_path);

        println!("Full Path: {}", full_path);
        // let parsed_path = object_store::path::Path::fr(&full_path).unwrap();
        // println!("Parsed Path: {:?}", parsed_path);
        let table_url = ListingTableUrl::parse(&full_path);
        println!("Table URL: {:?}", table_url);

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
