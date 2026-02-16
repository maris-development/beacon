use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use arrow::datatypes::SchemaRef;
use beacon_common::listing_url::parse_listing_table_url;
use beacon_formats::{Dataset, FileFormatFactoryExt, file_formats};
use beacon_object_storage::get_datasets_object_store;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    datasource::listing::ListingTableUrl,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};
use futures::{
    stream::BoxStream,
    {StreamExt, TryFutureExt},
};
use object_store::{ObjectStore, path::PathPart};
use url::Url;

use crate::{
    files::{collection::FileCollection, temp_output_file::TempOutputFile},
    table::{Table, empty::EmptyTable, error::TableError},
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
    table_directory_store_url: ObjectStoreUrl,
    tmp_directory_store_url: ObjectStoreUrl,

    /// The session context used for executing queries and managing the session state.
    session_context: Arc<SessionContext>,

    config: Config,
    // Map of tables
    tables: parking_lot::Mutex<HashMap<String, Table>>,
    table_providers: parking_lot::Mutex<HashMap<String, Arc<dyn TableProvider>>>,

    // File formats
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl Debug for DataLake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataLake")
            .field("data_directory_store_url", &self.data_directory_store_url)
            .field("table_directory_store_url", &self.table_directory_store_url)
            .field("config", &self.config)
            .field("tables", &self.tables)
            .finish()
    }
}

pub static DATASETS_OBJECT_STORE_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse("datasets://").expect("Failed to parse datasets URL"));
pub static TABLES_OBJECT_STORE_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse("tables://").expect("Failed to parse tables URL"));
pub static TMP_OBJECT_STORE_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse("tmp://").expect("Failed to parse tmp URL"));
pub static INDEX_OBJECT_STORE_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse("index://").expect("Failed to parse index URL")); // ToDo: implement indexing on top of existing files utilizing the notified storage events.

impl DataLake {
    #[inline(always)]
    pub fn try_create_listing_url(
        &self,
        path: String,
    ) -> datafusion::error::Result<ListingTableUrl> {
        parse_listing_table_url(&self.data_directory_store_url, &path)
    }

    pub fn data_object_store_url(&self) -> ObjectStoreUrl {
        self.data_directory_store_url.clone()
    }

    pub fn try_create_temp_output_file(&self, extension: &str) -> TempOutputFile {
        Self::create_temp_output_file(extension)
    }
    pub fn create_temp_output_file(extension: &str) -> TempOutputFile {
        TempOutputFile::new(extension)
    }

    pub async fn new(session_context: Arc<SessionContext>) -> Self {
        // Register object stores
        // Init them if they have not been initialized yet.
        beacon_object_storage::init_datastores()
            .await
            .expect("Failed to initialize Data Lake Engine...");
        let datasets_object_store = get_datasets_object_store().await;
        let datasets_object_store_url = DATASETS_OBJECT_STORE_URL.clone();
        // Register datasets object store
        session_context.register_object_store(
            &Url::parse(datasets_object_store_url.as_str()).unwrap(),
            datasets_object_store,
        );
        // Register tables object store
        let tables_object_store = beacon_object_storage::get_tables_object_store().await;
        let tables_object_store_url = TABLES_OBJECT_STORE_URL.clone();
        session_context.register_object_store(
            &Url::parse(tables_object_store_url.as_str()).unwrap(),
            tables_object_store,
        );
        // Register tmp object store
        let tmp_object_store = beacon_object_storage::get_tmp_object_store().await;
        let tmp_object_store_url = TMP_OBJECT_STORE_URL.clone();
        session_context.register_object_store(
            &Url::parse(tmp_object_store_url.as_str()).unwrap(),
            tmp_object_store,
        );

        let config = Self::read_config();

        let mut table_providers = HashMap::new();
        let mut tables = HashMap::new();

        let file_formats =
            file_formats(session_context.clone(), get_datasets_object_store().await).unwrap();

        Self::init_tables(
            tables_object_store_url.clone(),
            datasets_object_store_url.clone(),
            session_context.clone(),
            &mut table_providers,
            &mut tables,
        )
        .await;

        let data_lake = Self {
            data_directory_store_url: datasets_object_store_url,
            table_directory_store_url: tables_object_store_url,
            tmp_directory_store_url: tmp_object_store_url,
            session_context,
            config,
            file_formats,
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

    async fn init_tables(
        tables_object_store_url: ObjectStoreUrl,
        data_directory_store_url: ObjectStoreUrl,
        session_context: Arc<SessionContext>,
        table_providers: &mut HashMap<String, Arc<dyn TableProvider>>,
        tables: &mut HashMap<String, Table>,
    ) {
        tracing::info!("Initializing tables from object store");
        let tables_object_store = session_context
            .runtime_env()
            .object_store(&tables_object_store_url)
            .unwrap();

        let mut entry_stream = tables_object_store.list(None);
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
                                tables_object_store_url.clone(),
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

    pub fn spawn_sync_table_refresh(self: &Arc<Self>, interval_secs: u64) {
        if interval_secs == 0 {
            tracing::info!("Table sync interval is set to 0, skipping table refresh task.");
            return;
        }
        let data_lake = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            //Consume the first tick
            interval.tick().await;
            loop {
                interval.tick().await;
                tracing::info!("Refreshing tables...");
                let table_names: Vec<String> = {
                    let tables = data_lake.tables.lock();
                    tables.keys().cloned().collect()
                };
                for table_name in table_names {
                    if let Err(e) = data_lake.refresh_table(&table_name).await {
                        tracing::error!(
                            "Failed to refresh table {}: {}. Removing it from the list of available tables.",
                            table_name,
                            e
                        );
                        // Should we remove or keep the table from the list of available tables if it fails to refresh?
                        // For now remove it from the list of available tables, but we could also keep it and just mark it as not refreshable or something like that.
                        {
                            let mut tables = data_lake.tables.lock();
                            tables.remove(&table_name);
                        }
                        {
                            let mut table_providers = data_lake.table_providers.lock();
                            table_providers.remove(&table_name);
                        }
                    } else {
                        tracing::info!("Successfully refreshed table {}", table_name);
                    }
                }
            }
        });
    }

    pub async fn list_datasets(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
        pattern: Option<String>,
    ) -> datafusion::error::Result<Vec<Dataset>> {
        let state = self.session_context.state();
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(self.data_directory_store_url.clone())?;

        let listing_url =
            self.try_create_listing_url(pattern.unwrap_or_else(|| "*".to_string()))?;

        let mut objects = Vec::new();
        let mut entry_stream = listing_url
            .list_all_files(&state, &object_store, "")
            .await?;

        while let Some(entry) = entry_stream.next().await {
            if let Ok(entry) = entry {
                objects.push(entry);
            }
        }

        let mut datasets = vec![];

        for file_format in self.file_formats.iter() {
            let format_datasets = file_format.discover_datasets(&objects)?;
            datasets.extend(format_datasets);
        }

        // Apply offset and limit
        let start = offset.unwrap_or(0);
        let end = limit.map(|l| start + l).unwrap_or(datasets.len());
        let datasets = datasets.into_iter().skip(start).take(end - start).collect();

        Ok(datasets)
    }

    pub async fn list_dataset_schema(
        &self,
        file_pattern: &str,
    ) -> datafusion::error::Result<SchemaRef> {
        let session_state = self.session_context.state();
        let extension = if file_pattern.ends_with("zarr.json") {
            "zarr.json".to_string()
        } else {
            match Path::new(file_pattern).extension() {
                Some(ext) => {
                    // Fetch file format from extension
                    ext.to_string_lossy().to_string()
                }
                None => {
                    return Err(DataFusionError::Plan(format!(
                        "No file extension found for {}. No file type information available.",
                        file_pattern
                    )));
                }
            }
        };
        tracing::debug!("Interpreted file extension: {}", extension);
        let listing_url = self.try_create_listing_url(file_pattern.to_string())?;

        let file_format_factory = session_state
            .get_file_format_factory(&extension)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("No file format reader found for {}", extension))
            })?;
        let file_format = file_format_factory.create(&session_state, &HashMap::new())?;
        tracing::debug!("Using file format: {:?}", file_format);

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

        let mut table_directory: Vec<PathPart<'static>> = vec![];
        table_directory.push(PathPart::from(table.table_name.clone()));
        drop(tables); // Release the lock before saving the table as to not deadlock across the async call
        table.save(table_object_store, table_directory).await;
        let table_provider = table
            .table_provider(
                self.session_context.clone(),
                self.data_directory_store_url.clone(),
                self.table_directory_store_url.clone(),
            )
            .await?;
        // Re-acquire the lock to insert the table
        tables = self.tables.lock();
        let mut table_providers = self.table_providers.lock();
        table_providers.insert(table.table_name.clone(), table_provider);
        tables.insert(table.table_name.clone(), table);
        Ok(())
    }

    pub async fn refresh_table(&self, table_name: &str) -> Result<(), TableError> {
        let tables = self.tables.lock();
        let table = tables
            .get(table_name)
            .ok_or(TableError::TableNotFound(table_name.to_string()))?;

        let refreshed_table_provider = table
            .table_provider(
                self.session_context.clone(),
                self.data_directory_store_url.clone(),
                self.table_directory_store_url.clone(),
            )
            .await?;

        let mut table_providers = self.table_providers.lock();
        table_providers.insert(table_name.to_string(), refreshed_table_provider);
        Ok(())
    }

    pub async fn apply_operation(
        &self,
        table_name: &str,
        _op: serde_json::Value,
    ) -> Result<(), TableError> {
        let tables = self.tables.lock();
        let table = tables
            .get(table_name)
            .ok_or(TableError::TableNotFound(table_name.to_string()))?;

        table
            .table_type
            .apply_operation(
                _op,
                self.session_context.clone(),
                &self.data_directory_store_url,
            )
            .map_err(|e| TableError::GenericTableError(format!("Failed to apply operation: {}", e)))
            .await
    }

    pub async fn upload_file<S>(
        &self,
        file_path: &str,
        mut stream: S,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>
            + Unpin,
    {
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.data_directory_store_url)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let upload_path = object_store::path::Path::from(file_path);

        let mut writer = object_store
            .put_multipart(&upload_path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        while let Some(chunk) = stream.next().await {
            let bytes = chunk?;
            writer
                .put_part(bytes.into())
                .await
                .map_err(|e: object_store::Error| Box::new(e))?;
        }

        // Finalize the upload
        writer
            .complete()
            .await
            .map_err(|e: object_store::Error| Box::new(e))?;

        Ok(())
    }

    pub async fn download_file(
        &self,
        file_path: &str,
    ) -> Result<
        BoxStream<'static, Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.data_directory_store_url)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let file_path = object_store::path::Path::from(file_path);

        let get_result = object_store
            .get(&file_path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let stream = get_result.into_stream();

        let file_stream = Box::pin(stream.map(|result| {
            result.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        }));

        Ok(file_stream)
    }

    pub async fn delete_file(
        &self,
        file_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.data_directory_store_url)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let file_path = object_store::path::Path::from(file_path);

        object_store
            .delete(&file_path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(())
    }

    pub async fn remove_table(&self, table_name: &str) -> Result<(), TableError> {
        let mut table_providers = self.table_providers.lock();
        table_providers.remove(table_name);
        drop(table_providers); // Release the lock before deleting the table
        let mut tables = self.tables.lock();
        let table = tables.remove(table_name);
        drop(tables); // Release the lock before async call

        if let Some(table) = table {
            let table_object_store_url = self.table_directory_store_url.clone();

            table
                .delete_table(self.session_context.clone(), table_object_store_url)
                .await;
            Ok(())
        } else {
            Err(TableError::TableNotFound(table_name.to_string()))
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
