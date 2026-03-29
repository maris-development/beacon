use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    path::Path,
    sync::{Arc, LazyLock},
};

use arrow::datatypes::SchemaRef;
use beacon_common::listing_url::parse_listing_table_url;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_formats::file_formats;
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
    table::{_type::TableType, Table, empty::EmptyTable, error::TableError},
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
    fn table_directory_from_location(
        location: &object_store::path::Path,
    ) -> Option<Vec<PathPart<'static>>> {
        if location.filename() != Some("table.json") {
            return None;
        }

        let mut table_directory = location
            .parts()
            .map(|part| part.as_ref().to_string().into())
            .collect::<Vec<_>>();
        table_directory.pop();

        Some(table_directory)
    }

    fn partition_tables_for_initialization(tables: Vec<Table>) -> (Vec<Table>, Vec<Table>) {
        let mut regular_tables = Vec::new();
        let mut merged_tables = Vec::new();

        for table in tables {
            if matches!(table.table_type, TableType::Merged(_)) {
                merged_tables.push(table);
            } else {
                regular_tables.push(table);
            }
        }

        (regular_tables, merged_tables)
    }

    fn merged_table_references(
        tables: &HashMap<String, Table>,
        table_name: &str,
    ) -> Option<String> {
        for (candidate_name, candidate) in tables {
            if candidate_name == table_name {
                continue;
            }

            if let TableType::Merged(merged_table) = &candidate.table_type
                && merged_table
                    .table_names
                    .iter()
                    .any(|name| name == table_name)
            {
                return Some(candidate_name.clone());
            }
        }

        None
    }

    fn ordered_table_names_for_refresh(tables: &HashMap<String, Table>) -> Vec<String> {
        let mut non_merged = Vec::new();
        let mut merged = Vec::new();

        for (name, table) in tables {
            if matches!(table.table_type, TableType::Merged(_)) {
                merged.push(name.clone());
            } else {
                non_merged.push(name.clone());
            }
        }

        non_merged.extend(merged);
        non_merged
    }

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

        let table_providers = HashMap::new();
        let tables = HashMap::new();

        let file_formats =
            file_formats(session_context.clone(), get_datasets_object_store().await).unwrap();

        Self {
            data_directory_store_url: datasets_object_store_url,
            table_directory_store_url: tables_object_store_url,
            session_context,
            config,
            file_formats,
            table_providers: parking_lot::Mutex::new(table_providers),
            tables: parking_lot::Mutex::new(tables),
        }
    }

    fn read_config() -> Config {
        // Read the config from environment variables or a config file
        Config {
            read_only: false, // Example value, replace with actual logic
        }
    }

    async fn ensure_default_table(&self) -> anyhow::Result<()> {
        if self.table_exist("default") {
            return Ok(());
        }

        let table = Table {
            table_directory: vec![],
            table_name: "default".to_string(),
            table_type: table::_type::TableType::Empty(EmptyTable::new()),
            description: Some("Default Table.".to_string()),
        };

        self.create_table(table)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create default table: {}", e))
    }

    async fn load_tables_from_object_store(
        tables_object_store: Arc<dyn ObjectStore>,
    ) -> Vec<Table> {
        let mut discovered_tables = Vec::new();
        let mut entry_stream = tables_object_store.list(None);

        while let Some(entry) = entry_stream.next().await {
            tracing::info!("Found table entry: {:?}", entry);

            let Ok(entry) = entry else {
                continue;
            };

            let Some(table_directory) = Self::table_directory_from_location(&entry.location) else {
                continue;
            };

            match Table::open(tables_object_store.clone(), table_directory).await {
                Ok(table) => discovered_tables.push(table),
                Err(e) => {
                    tracing::error!("Failed to open table at {:?}: {}", entry.location, e);
                }
            }
        }

        discovered_tables
    }

    async fn register_tables(
        tables_to_register: Vec<Table>,
        tables_object_store_url: &ObjectStoreUrl,
        data_directory_store_url: &ObjectStoreUrl,
        session_context: &Arc<SessionContext>,
        table_providers: &mut HashMap<String, Arc<dyn TableProvider>>,
        tables: &mut HashMap<String, Table>,
        table_kind: &str,
    ) {
        for table in tables_to_register {
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
                    "Failed to create {} provider for {}: {}",
                    table_kind,
                    table.table_name,
                    provider.unwrap_err()
                );
            }
        }
    }

    fn replace_registered_tables(
        &self,
        table_providers: HashMap<String, Arc<dyn TableProvider>>,
        tables: HashMap<String, Table>,
    ) {
        *self.table_providers.lock() = table_providers;
        *self.tables.lock() = tables;
    }

    pub async fn init_tables(&self, session_context: Arc<SessionContext>) -> anyhow::Result<()> {
        let (regular_tables, merged_tables) = Self::init_tables_impl(
            self.table_directory_store_url.clone(),
            self.data_directory_store_url.clone(),
            session_context,
        )
        .await?;

        let mut table_providers = HashMap::new();
        let mut tables = HashMap::new();

        Self::register_tables(
            regular_tables,
            &self.table_directory_store_url,
            &self.data_directory_store_url,
            &self.session_context,
            &mut table_providers,
            &mut tables,
            "table",
        )
        .await;

        self.replace_registered_tables(table_providers.clone(), tables.clone());

        Self::register_tables(
            merged_tables,
            &self.table_directory_store_url,
            &self.data_directory_store_url,
            &self.session_context,
            &mut table_providers,
            &mut tables,
            "merged table",
        )
        .await;

        self.replace_registered_tables(table_providers, tables);

        self.ensure_default_table().await
    }

    async fn init_tables_impl(
        tables_object_store_url: ObjectStoreUrl,
        _data_directory_store_url: ObjectStoreUrl,
        session_context: Arc<SessionContext>,
    ) -> anyhow::Result<(Vec<Table>, Vec<Table>)> {
        tracing::info!("Initializing tables from object store");
        let tables_object_store = session_context
            .runtime_env()
            .object_store(&tables_object_store_url)
            .map_err(|e| anyhow::anyhow!("Failed to get tables object store: {}", e))?;

        let discovered_tables = Self::load_tables_from_object_store(tables_object_store).await;
        Ok(Self::partition_tables_for_initialization(discovered_tables))
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
                    Self::ordered_table_names_for_refresh(&tables)
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
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
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

    pub async fn update_table(&self, table: Table) -> Result<(), TableError> {
        self.remove_table(&table.table_name).await?;
        self.create_table(table).await
    }

    pub async fn create_table(&self, mut table: Table) -> Result<(), TableError> {
        if self.tables.lock().contains_key(&table.table_name) {
            return Err(TableError::TableAlreadyExists(table.table_name));
        }

        let table_provider = table
            .table_provider(
                self.session_context.clone(),
                self.data_directory_store_url.clone(),
                self.table_directory_store_url.clone(),
            )
            .await?;

        let table_object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.table_directory_store_url)
            .unwrap();

        let table_directory: Vec<PathPart<'static>> =
            vec![PathPart::from(table.table_name.clone())];
        table.save(table_object_store, table_directory).await;

        let mut tables = self.tables.lock();
        let mut table_providers = self.table_providers.lock();
        table_providers.insert(table.table_name.clone(), table_provider);
        tables.insert(table.table_name.clone(), table);
        Ok(())
    }

    pub async fn refresh_table(&self, table_name: &str) -> Result<(), TableError> {
        let table = self
            .tables
            .lock()
            .get(table_name)
            .cloned()
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
        let mut tables = self.tables.lock();
        if !tables.contains_key(table_name) {
            return Err(TableError::TableNotFound(table_name.to_string()));
        }

        if let Some(merged_table) = Self::merged_table_references(&tables, table_name) {
            return Err(TableError::TableReferencedByMerged {
                table_name: table_name.to_string(),
                merged_table,
            });
        }

        let table = tables.remove(table_name);
        drop(tables); // Release the lock before async call

        let mut table_providers = self.table_providers.lock();
        table_providers.remove(table_name);
        drop(table_providers); // Release the lock before deleting the table

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{_type::TableType, merged::MergedTable};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_table(name: &str, table_type: TableType) -> Table {
        Table {
            table_directory: vec![],
            table_name: name.to_string(),
            table_type,
            description: None,
        }
    }

    #[test]
    fn table_directory_from_location_extracts_parent_directory() {
        let location = object_store::path::Path::from("folder/example/table.json");

        let table_directory = DataLake::table_directory_from_location(&location)
            .expect("table.json path should produce a directory");
        let parts = table_directory
            .iter()
            .map(|part| part.as_ref())
            .collect::<Vec<_>>();

        assert_eq!(parts, vec!["folder", "example"]);
    }

    #[test]
    fn table_directory_from_location_ignores_non_table_config_files() {
        let location = object_store::path::Path::from("folder/example/not-a-table.json");

        assert!(DataLake::table_directory_from_location(&location).is_none());
    }

    #[test]
    fn partition_tables_for_initialization_keeps_merged_tables_separate() {
        let tables = vec![
            test_table("base_a", TableType::Empty(EmptyTable::new())),
            test_table(
                "merged_x",
                TableType::Merged(MergedTable {
                    table_names: vec!["base_a".to_string()],
                }),
            ),
            test_table("base_b", TableType::Empty(EmptyTable::new())),
            test_table(
                "merged_y",
                TableType::Merged(MergedTable {
                    table_names: vec!["base_b".to_string()],
                }),
            ),
        ];

        let (regular_tables, merged_tables) = DataLake::partition_tables_for_initialization(tables);

        assert_eq!(
            regular_tables
                .into_iter()
                .map(|table| table.table_name)
                .collect::<Vec<_>>(),
            vec!["base_a", "base_b"]
        );
        assert_eq!(
            merged_tables
                .into_iter()
                .map(|table| table.table_name)
                .collect::<Vec<_>>(),
            vec!["merged_x", "merged_y"]
        );
    }

    #[test]
    fn merged_table_references_detects_dependency() {
        let mut tables = HashMap::new();

        tables.insert(
            "base_table".to_string(),
            test_table("base_table", TableType::Empty(EmptyTable::new())),
        );

        tables.insert(
            "merged_table".to_string(),
            test_table(
                "merged_table",
                TableType::Merged(MergedTable {
                    table_names: vec!["base_table".to_string()],
                }),
            ),
        );

        let dependent = DataLake::merged_table_references(&tables, "base_table");
        assert_eq!(dependent, Some("merged_table".to_string()));
    }

    #[test]
    fn ordered_table_names_for_refresh_puts_merged_last() {
        let mut tables = HashMap::new();

        tables.insert(
            "base_a".to_string(),
            test_table("base_a", TableType::Empty(EmptyTable::new())),
        );

        tables.insert(
            "base_b".to_string(),
            test_table("base_b", TableType::Empty(EmptyTable::new())),
        );

        tables.insert(
            "merged_x".to_string(),
            test_table(
                "merged_x",
                TableType::Merged(MergedTable {
                    table_names: vec!["base_a".to_string(), "base_b".to_string()],
                }),
            ),
        );

        let order = DataLake::ordered_table_names_for_refresh(&tables);

        let merged_positions = order
            .iter()
            .enumerate()
            .filter_map(|(idx, name)| (name == "merged_x").then_some(idx))
            .collect::<Vec<_>>();
        let base_positions = order
            .iter()
            .enumerate()
            .filter_map(|(idx, name)| ((name == "base_a") || (name == "base_b")).then_some(idx))
            .collect::<Vec<_>>();

        assert_eq!(merged_positions.len(), 1);
        assert_eq!(base_positions.len(), 2);
        assert!(
            base_positions
                .into_iter()
                .all(|idx| idx < merged_positions[0])
        );
    }

    #[tokio::test]
    async fn init_tables_registers_base_tables_before_merged_tables() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();

        let base_a_name = format!("init-order-base-a-{suffix}");
        let base_b_name = format!("init-order-base-b-{suffix}");
        let merged_name = format!("init-order-merged-{suffix}");

        let creator_ctx = Arc::new(SessionContext::new());
        let creator = Arc::new(DataLake::new(creator_ctx.clone()).await);
        creator_ctx
            .catalog("datafusion")
            .expect("default catalog should exist")
            .register_schema("public", creator.clone())
            .expect("schema registration should succeed");

        creator
            .create_table(test_table(
                &base_a_name,
                TableType::Empty(EmptyTable::new()),
            ))
            .await
            .expect("base table A should be created");
        creator
            .create_table(test_table(
                &base_b_name,
                TableType::Empty(EmptyTable::new()),
            ))
            .await
            .expect("base table B should be created");
        creator
            .create_table(test_table(
                &merged_name,
                TableType::Merged(MergedTable {
                    table_names: vec![base_a_name.clone(), base_b_name.clone()],
                }),
            ))
            .await
            .expect("merged table should be created");

        let reload_ctx = Arc::new(SessionContext::new());
        let reloaded = Arc::new(DataLake::new(reload_ctx.clone()).await);
        reload_ctx
            .catalog("datafusion")
            .expect("default catalog should exist")
            .register_schema("public", reloaded.clone())
            .expect("schema registration should succeed");

        reloaded
            .init_tables(reload_ctx.clone())
            .await
            .expect("table initialization should succeed");

        assert!(reloaded.table_exist(&base_a_name));
        assert!(reloaded.table_exist(&base_b_name));
        assert!(reloaded.table_exist(&merged_name));

        let merged_table = reloaded
            .list_table(&merged_name)
            .expect("merged table metadata should be present");
        match merged_table.table_type {
            TableType::Merged(merged) => {
                assert_eq!(
                    merged.table_names,
                    vec![base_a_name.clone(), base_b_name.clone()]
                );
            }
            other => panic!("expected merged table, got {other:?}"),
        }

        let merged_provider = reload_ctx
            .table_provider(&merged_name)
            .await
            .expect("merged provider lookup should succeed");
        assert_eq!(
            merged_provider.table_type(),
            datafusion::datasource::TableType::Base
        );

        reloaded
            .remove_table(&merged_name)
            .await
            .expect("merged table cleanup should succeed");
        reloaded
            .remove_table(&base_a_name)
            .await
            .expect("base table A cleanup should succeed");
        reloaded
            .remove_table(&base_b_name)
            .await
            .expect("base table B cleanup should succeed");
    }
}
