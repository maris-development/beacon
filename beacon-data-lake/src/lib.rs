use std::{
    any::Any,
    fmt::Debug,
    sync::{Arc, LazyLock},
};

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use beacon_formats::file_formats;
use beacon_object_storage::get_datasets_object_store;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    datasource::listing::ListingTableUrl,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};
use futures::stream::BoxStream;
use url::Url;

use crate::{
    files::temp_output_file::TempOutputFile,
    table::{Table, TableFormat, error::TableError},
};

#[cfg(test)]
use crate::table::_type::TableType;
#[cfg(test)]
use crate::table::empty::EmptyTable;
#[cfg(test)]
use object_store::path::PathPart;
#[cfg(test)]
use std::collections::HashMap;

pub mod files;
pub mod table;
mod table_runtime;
pub mod util;

pub use files::manager::FileManager;
pub use table_runtime::table_manager::TableManager;

pub mod prelude {
    pub use super::DataLake;
    pub use super::FileManager;
    pub use super::TableManager;
    pub use super::files::*;
}

pub struct DataLake {
    data_directory_store_url: ObjectStoreUrl,
    table_directory_store_url: ObjectStoreUrl,

    table_manager: Arc<TableManager>,
    file_manager: Arc<FileManager>,
}

impl Debug for DataLake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataLake")
            .field("data_directory_store_url", &self.data_directory_store_url)
            .field("table_directory_store_url", &self.table_directory_store_url)
            .field("table_count", &self.table_manager.table_names().len())
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
    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
    async fn order_tables(tables: &HashMap<String, TableFormat>) -> Vec<TableFormat> {
        table_runtime::ordering::order_tables(tables).await
    }

    #[inline(always)]
    pub fn try_create_listing_url(
        &self,
        path: String,
    ) -> datafusion::error::Result<ListingTableUrl> {
        self.file_manager.try_create_listing_url(path)
    }

    pub fn data_object_store_url(&self) -> ObjectStoreUrl {
        self.file_manager.data_object_store_url()
    }

    pub fn try_create_temp_output_file(&self, extension: &str) -> TempOutputFile {
        self.file_manager.try_create_temp_output_file(extension)
    }

    pub fn create_temp_output_file(extension: &str) -> TempOutputFile {
        FileManager::create_temp_output_file(extension)
    }

    pub fn table_manager(&self) -> Arc<TableManager> {
        self.table_manager.clone()
    }

    pub fn file_manager(&self) -> Arc<FileManager> {
        self.file_manager.clone()
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

        let file_formats =
            file_formats(session_context.clone(), get_datasets_object_store().await).unwrap();
        let runtime_handle = tokio::runtime::Handle::current();

        let table_manager = Arc::new(TableManager::new(
            runtime_handle,
            session_context.clone(),
            datasets_object_store_url.clone(),
            tables_object_store_url.clone(),
        ));
        let file_manager = Arc::new(FileManager::new(
            session_context,
            datasets_object_store_url.clone(),
            file_formats,
        ));

        Self {
            data_directory_store_url: datasets_object_store_url,
            table_directory_store_url: tables_object_store_url,
            table_manager,
            file_manager,
        }
    }

    pub async fn init_tables(&self) -> anyhow::Result<()> {
        self.table_manager.init_tables().await
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
                if let Err(e) = data_lake.init_tables().await {
                    tracing::error!("Failed to refresh tables: {}", e);
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
        self.file_manager
            .list_datasets(offset, limit, pattern)
            .await
    }

    pub async fn list_dataset_schema(
        &self,
        file_pattern: &str,
    ) -> datafusion::error::Result<SchemaRef> {
        self.file_manager.list_dataset_schema(file_pattern).await
    }

    pub fn list_table_schema(&self, table_name: &str) -> Option<SchemaRef> {
        self.table_manager.list_table_schema(table_name)
    }

    pub fn list_table(&self, table_name: &str) -> Option<TableFormat> {
        self.table_manager.list_table(table_name)
    }

    pub async fn update_table(&self, table: Table) -> Result<(), TableError> {
        self.table_manager.update_table(table).await
    }

    pub async fn create_table(&self, table: Table) -> Result<(), TableError> {
        self.table_manager.create_table(table).await
    }

    pub async fn refresh_table(&self, table_name: &str) -> anyhow::Result<()> {
        self.table_manager.refresh_table(table_name).await
    }

    pub async fn apply_operation(
        &self,
        table_name: &str,
        op: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.table_manager.apply_operation(table_name, op).await
    }

    pub async fn upload_file<S>(
        &self,
        file_path: &str,
        stream: S,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>
            + Unpin,
    {
        self.file_manager.upload_file(file_path, stream).await
    }

    pub async fn download_file(
        &self,
        file_path: &str,
    ) -> Result<
        BoxStream<'static, Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        self.file_manager.download_file(file_path).await
    }

    pub async fn delete_file(
        &self,
        file_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.file_manager.delete_file(file_path).await
    }
}

#[async_trait::async_trait]
impl SchemaProvider for DataLake {
    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.table_manager.table_exist(name)
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        self.table_manager.table_names()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.table_manager.table_provider(name))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        self.table_manager.register_table(name, table)
    }

    /// If supported by the implementation, removes the `name` table from this
    /// schema and returns the previously registered [`TableProvider`], if any.
    ///
    /// If no `name` table exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        self.table_manager.deregister_table(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{_type::TableType, merged::MergedTable};
    use beacon_datafusion_ext::table_ext::{ExternalTableDefinition, ViewTableDefinition};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_table(name: &str, table_type: TableType) -> Table {
        Table {
            table_directory: vec![],
            table_name: name.to_string(),
            table_type,
            description: None,
        }
    }

    fn test_legacy_table_format(name: &str, table_type: TableType) -> TableFormat {
        TableFormat::Legacy(test_table(name, table_type))
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

    #[tokio::test]
    async fn ordered_table_names_for_refresh_puts_merged_last() {
        let mut tables = HashMap::new();

        tables.insert(
            "base_a".to_string(),
            test_legacy_table_format("base_a", TableType::Empty(EmptyTable::new())),
        );

        tables.insert(
            "base_b".to_string(),
            test_legacy_table_format("base_b", TableType::Empty(EmptyTable::new())),
        );

        tables.insert(
            "merged_x".to_string(),
            test_legacy_table_format(
                "merged_x",
                TableType::Merged(MergedTable {
                    table_names: vec!["base_a".to_string(), "base_b".to_string()],
                }),
            ),
        );

        let order = DataLake::order_tables(&tables).await;

        let merged_positions = order
            .iter()
            .enumerate()
            .filter_map(|(idx, table)| (table.table_name() == "merged_x").then_some(idx))
            .collect::<Vec<_>>();
        let base_positions = order
            .iter()
            .enumerate()
            .filter_map(|(idx, table)| {
                ((table.table_name() == "base_a") || (table.table_name() == "base_b"))
                    .then_some(idx)
            })
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
    async fn ordered_definition_views_follow_table_scan_dependencies() {
        let mut tables = HashMap::new();

        let base = ExternalTableDefinition {
            name: "base_table".to_string(),
            location: "dataset/base_table/*.parquet".to_string(),
            file_type: "parquet".to_string(),
            schema: Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            definition: None,
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };

        let view_a = ViewTableDefinition {
            name: "view_a".to_string(),
            definition: "SELECT * FROM base_table".to_string(),
            dependencies: vec!["base_table".to_string()],
        };

        let view_b = ViewTableDefinition {
            name: "view_b".to_string(),
            definition: "SELECT * FROM view_a".to_string(),
            dependencies: vec!["view_a".to_string()],
        };

        tables.insert(
            base.name.clone(),
            TableFormat::DefinitionBased(Arc::new(base)),
        );
        tables.insert(
            view_a.name.clone(),
            TableFormat::DefinitionBased(Arc::new(view_a)),
        );
        tables.insert(
            view_b.name.clone(),
            TableFormat::DefinitionBased(Arc::new(view_b)),
        );

        let order = DataLake::order_tables(&tables).await;
        let ordered_names = order
            .iter()
            .map(TableFormat::table_name)
            .collect::<Vec<_>>();

        let base_pos = ordered_names
            .iter()
            .position(|name| *name == "base_table")
            .expect("base table should be present");
        let view_a_pos = ordered_names
            .iter()
            .position(|name| *name == "view_a")
            .expect("view_a should be present");
        let view_b_pos = ordered_names
            .iter()
            .position(|name| *name == "view_b")
            .expect("view_b should be present");

        assert!(base_pos < view_a_pos);
        assert!(view_a_pos < view_b_pos);
    }

    #[tokio::test(flavor = "multi_thread")]
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
            .init_tables()
            .await
            .expect("table initialization should succeed");

        assert!(reloaded.table_exist(&base_a_name));
        assert!(reloaded.table_exist(&base_b_name));
        assert!(reloaded.table_exist(&merged_name));

        let merged_table = reloaded
            .list_table(&merged_name)
            .expect("merged table metadata should be present");
        match merged_table {
            TableFormat::Legacy(table) => match table.table_type {
                TableType::Merged(merged) => {
                    assert_eq!(
                        merged.table_names,
                        vec![base_a_name.clone(), base_b_name.clone()]
                    );
                }
                other => panic!("expected merged legacy table, got {other:?}"),
            },
            other => panic!("expected legacy table format, got {other:?}"),
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
            .deregister_table(&merged_name)
            .expect("merged table cleanup should succeed");
        reloaded
            .deregister_table(&base_a_name)
            .expect("base table A cleanup should succeed");
        reloaded
            .deregister_table(&base_b_name)
            .expect("base table B cleanup should succeed");
    }
}
