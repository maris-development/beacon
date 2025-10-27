use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, functions::core::version,
    prelude::SessionContext,
};
use iceberg::{
    Catalog, CatalogBuilder, MemoryCatalog, TableIdent,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::TableMetadata,
};

use crate::table::error::TableError;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IcebergTable {
    table_identifier: String,
    table_alias: Option<String>,
    #[serde(default = "default_read_only")]
    read_only: bool,
    #[serde(default)]
    snapshot_id: Option<i64>,
}

fn default_read_only() -> bool {
    true
}

impl IcebergTable {
    const CATALOG_NAME: &'static str = "iceberg";

    pub async fn table_provider(
        &self,
        session_ctx: Arc<SessionContext>,
        data_directory_store_url: ObjectStoreUrl,
        data_directory_prefix: object_store::path::Path,
        table_directory_store_url: ObjectStoreUrl,
        table_directory_prefix: object_store::path::Path,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        // Get the object store
        let object_store = session_ctx
            .runtime_env()
            .object_store(&data_directory_store_url)
            .unwrap();

        let catalog_builder = MemoryCatalogBuilder::default();
        let catalog = catalog_builder
            .load(
                Self::CATALOG_NAME,
                vec![(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    data_directory_store_url.to_string(),
                )]
                .into_iter()
                .collect(),
            )
            .await
            .unwrap();

        // let version_hint_path = table_path.child("metadata").child("version-hint.text");

        // // Initialize Iceberg table provider

        // // let file_io = ice
        // let builder = iceberg::table::Table::builder();

        // builder.metadata(metadata)

        // catalog.load_table(table)

        todo!()
    }
}
