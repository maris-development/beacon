use std::sync::LazyLock;

use beacon_object_storage::get_datasets_object_store;
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};
use url::Url;

pub mod files;
pub mod table;
mod table_runtime;

pub use files::temp_output_file::TempOutputFile;
pub use files::{create_listing_url, create_temp_output_file, list_dataset_schema, list_datasets};
pub use table_runtime::init_tables;
pub use table_runtime::persistent_schema_provider::PersistentSchemaProvider;
pub use table_runtime::schema_persistence::definition_from_provider;

pub mod prelude {
    pub use super::files::*;
    pub use super::{
        definition_from_provider, init_tables, register_object_stores, PersistentSchemaProvider,
    };
}

pub static DATASETS_OBJECT_STORE_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse("datasets://").expect("Failed to parse datasets URL"));
pub static TABLES_OBJECT_STORE_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse("tables://").expect("Failed to parse tables URL"));
pub static TMP_OBJECT_STORE_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse("tmp://").expect("Failed to parse tmp URL"));
pub static INDEX_OBJECT_STORE_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse("index://").expect("Failed to parse index URL")); // ToDo: implement indexing on top of existing files utilizing the notified storage events.

/// Initialize and register beacon's custom object stores on the session context.
///
/// Registers the `datasets://`, `internal://` (materialized-view data),
/// `tables://` (table definitions) and `tmp://` (query output) stores. Must be
/// called before any table or dataset access.
pub async fn register_object_stores(session_context: &SessionContext) -> anyhow::Result<()> {
    // Init the data stores if they have not been initialized yet.
    beacon_object_storage::init_datastores()
        .await
        .map_err(|error| anyhow::anyhow!("Failed to initialize Data Lake Engine: {}", error))?;

    let datasets_object_store = get_datasets_object_store().await;
    // Register the Beacon-internal store (rooted at the `__beacon__` prefix)
    // used by materialized views to persist and read their data directly,
    // bypassing the datasets store's user-facing hiding and metadata cache.
    session_context.register_object_store(
        &Url::parse(beacon_datafusion_ext::table_ext::INTERNAL_STORE_URL).unwrap(),
        datasets_object_store.internal_store(),
    );
    // Register datasets object store
    session_context.register_object_store(
        &Url::parse(DATASETS_OBJECT_STORE_URL.as_str()).unwrap(),
        datasets_object_store,
    );
    // Register tables object store
    let tables_object_store = beacon_object_storage::get_tables_object_store().await;
    session_context.register_object_store(
        &Url::parse(TABLES_OBJECT_STORE_URL.as_str()).unwrap(),
        tables_object_store,
    );
    // Register tmp object store
    let tmp_object_store = beacon_object_storage::get_tmp_object_store().await;
    session_context.register_object_store(
        &Url::parse(TMP_OBJECT_STORE_URL.as_str()).unwrap(),
        tmp_object_store,
    );

    Ok(())
}
