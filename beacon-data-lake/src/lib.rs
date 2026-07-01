use std::sync::LazyLock;

use beacon_object_storage::ObjectStores;
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};
use url::Url;

pub mod crawler;
pub mod file_formats;
pub mod files;
pub mod table;
mod table_runtime;

pub use file_formats::file_formats;
pub use files::temp_output_file::TempOutputFile;
pub use files::{create_listing_url, create_temp_output_file, list_dataset_schema, list_datasets};
pub use table_runtime::init_tables;
pub use table_runtime::persistent_schema_provider::PersistentSchemaProvider;
pub use table_runtime::schema_persistence::{definition_from_provider, SchemaPersistenceService};

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

/// Register beacon's custom object stores on the session context.
///
/// The runtime owns the [`ObjectStores`] (built from the storage config, not a
/// process-global) and passes them in; this registers the `datasets://`,
/// `internal://` (materialized-view data), `tables://` (table definitions) and
/// `tmp://` (query output) URLs so DataFusion can resolve them. Must be called
/// before any table or dataset access.
pub fn register_object_stores(
    session_context: &SessionContext,
    object_stores: &ObjectStores,
) -> anyhow::Result<()> {
    let datasets_object_store = object_stores.datasets.clone();
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
    session_context.register_object_store(
        &Url::parse(TABLES_OBJECT_STORE_URL.as_str()).unwrap(),
        object_stores.tables.clone(),
    );
    // Register tmp object store
    session_context.register_object_store(
        &Url::parse(TMP_OBJECT_STORE_URL.as_str()).unwrap(),
        object_stores.tmp.clone(),
    );

    Ok(())
}
