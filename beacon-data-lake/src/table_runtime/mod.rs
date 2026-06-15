use std::{collections::HashMap, sync::Arc};

use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::prelude::SessionContext;

use crate::{DATASETS_OBJECT_STORE_URL, TABLES_OBJECT_STORE_URL};

pub mod loading;
pub mod ordering;
pub mod persistent_schema_provider;
pub mod schema_persistence;

pub use persistent_schema_provider::PersistentSchemaProvider;

/// Rebuild the catalog from the persisted table definitions.
///
/// Loads every `tables://<name>/table.json`, orders the definitions so a table
/// is registered after the tables it depends on (base tables, then temporary
/// definitions, then views in dependency order), builds each provider against
/// the live session — so a view's defining query resolves the tables already
/// registered ahead of it — and inserts it into `schema` without re-persisting.
/// Finishes by ensuring the empty `default` table exists.
pub async fn init_tables(
    session_ctx: &Arc<SessionContext>,
    schema: &PersistentSchemaProvider,
) -> anyhow::Result<()> {
    tracing::info!("Initializing tables from object store");
    let tables_object_store = session_ctx
        .runtime_env()
        .object_store(&*TABLES_OBJECT_STORE_URL)
        .map_err(|error| anyhow::anyhow!("Failed to get tables object store: {}", error))?;

    let discovered = loading::load_tables_from_object_store(tables_object_store).await;
    let table_map = discovered
        .into_iter()
        .map(|table| (table.table_name().to_string(), table))
        .collect::<HashMap<String, Arc<dyn TableDefinition>>>();
    let ordered = ordering::order_tables(&table_map).await;

    for definition in ordered {
        let table_name = definition.table_name().to_string();
        match definition
            .build_provider(session_ctx.clone(), &DATASETS_OBJECT_STORE_URL)
            .await
        {
            Ok(provider) => {
                let _ = schema.insert_loaded(table_name.clone(), provider);
                tracing::info!("Registered table '{}'", table_name);
            }
            Err(error) => {
                tracing::error!(
                    "Failed to build provider for table '{}': {}. Skipping registration of this table.",
                    table_name,
                    error
                );
            }
        }
    }

    schema.ensure_default_table();
    Ok(())
}
