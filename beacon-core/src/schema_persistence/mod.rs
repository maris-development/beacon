//! Schema management: the persistent catalog and everything that rebuilds it.
//!
//! Beacon's catalog is the source of truth for which tables exist. This module owns:
//! - [`PersistentSchemaProvider`] — the `beacon.public` schema provider that persists
//!   a table's definition on registration and removes it on deregistration;
//! - [`SchemaPersistenceService`] — the durable `db://<name>/table.json` read/write path;
//! - [`init_tables`] — startup recovery that rebuilds every provider from those files;
//! - the private `loading`/`ordering` helpers `init_tables` drives.

use std::{collections::HashMap, sync::Arc};

use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};

mod loading;
mod ordering;
pub mod provider;
pub mod service;

pub use provider::PersistentSchemaProvider;
pub use service::{definition_from_provider, SchemaPersistenceService};

/// Rebuild the catalog from the persisted table definitions.
///
/// Loads every `db://<name>/table.json`, orders the definitions so a table
/// is registered after the tables it depends on (base tables, then temporary
/// definitions, then views in dependency order), builds each provider against
/// the live session — so a view's defining query resolves the tables already
/// registered ahead of it — and inserts it into `schema` without re-persisting.
/// Finishes by ensuring the empty `default` table exists.
///
/// `tables_store_url` is the store the persisted `<name>/table.json` definitions
/// are read from (the caller supplies it — the runtime uses its tables store).
///
/// `datasets_url` is the store a persisted definition's relative location is
/// resolved against; it must match the URL the definition was created under, or
/// the rebuilt provider lists a different (likely empty) set of files.
pub async fn init_tables(
    session_ctx: &Arc<SessionContext>,
    schema: &PersistentSchemaProvider,
    tables_store_url: &ObjectStoreUrl,
) -> anyhow::Result<()> {
    tracing::info!("Initializing tables from object store");
    let tables_object_store = session_ctx
        .runtime_env()
        .object_store(tables_store_url)
        .map_err(|error| anyhow::anyhow!("Failed to get tables object store: {}", error))?;

    let discovered = loading::load_tables_from_object_store(tables_object_store).await;
    let table_map = discovered
        .into_iter()
        .map(|table| (table.table_name().to_string(), table))
        .collect::<HashMap<String, Arc<dyn TableDefinition>>>();
    let ordered = ordering::order_tables(&table_map).await;

    for definition in ordered {
        let table_name = definition.table_name().to_string();
        match definition.build_provider(session_ctx.clone()).await {
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
