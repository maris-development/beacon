use std::sync::Arc;

use beacon_datafusion_ext::table_ext::TableDefinition;
use object_store::{ObjectStore, ObjectStoreExt};

mod legacy;

/// Reads a `table.json` config from the tables object store and parses it as a
/// serializable [`TableDefinition`].
///
/// Tries the current typetag format first. If that fails, falls back to the
/// pre-typetag `{ "table_name", "table_type": { "logical": ... } }` envelope used
/// before logical tables were removed, mapping it onto a
/// [`LogicalTableDefinition`](beacon_datafusion_ext::table_ext::LogicalTableDefinition)
/// so old catalogs keep loading. Only the `logical` variant is supported as a
/// fallback; other legacy table types are reported as an error.
pub async fn try_open(
    store: Arc<dyn ObjectStore>,
    table_json: object_store::path::Path,
) -> anyhow::Result<Arc<dyn TableDefinition>> {
    let json_bytes = store
        .get(&table_json)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read table config: {:?}", e))?
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read table config bytes: {:?}", e))?;

    if let Ok(definition) = serde_json::from_slice::<Arc<dyn TableDefinition>>(&json_bytes) {
        return Ok(definition);
    }

    // Not the current typetag format — try the legacy logical-table envelope.
    if let Some(definition) = legacy::logical_definition_from_legacy_json(&json_bytes)? {
        return Ok(Arc::new(definition));
    }

    anyhow::bail!(
        "Failed to parse table config at {}: not a recognized table definition",
        table_json
    )
}
