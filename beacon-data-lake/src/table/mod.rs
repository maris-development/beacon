use std::sync::Arc;

use beacon_datafusion_ext::table_ext::TableDefinition;
use object_store::{ObjectStore, ObjectStoreExt};

/// Reads a `table.json` config from the tables object store and parses it as a
/// serializable [`TableDefinition`].
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

    let definition = serde_json::from_slice::<Arc<dyn TableDefinition>>(&json_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to parse table config: {:?}", e))?;

    Ok(definition)
}
