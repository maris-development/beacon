use std::sync::Arc;

use beacon_datafusion_ext::table_ext::TableDefinition;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};

pub async fn load_tables_from_object_store(
    tables_object_store: Arc<dyn ObjectStore>,
) -> Vec<Arc<dyn TableDefinition>> {
    let mut discovered_tables = Vec::new();
    let mut entry_stream = tables_object_store.list(None);

    let paths = entry_stream
        .by_ref()
        .filter_map(|entry| async move {
            match entry {
                Ok(entry) => Some(entry.location),
                Err(error) => {
                    tracing::error!("Failed to list table directory: {}", error);
                    None
                }
            }
        })
        .collect::<Vec<_>>()
        .await;

    let table_json_paths: Vec<_> = paths
        .into_iter()
        .filter(|location| location.filename() == Some("table.json"))
        .collect();

    for table_json in table_json_paths {
        match open_table_definition(&tables_object_store, &table_json).await {
            Ok(table) => discovered_tables.push(table),
            Err(error) => tracing::error!("Failed to load table from object store: {}", error),
        }
    }

    discovered_tables
}

/// Read a `table.json` and parse it as a serializable [`TableDefinition`] (the
/// typetag format every managed definition is persisted in).
async fn open_table_definition(
    store: &Arc<dyn ObjectStore>,
    table_json: &object_store::path::Path,
) -> anyhow::Result<Arc<dyn TableDefinition>> {
    let json_bytes = store
        .get(table_json)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read table config: {:?}", e))?
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read table config bytes: {:?}", e))?;

    serde_json::from_slice::<Arc<dyn TableDefinition>>(&json_bytes).map_err(|e| {
        anyhow::anyhow!(
            "Failed to parse table config at {}: {}",
            table_json,
            e
        )
    })
}

#[cfg(test)]
mod tests {
    use super::load_tables_from_object_store;
    use beacon_datafusion_ext::table_ext::{TableDefinition, ViewTableDefinition};
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
    use std::sync::Arc;

    fn view_definition(name: &str) -> Arc<dyn TableDefinition> {
        Arc::new(ViewTableDefinition {
            name: name.to_string(),
            definition: "SELECT 1 AS x".to_string(),
            dependencies: vec![],
        })
    }

    #[tokio::test]
    async fn load_tables_only_reads_table_json_and_skips_invalid_entries() {
        let store = InMemory::new();

        let valid = serde_json::to_vec(&view_definition("ok_table"))
            .expect("valid test table definition should serialize");
        store
            .put(&Path::from("ok_table/table.json"), valid.into())
            .await
            .expect("valid table.json should be written");

        store
            .put(
                &Path::from("ok_table/not_table.json"),
                br#"{"ignored":true}"#.to_vec().into(),
            )
            .await
            .expect("non table.json fixture should be written");

        store
            .put(
                &Path::from("broken/table.json"),
                br#"{"invalid": ["json"}"#.to_vec().into(),
            )
            .await
            .expect("invalid table.json fixture should be written");

        let loaded = load_tables_from_object_store(Arc::new(store)).await;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].table_name(), "ok_table");
    }
}
