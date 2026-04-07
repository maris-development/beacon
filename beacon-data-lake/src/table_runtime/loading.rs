use std::sync::Arc;

use futures::StreamExt;
use object_store::ObjectStore;

use crate::table::TableFormat;

pub async fn load_tables_from_object_store(
    tables_object_store: Arc<dyn ObjectStore>,
) -> Vec<TableFormat> {
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
        match TableFormat::try_open(tables_object_store.clone(), table_json).await {
            Ok(table) => discovered_tables.push(table),
            Err(error) => tracing::error!("Failed to load table from object store: {}", error),
        }
    }

    discovered_tables
}

#[cfg(test)]
mod tests {
    use super::load_tables_from_object_store;
    use crate::table::{_type::TableType, Table, TableFormat, empty::EmptyTable};
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    fn legacy_table_format(name: &str) -> TableFormat {
        TableFormat::Legacy(Table {
            table_directory: vec![],
            table_name: name.to_string(),
            table_type: TableType::Empty(EmptyTable::new()),
            description: Some("loading test table".to_string()),
        })
    }

    #[tokio::test]
    async fn load_tables_only_reads_table_json_and_skips_invalid_entries() {
        let store = InMemory::new();

        let valid = serde_json::to_vec(&legacy_table_format("ok_table"))
            .expect("valid test table format should serialize");
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

        let loaded = load_tables_from_object_store(std::sync::Arc::new(store)).await;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].table_name(), "ok_table");
    }
}
