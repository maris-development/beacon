//! Size figures of a managed Lance table.

use futures::TryStreamExt;
use object_store::ObjectStore;

use crate::provider::open_dataset;
use crate::warehouse::LanceWarehouse;

/// The size of one Lance table, read from its latest version and from the store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableSize {
    /// Rows in the latest version.
    pub rows: u64,
    /// Fragments in the latest version.
    pub fragments: u64,
    /// Versions the dataset still holds.
    pub versions: u64,
    /// Bytes of all objects of the table: data of all versions, indexes and manifests.
    pub bytes: u64,
}

/// Measure the Lance table at `uri`.
///
/// # Arguments
///
/// * `warehouse` - The warehouse that holds the table
/// * `uri` - The `db://` location of the table
///
/// # Returns
///
/// The row, fragment and version counts, and the bytes the table holds in the store
///
/// # Errors
///
/// Returns an error if the dataset does not open or if the store listing fails
pub async fn table_size(warehouse: &LanceWarehouse, uri: &str) -> anyhow::Result<TableSize> {
    let dataset = open_dataset(uri, warehouse.session())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;
    let prefix = LanceWarehouse::object_path(uri);

    let rows = async {
        dataset
            .count_rows(None)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to count the rows of '{uri}': {e}"))
    };
    let versions = async {
        dataset
            .versions()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list the versions of '{uri}': {e}"))
    };
    let bytes = async {
        warehouse
            .store()
            .list(Some(&prefix))
            .try_fold(0u64, |total, object| async move { Ok(total + object.size) })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list the files of '{uri}': {e}"))
    };
    // The three reads are independent, so they run at the same time.
    let (rows, versions, bytes) = futures::try_join!(rows, versions, bytes)?;

    Ok(TableSize {
        rows: rows as u64,
        fragments: dataset.count_fragments() as u64,
        versions: versions.len() as u64,
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use object_store::memory::InMemory;

    use super::*;
    use crate::io::{WriteKind, write_stream};
    use crate::{beacon_namespace, create_lance_table};

    async fn append(warehouse: &LanceWarehouse, uri: &str, schema: &Arc<Schema>, ids: Vec<i64>) {
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))])
            .expect("the batch matches the schema");
        let stream =
            RecordBatchStreamAdapter::new(schema.clone(), futures::stream::iter([Ok(batch)]));
        write_stream(
            uri,
            warehouse.session(),
            Box::pin(stream),
            WriteKind::Append,
        )
        .await
        .expect("the append succeeds");
    }

    #[tokio::test]
    async fn table_size_counts_rows_fragments_versions_and_bytes() {
        let warehouse = Arc::new(LanceWarehouse::new(Arc::new(InMemory::new())));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table = create_lance_table(warehouse.clone(), &beacon_namespace(), "sized", &schema)
            .await
            .expect("the table is created");
        let uri = table.definition().location.clone();
        let empty = table_size(&warehouse, &uri).await.expect("an empty size");

        append(&warehouse, &uri, &schema, vec![1, 2, 3]).await;
        append(&warehouse, &uri, &schema, vec![4, 5]).await;
        let size = table_size(&warehouse, &uri).await.expect("a size");

        assert_eq!((empty.rows, empty.fragments, empty.versions), (0, 0, 1));
        assert_eq!((size.rows, size.fragments, size.versions), (5, 2, 3));
        assert!(size.bytes > empty.bytes, "the appends add bytes: {size:?}");
    }

    #[tokio::test]
    async fn table_size_ignores_a_table_with_a_longer_name() {
        let warehouse = Arc::new(LanceWarehouse::new(Arc::new(InMemory::new())));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let namespace = beacon_namespace();
        let short = create_lance_table(warehouse.clone(), &namespace, "t", &schema)
            .await
            .expect("the table is created");
        let uri = short.definition().location.clone();
        let alone = table_size(&warehouse, &uri).await.expect("a size");

        let long = create_lance_table(warehouse.clone(), &namespace, "t.lance2", &schema)
            .await
            .expect("the table is created");
        append(&warehouse, &long.definition().location, &schema, vec![1]).await;

        let size = table_size(&warehouse, &uri).await.expect("a size");
        assert_eq!(size, alone);
    }
}
