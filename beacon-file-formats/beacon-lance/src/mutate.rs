//! Native row mutations for managed Lance tables (`DELETE` / `UPDATE`).
//!
//! Unlike the copy-on-write path (which rewrites the whole table with the
//! surviving/updated rows), these use Lance's native operations: `delete` marks
//! rows via deletion vectors, and `UpdateBuilder` rewrites only affected
//! fragments. Both commit a new dataset version. Predicates and SET values are
//! SQL strings, which Lance parses against the dataset schema.

use std::sync::Arc;

use lance::dataset::builder::DatasetBuilder;
use lance::dataset::write::update::UpdateBuilder;

use crate::warehouse::LanceWarehouse;

/// `DELETE FROM t [WHERE predicate]`. `None` deletes every row.
pub async fn delete_rows(
    warehouse: &LanceWarehouse,
    uri: &str,
    predicate: Option<&str>,
) -> anyhow::Result<()> {
    tracing::info!(uri = %uri, predicate = ?predicate, "deleting Lance rows");

    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let mut dataset = DatasetBuilder::from_uri(uri)
        .with_session(warehouse.session())
        .load()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;

    dataset
        .delete(predicate.unwrap_or("true"))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to delete rows from '{uri}': {e}"))?;
    Ok(())
}

/// `UPDATE t SET <assignments> [WHERE predicate]`. `assignments` are
/// `(column, value_sql)` pairs; `predicate` `None` updates every row.
pub async fn update_rows(
    warehouse: &LanceWarehouse,
    uri: &str,
    predicate: Option<&str>,
    assignments: &[(String, String)],
) -> anyhow::Result<()> {
    if assignments.is_empty() {
        return Ok(());
    }
    tracing::info!(uri = %uri, predicate = ?predicate, cols = assignments.len(), "updating Lance rows");

    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let dataset = DatasetBuilder::from_uri(uri)
        .with_session(warehouse.session())
        .load()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;

    let mut builder = UpdateBuilder::new(Arc::new(dataset));
    if let Some(predicate) = predicate {
        builder = builder
            .update_where(predicate)
            .map_err(|e| anyhow::anyhow!("Invalid UPDATE predicate '{predicate}': {e}"))?;
    }
    for (column, value) in assignments {
        builder = builder
            .set(column, value)
            .map_err(|e| anyhow::anyhow!("Invalid SET {column} = {value}: {e}"))?;
    }
    let job = builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Lance update for '{uri}': {e}"))?;
    job.execute()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to update rows in '{uri}': {e}"))?;
    Ok(())
}
