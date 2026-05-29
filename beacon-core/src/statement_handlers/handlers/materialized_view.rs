//! Shared helpers for materialized-view statement handling.

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_data_lake::DATASETS_OBJECT_STORE_URL;
use datafusion::{
    config::TableParquetOptions,
    datasource::{
        file_format::parquet::ParquetSink,
        listing::ListingTableUrl,
        physical_plan::{FileGroup, FileSinkConfig},
        sink::DataSinkExec,
    },
    logical_expr::dml::InsertOp,
    parquet::arrow::ArrowWriter,
    prelude::{DataFrame, SessionContext},
};
use futures::StreamExt;

/// Current time as Unix epoch milliseconds (0 if the clock is before the epoch).
pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Writes a query's result to Parquet under `dir_path` (relative to the datasets
/// object store) and returns the output schema.
///
/// Uses an explicit `object_store_url` in the sink config — the same pattern as
/// `BeaconTable` — so the store is resolved by scheme (`datasets://`) rather than
/// from the URL authority, which would otherwise be parsed from the path prefix.
pub(crate) async fn write_query_to_datasets_parquet(
    session_ctx: &SessionContext,
    df: DataFrame,
    dir_path: &str,
) -> anyhow::Result<SchemaRef> {
    let store_url = DATASETS_OBJECT_STORE_URL.clone();
    let physical_plan = df.create_physical_plan().await?;
    let output_schema = physical_plan.schema();

    let original_url = format!("{store_url}{dir_path}");
    let parsed_url = ListingTableUrl::parse(&original_url)?;

    let sink_config = FileSinkConfig {
        original_url,
        object_store_url: store_url.clone(),
        file_group: FileGroup::default(),
        table_paths: vec![parsed_url],
        output_schema: output_schema.clone(),
        table_partition_cols: vec![],
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension: "parquet".to_string(),
    };

    let sink = Arc::new(ParquetSink::new(sink_config, TableParquetOptions::default()));
    let sink_exec = Arc::new(DataSinkExec::new(physical_plan, sink, None));
    let task_ctx = session_ctx.task_ctx();
    datafusion::physical_plan::collect(sink_exec, task_ctx).await?;

    // Guarantee at least one Parquet file exists, even when the query produced zero
    // rows (DataFusion's sink writes no file for an empty result). Otherwise the
    // view's ListingTable would have no file to infer a schema from, and reads /
    // refresh would fail. The empty file carries the schema, so reads return zero rows.
    let store = session_ctx.runtime_env().object_store(&store_url)?;
    let prefix = object_store::path::Path::from(dir_path.trim_end_matches('/'));
    let wrote_any = store.list(Some(&prefix)).next().await.is_some();
    if !wrote_any {
        let mut buffer: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, output_schema.clone(), None)?;
        writer.write(&RecordBatch::new_empty(output_schema.clone()))?;
        writer.close()?;
        store.put(&prefix.child("part-0.parquet"), buffer.into()).await?;
    }

    Ok(output_schema)
}

/// Best-effort recursive delete of every object under `prefix` in the datasets
/// object store. Failures are logged rather than propagated, since this is only
/// used to reclaim space for replaced/dropped materialized-view data.
pub(crate) async fn delete_datasets_prefix(session_ctx: &SessionContext, prefix: &str) {
    let store_url = DATASETS_OBJECT_STORE_URL.clone();
    let store = match session_ctx.runtime_env().object_store(&store_url) {
        Ok(store) => store,
        Err(error) => {
            tracing::warn!("Failed to resolve datasets object store for cleanup of '{prefix}': {error}");
            return;
        }
    };

    let path = object_store::path::Path::from(prefix.trim_end_matches('/'));
    let mut listing = store.list(Some(&path));
    while let Some(entry) = listing.next().await {
        match entry {
            Ok(meta) => {
                if let Err(error) = store.delete(&meta.location).await {
                    tracing::warn!("Failed to delete '{}' during cleanup: {error}", meta.location);
                }
            }
            Err(error) => {
                tracing::warn!("Failed to list '{prefix}' during cleanup: {error}");
            }
        }
    }
}
