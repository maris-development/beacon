//! Shared helpers for materialized-view statement handling.

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_data_lake::DATASETS_OBJECT_STORE_URL;
use beacon_datafusion_ext::table_ext::{
    MaterializedView, MaterializedViewDefinition, TableDefinition,
};
use datafusion::{
    config::TableParquetOptions,
    datasource::{
        file_format::parquet::ParquetSink,
        listing::ListingTableUrl,
        physical_plan::{FileGroup, FileOutputMode, FileSinkConfig},
        sink::DataSinkExec,
    },
    logical_expr::dml::InsertOp,
    parquet::arrow::ArrowWriter,
    prelude::{DataFrame, SessionContext},
    sql::{TableReference, sqlparser::ast::ObjectName},
};
use futures::StreamExt;
use object_store::ObjectStoreExt;

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
        file_output_mode: FileOutputMode::SingleFile,
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

/// Recompute a materialized view and atomically swap the catalog pointer.
///
/// Re-runs the view's stored SQL into a fresh versioned Parquet directory, so
/// the existing data remains usable if anything fails before the swap, then
/// reclaims the old directory.
///
/// Errors if `view_name` does not resolve to a `MaterializedView` provider.
pub(crate) async fn refresh_materialized_view(
    session_ctx: &Arc<SessionContext>,
    view_name: &ObjectName,
) -> anyhow::Result<()> {
    let name = view_name.to_string();
    let table_ref = TableReference::parse_str(&name);

    let provider = session_ctx
        .table_provider(table_ref.clone())
        .await
        .map_err(|_| anyhow::anyhow!("Materialized view '{name}' does not exist"))?;

    let old_definition = provider
        .as_any()
        .downcast_ref::<MaterializedView>()
        .ok_or_else(|| anyhow::anyhow!("Object '{name}' is not a materialized view"))?
        .definition()
        .clone();

    let df = session_ctx.sql(&old_definition.definition).await?;
    let dir_path = format!("__beacon__/{}/{}", name, uuid::Uuid::new_v4());
    let schema = write_query_to_datasets_parquet(session_ctx, df, &dir_path).await?;
    let new_storage_location = format!("{dir_path}/");

    let new_definition = MaterializedViewDefinition {
        name: name.clone(),
        definition: old_definition.definition.clone(),
        schema,
        storage_location: new_storage_location,
        created_at: old_definition.created_at,
        last_refreshed: Some(now_millis()),
    };

    let store_url = DATASETS_OBJECT_STORE_URL.clone();
    let new_provider = new_definition
        .build_provider(session_ctx.clone(), &store_url)
        .await?;

    session_ctx.register_table(table_ref, new_provider)?;

    delete_datasets_prefix(session_ctx, &old_definition.storage_location).await;

    tracing::info!("Refreshed materialized view '{name}'");
    Ok(())
}
