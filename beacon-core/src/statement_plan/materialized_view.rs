//! Materialized-view side effects, executed by the statement-plan exec nodes.
//!
//! These functions hold the actual work for `CREATE MATERIALIZED VIEW` and
//! `REFRESH`: running the defining query, persisting its result as Parquet in the
//! internal object store, and registering/replacing the catalog provider. They
//! are invoked from the corresponding [`super::physical`] execution-plan nodes.

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_datafusion_ext::table_ext::{
    internal_object_store_url, ExternalTable, MaterializedView, MaterializedViewDefinition,
    TableDefinition,
};
use datafusion::{
    dataframe::DataFrameWriteOptions,
    parquet::arrow::ArrowWriter,
    prelude::{DataFrame, SessionContext},
    sql::TableReference,
};
use futures::StreamExt;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt};

/// Current time as Unix epoch milliseconds (0 if the clock is before the epoch).
pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Create a materialized view: run its defining query, persist the result, and
/// register the provider in the catalog.
///
/// Errors if a table/view with `name` already exists.
pub(crate) async fn create_materialized_view(
    session_ctx: &Arc<SessionContext>,
    name: &str,
    query_sql: &str,
) -> anyhow::Result<()> {
    let table_ref = TableReference::parse_str(name);

    if session_ctx.table_exist(table_ref.clone())? {
        return Err(anyhow::anyhow!("Materialized view '{name}' already exists"));
    }

    // Execute the defining query and persist its result as a single Parquet file.
    let df = session_ctx.sql(query_sql).await?;
    let dir_path = format!("{}/{}", name, uuid::Uuid::new_v4());
    let schema = write_query_to_datasets_parquet(session_ctx, df, &dir_path).await?;
    let storage_location = format!("{dir_path}/");

    let now = now_millis();
    let definition = MaterializedViewDefinition {
        name: name.to_string(),
        definition: query_sql.to_string(),
        schema,
        storage_location,
        created_at: now,
        last_refreshed: Some(now),
    };

    let store_url = internal_object_store_url();
    let provider = definition
        .build_provider(session_ctx.clone(), &store_url)
        .await?;

    // Registering through the session context persists the definition to the
    // catalog (tables://<name>/table.json) via the schema provider.
    session_ctx.register_table(table_ref, provider)?;

    tracing::info!("Created materialized view '{name}'");
    Ok(())
}

/// Refresh an external table or a materialized view by `name`.
///
/// Errors if `name` does not resolve to a refreshable provider.
pub(crate) async fn refresh_table(
    session_ctx: &Arc<SessionContext>,
    name: &str,
) -> anyhow::Result<()> {
    let table_ref = TableReference::parse_str(name);
    let provider = session_ctx
        .table_provider(table_ref)
        .await
        .map_err(|_| anyhow::anyhow!("'{name}' does not exist"))?;

    if let Some(external) = provider.as_any().downcast_ref::<ExternalTable>() {
        external.refresh().await?;
    } else if provider.as_any().is::<MaterializedView>() {
        refresh_materialized_view(session_ctx, name).await?;
    } else {
        return Err(anyhow::anyhow!(
            "'{name}' is not a materialized view or external table; REFRESH is only supported for those"
        ));
    }

    Ok(())
}

/// Writes a query's result as a single Parquet file inside `dir_path` (relative to
/// the internal object store) and returns the output schema.
///
/// Uses [`DataFrame::write_parquet`] with single-file output, so the directory
/// holds exactly one `part.parquet` rather than a fan of part files. The view
/// reads it back by listing `dir_path/`.
pub(crate) async fn write_query_to_datasets_parquet(
    session_ctx: &SessionContext,
    df: DataFrame,
    dir_path: &str,
) -> anyhow::Result<SchemaRef> {
    let store_url = internal_object_store_url();
    let output_schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());

    // Write the result as a single Parquet file inside the version directory. The
    // view reads it back by listing `dir_path/`, which round-trips cleanly through
    // the internal prefix store (unlike a bare single-file path).
    let file_path = format!("{}/part.parquet", dir_path.trim_end_matches('/'));
    let file_url = format!("{store_url}{file_path}");
    df.write_parquet(
        &file_url,
        DataFrameWriteOptions::new().with_single_file_output(true),
        None,
    )
    .await?;

    // A zero-row result produces no file (DataFusion writes nothing for an empty
    // stream). Write an empty Parquet (schema only) so the view always has a file to
    // read and infer its schema from; reads then return zero rows.
    let store = session_ctx.runtime_env().object_store(&store_url)?;
    let path = object_store::path::Path::from(file_path);
    let exists = store
        .get_opts(
            &path,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .is_ok();
    if !exists {
        let mut buffer: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, output_schema.clone(), None)?;
        writer.write(&RecordBatch::new_empty(output_schema.clone()))?;
        writer.close()?;
        store.put(&path, buffer.into()).await?;
    }

    Ok(output_schema)
}

/// Best-effort recursive delete of every object under `prefix` in the internal
/// object store. Failures are logged rather than propagated, since this is only
/// used to reclaim space for replaced/dropped materialized-view data.
pub(crate) async fn delete_datasets_prefix(session_ctx: &SessionContext, prefix: &str) {
    let store_url = internal_object_store_url();
    let store = match session_ctx.runtime_env().object_store(&store_url) {
        Ok(store) => store,
        Err(error) => {
            tracing::warn!(
                "Failed to resolve internal object store for cleanup of '{prefix}': {error}"
            );
            return;
        }
    };

    let path = object_store::path::Path::from(prefix.trim_end_matches('/'));
    let mut listing = store.list(Some(&path));
    while let Some(entry) = listing.next().await {
        match entry {
            Ok(meta) => {
                if let Err(error) = store.delete(&meta.location).await {
                    tracing::warn!(
                        "Failed to delete '{}' during cleanup: {error}",
                        meta.location
                    );
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
/// Re-runs the view's stored SQL into a fresh versioned Parquet file, so the
/// existing data remains usable if anything fails before the swap, then reclaims
/// the old file.
///
/// Errors if `name` does not resolve to a `MaterializedView` provider.
pub(crate) async fn refresh_materialized_view(
    session_ctx: &Arc<SessionContext>,
    name: &str,
) -> anyhow::Result<()> {
    let table_ref = TableReference::parse_str(name);

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
    let dir_path = format!("{}/{}", name, uuid::Uuid::new_v4());
    let schema = write_query_to_datasets_parquet(session_ctx, df, &dir_path).await?;

    let new_definition = MaterializedViewDefinition {
        name: name.to_string(),
        definition: old_definition.definition.clone(),
        schema,
        storage_location: format!("{dir_path}/"),
        created_at: old_definition.created_at,
        last_refreshed: Some(now_millis()),
    };

    let store_url = internal_object_store_url();
    let new_provider = new_definition
        .build_provider(session_ctx.clone(), &store_url)
        .await?;

    session_ctx.register_table(table_ref, new_provider)?;

    delete_datasets_prefix(session_ctx, &old_definition.storage_location).await;

    tracing::info!("Refreshed materialized view '{name}'");
    Ok(())
}
