use async_trait::async_trait;
use beacon_data_lake::DATASETS_OBJECT_STORE_URL;
use beacon_datafusion_ext::table_ext::{
    MaterializedView, MaterializedViewDefinition, TableDefinition,
};
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions, sql::TableReference};

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

use super::materialized_view::{
    delete_datasets_prefix, now_millis, write_query_to_datasets_parquet,
};

pub(crate) struct RefreshMaterializedViewStatementHandler;

#[async_trait]
impl StatementHandler for RefreshMaterializedViewStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::RefreshMaterializedView
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        _sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_refresh_materialized_view()?;
        let session_ctx = context.session_ctx();
        let name = statement.view_name.to_string();
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

        // Recompute the query into a fresh versioned directory so the existing data
        // remains usable if this fails before the catalog pointer is swapped.
        let df = session_ctx.sql(&old_definition.definition).await?;
        let dir_path = format!("__beacon__/{}/{}", name, uuid::Uuid::new_v4());
        let schema = write_query_to_datasets_parquet(&session_ctx, df, &dir_path).await?;
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

        // Atomically swap the catalog pointer (single table.json PUT) and the provider.
        session_ctx.register_table(table_ref, new_provider)?;

        // The pointer now references the new data; reclaim the old directory.
        delete_datasets_prefix(&session_ctx, &old_definition.storage_location).await;

        tracing::info!("Refreshed materialized view '{name}'");
        Ok(context.empty_record_batch_stream())
    }
}
