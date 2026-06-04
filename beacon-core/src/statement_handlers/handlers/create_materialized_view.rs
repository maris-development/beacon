use beacon_datafusion_ext::table_ext::{
    MaterializedViewDefinition, TableDefinition, internal_object_store_url,
};
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions, sql::TableReference};

use async_trait::async_trait;

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

use super::materialized_view::{now_millis, write_query_to_datasets_parquet};

pub(crate) struct CreateMaterializedViewStatementHandler;

#[async_trait]
impl StatementHandler for CreateMaterializedViewStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::CreateMaterializedView
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        _sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_create_materialized_view()?;
        let session_ctx = context.session_ctx();
        let name = statement.view_name.to_string();
        let table_ref = TableReference::parse_str(&name);

        if session_ctx.table_exist(table_ref.clone())? {
            return Err(anyhow::anyhow!(
                "Materialized view '{name}' already exists"
            ));
        }

        // Execute the defining query and persist its result as a single Parquet file.
        let df = session_ctx.sql(&statement.query_sql).await?;
        let dir_path = format!("{}/{}", name, uuid::Uuid::new_v4());
        let schema = write_query_to_datasets_parquet(&session_ctx, df, &dir_path).await?;
        let storage_location = format!("{dir_path}/");

        let now = now_millis();
        let definition = MaterializedViewDefinition {
            name: name.clone(),
            definition: statement.query_sql,
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
        Ok(context.empty_record_batch_stream())
    }
}
