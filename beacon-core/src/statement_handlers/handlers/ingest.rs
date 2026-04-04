use async_trait::async_trait;
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions};

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

pub(crate) struct IngestStatementHandler;

#[async_trait]
impl StatementHandler for IngestStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::Ingest
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        _sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_ingest()?;
        let table = context.resolve_table_provider(&statement.table_name).await?;
        let atlas_table = context.as_atlas_table(table.as_ref())?;

        let loader = context.ingest_loader(&statement.format).ok_or_else(|| {
            anyhow::anyhow!(
                "Unsupported format '{}' in INGEST statement",
                statement.format
            )
        })?;

        let ingestion = loader.load(context, &statement.glob_pattern).await?;

        atlas_table
            .ingest_into_partition(
                context.session_ctx(),
                &context.data_lake().data_object_store_url(),
                &statement.partition_name,
                ingestion,
                false,
            )
            .await
    }
}