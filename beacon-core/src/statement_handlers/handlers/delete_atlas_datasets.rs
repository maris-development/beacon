use async_trait::async_trait;
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions};

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

pub(crate) struct DeleteAtlasDatasetsStatementHandler;

#[async_trait]
impl StatementHandler for DeleteAtlasDatasetsStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::DeleteAtlasDatasets
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        _sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_delete_atlas_datasets()?;
        let table = context.resolve_table_provider(&statement.table_name).await?;
        let atlas_table = context.as_atlas_table(table.as_ref())?;

        atlas_table
            .delete_datasets_from_partition(
                context.session_ctx(),
                &context.data_lake().data_object_store_url(),
                &statement.partition_name,
                statement.dataset_names.unwrap_or_default(),
            )
            .await
    }
}