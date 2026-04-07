use async_trait::async_trait;
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions};

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

pub(crate) struct AlterAtlasStatementHandler;

#[async_trait]
impl StatementHandler for AlterAtlasStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::AlterAtlas
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        _sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_alter_atlas()?;
        let table = context.resolve_table_provider(&statement.table_name).await?;
        let _atlas_table = context.as_atlas_table(table.as_ref())?;

        todo!()
    }
}