use async_trait::async_trait;
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions};

use crate::statement_handlers::{context::HandlerContext, payload::StatementPayload};

#[async_trait]
pub(crate) trait StatementHandler: Send + Sync {
    fn kind(&self) -> crate::statement_handlers::payload::StatementKind;

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream>;
}
