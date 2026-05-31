use async_trait::async_trait;
use beacon_datafusion_ext::table_ext::{ExternalTable, MaterializedView};
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions};

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

use super::materialized_view::refresh_materialized_view;

pub(crate) struct RefreshStatementHandler;

#[async_trait]
impl StatementHandler for RefreshStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::Refresh
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        _sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_refresh()?;
        let name = statement.name.to_string();

        let provider = context
            .resolve_table_provider(&statement.name)
            .await
            .map_err(|_| anyhow::anyhow!("REFRESH target '{name}' does not exist"))?;

        if let Some(external) = provider.as_any().downcast_ref::<ExternalTable>() {
            external.refresh().await?;
        } else if provider.as_any().is::<MaterializedView>() {
            refresh_materialized_view(&context.session_ctx(), &statement.name).await?;
        } else {
            return Err(anyhow::anyhow!(
                "'{name}' is not a materialized view or external table"
            ));
        }

        Ok(context.empty_record_batch_stream())
    }
}
