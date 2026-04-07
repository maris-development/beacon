use std::sync::Arc;

use async_trait::async_trait;
use beacon_atlas::datafusion::table::{AtlasTable, AtlasTableDefinition};
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions, sql::TableReference};

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

pub(crate) struct CreateAtlasTableStatementHandler;

#[async_trait]
impl StatementHandler for CreateAtlasTableStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::CreateAtlasTable
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        _sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_create_atlas_table()?;
        let data_store_url = context.data_object_store_url();

        let definition = AtlasTableDefinition::create(
            context.session_ctx(),
            &data_store_url,
            statement.table_name.to_string(),
            statement.location,
        )
        .await?;

        let table = AtlasTable::from_definition(
            definition,
            context.session_ctx(),
            &data_store_url,
        )
        .await?;

        context.session_ctx().register_table(
            TableReference::parse_str(&statement.table_name.to_string()),
            Arc::new(table),
        )?;

        Ok(context.empty_record_batch_stream())
    }
}
