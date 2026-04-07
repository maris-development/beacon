use std::sync::Arc;

use async_trait::async_trait;
use beacon_atlas::datafusion::table::{AtlasTable, AtlasTableDefinition};
use datafusion::{
    error::DataFusionError,
    execution::{object_store::ObjectStoreUrl, SendableRecordBatchStream},
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::{SQLOptions, SessionContext},
    sql::TableReference,
};
use futures::StreamExt;

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

pub(crate) struct IngestStatementHandler;

async fn refresh_registered_atlas_table(
    session_ctx: Arc<SessionContext>,
    data_store_url: ObjectStoreUrl,
    table_name: String,
    definition: AtlasTableDefinition,
) -> anyhow::Result<()> {
    let refreshed_table =
        AtlasTable::from_definition(definition, session_ctx.clone(), &data_store_url).await?;

    let _ = session_ctx.deregister_table(TableReference::parse_str(&table_name))?;
    session_ctx.register_table(
        TableReference::parse_str(&table_name),
        Arc::new(refreshed_table),
    )?;

    Ok(())
}

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
        let table = context
            .resolve_table_provider(&statement.table_name)
            .await?;
        let atlas_table = context.as_atlas_table(table.as_ref())?;
        let data_store_url = context.data_object_store_url();

        let loader = context.ingest_loader(&statement.format).ok_or_else(|| {
            anyhow::anyhow!(
                "Unsupported format '{}' in INGEST statement",
                statement.format
            )
        })?;

        let definition = atlas_table.definition();
        let ingestion = loader.load(context, &statement.glob_pattern).await?;

        let ingestion_stream = definition
            .ingest_into_partition(
                context.session_ctx(),
                &data_store_url,
                &statement.partition_name,
                ingestion,
                false,
            )
            .await?;

        let stream_schema = ingestion_stream.schema();
        let refresh_table_name = statement.table_name.to_string();
        let refresh_partition_name = statement.partition_name.clone();
        let refresh_data_store_url = data_store_url.clone();
        let refresh_session_ctx = context.session_ctx();

        let stream = futures::stream::unfold(
            (
                ingestion_stream,
                Some((
                    refresh_session_ctx,
                    refresh_data_store_url,
                    refresh_table_name,
                    definition,
                    refresh_partition_name,
                )),
            ),
            |(mut ingestion_stream, mut refresh_state)| async move {
                if let Some(next_batch) = ingestion_stream.next().await {
                    return Some((next_batch, (ingestion_stream, refresh_state)));
                }

                if let Some((
                    refresh_session_ctx,
                    refresh_data_store_url,
                    refresh_table_name,
                    definition,
                    refresh_partition_name,
                )) = refresh_state.take()
                {
                    let refresh_result = refresh_registered_atlas_table(
                        refresh_session_ctx,
                        refresh_data_store_url,
                        refresh_table_name.clone(),
                        definition,
                    )
                    .await;

                    if let Err(error) = refresh_result {
                        let refresh_error = DataFusionError::Execution(format!(
                            "ingestion for partition '{}' completed but failed to refresh atlas table '{}': {}",
                            refresh_partition_name,
                            refresh_table_name,
                            error
                        ));

                        return Some((Err(refresh_error), (ingestion_stream, refresh_state)));
                    }
                }

                None
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}
