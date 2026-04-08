use std::sync::Arc;

use beacon_data_lake::FileManager;
use datafusion::{
    execution::SendableRecordBatchStream,
    prelude::{SQLOptions, SessionContext},
};

use crate::{
    parser::statement::BeaconStatement,
    statement_handlers::{
        context::HandlerContext,
        handlers::register_default_statement_handlers,
        loaders::register_default_ingest_loaders,
        payload::StatementPayload,
        registry::{IngestFormatLoaderRegistry, StatementRegistry},
    },
};

pub(crate) struct SqlStatementExecutor {
    context: HandlerContext,
    statement_registry: StatementRegistry,
}

impl SqlStatementExecutor {
    pub(crate) fn new(session_ctx: Arc<SessionContext>, file_manager: Arc<FileManager>) -> Self {
        let mut loader_registry = IngestFormatLoaderRegistry::new();
        register_default_ingest_loaders(&mut loader_registry);

        let mut statement_registry = StatementRegistry::new();
        register_default_statement_handlers(&mut statement_registry);

        Self {
            context: HandlerContext::new(session_ctx, file_manager, loader_registry),
            statement_registry,
        }
    }

    pub(crate) async fn execute(
        &self,
        statement: BeaconStatement,
        sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let payload: StatementPayload = statement.into();
        let handler = self.statement_registry.get_handler(payload.kind())?;
        let stream = handler.execute(payload, &self.context, sql_options).await?;

        Ok(stream)
    }
}
