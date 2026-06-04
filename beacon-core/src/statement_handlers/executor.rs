use std::sync::Arc;

use beacon_data_lake::FileManager;
use beacon_datafusion_ext::listing_table_factory_ext::ListingTableFactoryExt;
use datafusion::{
    execution::SendableRecordBatchStream,
    prelude::{SQLOptions, SessionContext},
};

use crate::{
    parser::statement::BeaconStatement,
    statement_handlers::{
        context::HandlerContext,
        handlers::register_default_statement_handlers,
        payload::StatementPayload,
        registry::StatementRegistry,
        stream_coalescer::coalesce_sql_stream,
    },
};

pub(crate) struct SqlStatementExecutor {
    context: Arc<HandlerContext>,
    statement_registry: StatementRegistry,
}

impl SqlStatementExecutor {
    pub(crate) fn new(session_ctx: Arc<SessionContext>, file_manager: Arc<FileManager>) -> Self {
        let mut statement_registry = StatementRegistry::new();
        let table_factory = Arc::new(ListingTableFactoryExt::new(
            file_manager.data_object_store_url(),
            Arc::downgrade(&session_ctx),
        ));
        let context = Arc::new(HandlerContext::new(
            session_ctx,
            file_manager,
            table_factory,
        ));

        register_default_statement_handlers(&mut statement_registry);

        Self {
            context,
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

        Ok(coalesce_sql_stream(stream))
    }
}
