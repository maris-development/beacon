mod create_materialized_view;
mod df_statement;
mod materialized_view;
mod refresh;

use std::sync::Arc;

use create_materialized_view::CreateMaterializedViewStatementHandler;
use df_statement::DFStatementHandler;
use refresh::RefreshStatementHandler;

use crate::statement_handlers::registry::StatementRegistry;

pub(crate) fn register_default_statement_handlers(registry: &mut StatementRegistry) {
    registry.register_handler(Arc::new(DFStatementHandler));
    registry.register_handler(Arc::new(CreateMaterializedViewStatementHandler));
    registry.register_handler(Arc::new(RefreshStatementHandler));
}
