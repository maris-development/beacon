mod df_statement;

use std::sync::Arc;

use df_statement::DFStatementHandler;

use crate::statement_handlers::registry::StatementRegistry;

pub(crate) fn register_default_statement_handlers(registry: &mut StatementRegistry) {
    registry.register_handler(Arc::new(DFStatementHandler));
}
