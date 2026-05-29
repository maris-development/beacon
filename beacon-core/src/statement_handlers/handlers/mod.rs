mod alter_atlas;
mod create_atlas_table;
mod create_materialized_view;
mod delete_atlas_datasets;
mod df_statement;
mod ingest;
mod materialized_view;
mod refresh_materialized_view;

use std::sync::Arc;

use alter_atlas::AlterAtlasStatementHandler;
use create_atlas_table::CreateAtlasTableStatementHandler;
use create_materialized_view::CreateMaterializedViewStatementHandler;
use delete_atlas_datasets::DeleteAtlasDatasetsStatementHandler;
use df_statement::DFStatementHandler;
use ingest::IngestStatementHandler;
use refresh_materialized_view::RefreshMaterializedViewStatementHandler;

use crate::statement_handlers::registry::StatementRegistry;

pub(crate) fn register_default_statement_handlers(registry: &mut StatementRegistry) {
    registry.register_handler(Arc::new(DFStatementHandler));
    registry.register_handler(Arc::new(CreateMaterializedViewStatementHandler));
    registry.register_handler(Arc::new(RefreshMaterializedViewStatementHandler));
    // ToDo: Re-enable when the handlers are implemented
    // registry.register_handler(Arc::new(IngestStatementHandler));
    // registry.register_handler(Arc::new(DeleteAtlasDatasetsStatementHandler));
    // registry.register_handler(Arc::new(CreateAtlasTableStatementHandler));
    // registry.register_handler(Arc::new(AlterAtlasStatementHandler));
}
