mod alter_atlas;
mod create_atlas_table;
mod delete_atlas_datasets;
mod df_statement;
mod ingest;

use std::sync::Arc;

use alter_atlas::AlterAtlasStatementHandler;
use create_atlas_table::CreateAtlasTableStatementHandler;
use delete_atlas_datasets::DeleteAtlasDatasetsStatementHandler;
use df_statement::DFStatementHandler;
use ingest::IngestStatementHandler;

use crate::statement_handlers::registry::StatementRegistry;

pub(crate) fn register_default_statement_handlers(registry: &mut StatementRegistry) {
    registry.register_handler(Arc::new(DFStatementHandler));
    registry.register_handler(Arc::new(IngestStatementHandler));
    registry.register_handler(Arc::new(DeleteAtlasDatasetsStatementHandler));
    registry.register_handler(Arc::new(CreateAtlasTableStatementHandler));
    registry.register_handler(Arc::new(AlterAtlasStatementHandler));
}
