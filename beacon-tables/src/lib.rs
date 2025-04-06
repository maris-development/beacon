use std::{path::PathBuf, sync::Arc};

use datafusion::{catalog::TableProvider, execution::SessionState, prelude::SessionContext};
use logical_table::LogicalTableError;

pub mod empty_table;
pub mod error;
pub mod file_format;
pub mod logical_table;
pub mod schema_provider;
pub mod table;

#[async_trait::async_trait]
#[typetag::serde]
pub trait LogicalTableProvider: std::fmt::Debug + Send + Sync {
    fn table_name(&self) -> &str;
    async fn create(
        &self,
        directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), LogicalTableError>;
    async fn table_provider(
        &self,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, LogicalTableError>;
}
