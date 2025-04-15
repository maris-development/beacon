use std::{any::Any, path::PathBuf, sync::Arc};

use datafusion::{catalog::TableProvider, prelude::SessionContext};

pub mod geo_spatial;
pub mod geo_temporal;
pub mod temporal;

#[typetag::serde]
pub trait TableExtension: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn table_ext(&self) -> &str;

    fn table_name_with_ext(&self, table_name: &str) -> String {
        format!("{}__{}", table_name, self.table_ext())
    }
    fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
        origin_table_provider: Arc<dyn TableProvider>,
    ) -> TableExtensionResult<Arc<dyn TableProvider>>;
}

pub type TableExtensionError = Box<dyn std::error::Error + Send + Sync>;
pub type TableExtensionResult<T> = Result<T, TableExtensionError>;
