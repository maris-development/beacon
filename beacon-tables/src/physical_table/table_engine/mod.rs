use std::{path::PathBuf, sync::Arc};

use datafusion::{
    catalog::TableProvider,
    prelude::{DataFrame, SessionContext},
};

pub mod parquet;

#[async_trait::async_trait]
#[typetag::serde]
pub trait TableEngine: std::fmt::Debug + Send + Sync {
    async fn create(&self, table_directory: PathBuf, dataframe: DataFrame) -> Result<(), String>;

    async fn insert(&self, table_directory: PathBuf, dataframe: DataFrame) -> Result<(), String> {
        return Err("Insert operation not supported".to_string());
    }
    async fn delete(&self, table_directory: PathBuf, dataframe: DataFrame) -> Result<(), String> {
        return Err("Delete operation not supported".to_string());
    }

    async fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, String>;
}
