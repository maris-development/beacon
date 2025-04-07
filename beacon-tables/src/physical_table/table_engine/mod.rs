use std::{path::PathBuf, sync::Arc};

use datafusion::{
    catalog::TableProvider,
    prelude::{DataFrame, SessionContext},
};
use error::TableEngineError;

pub mod error;
pub mod parquet;
#[async_trait::async_trait]
#[typetag::serde]
pub trait TableEngine: std::fmt::Debug + Send + Sync {
    async fn create(
        &self,
        table_directory: PathBuf,
        dataframe: DataFrame,
    ) -> Result<(), TableEngineError>;

    async fn insert(
        &self,
        table_directory: PathBuf,
        dataframe: DataFrame,
    ) -> Result<(), TableEngineError> {
        unimplemented!()
    }
    async fn delete(
        &self,
        table_directory: PathBuf,
        dataframe: DataFrame,
    ) -> Result<(), TableEngineError> {
        unimplemented!()
    }

    async fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableEngineError>;
}
