use std::{path::PathBuf, sync::Arc};

use beacon_query::{parser::Parser, InnerQuery};
use datafusion::{
    catalog::TableProvider,
    prelude::{DataFrame, SessionContext},
};
use error::PhysicalTableError;
use table_engine::TableEngine;

use crate::error::TableError;

pub mod error;
pub mod table_engine;
#[async_trait::async_trait]
#[typetag::serde]
pub trait PhysicalTableProvider: std::fmt::Debug + Send + Sync {
    /// Creates a new table provider.
    ///
    /// # Arguments
    ///
    /// * `session_ctx` - A shared session context to be used during provider creation.
    ///
    /// # Errors
    ///
    /// Returns a `TableError` if there is an issue retrieving the table provider.
    async fn create(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), PhysicalTableError>;

    /// Returns the table provider.
    ///
    /// # Arguments
    ///     
    /// * `session_ctx` - A shared session context to be used during provider retrieval.
    ///
    /// # Errors
    ///
    /// Returns a `TableError` if there is an issue retrieving the table provider.
    async fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, PhysicalTableError>;
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct PhysicalTable {
    pub table_name: String,
    pub table_generation_query: InnerQuery,
    #[serde(flatten)]
    pub engine: Arc<dyn TableEngine>,
}

#[async_trait::async_trait]
#[typetag::serde(name = "physical_table")]
impl PhysicalTableProvider for PhysicalTable {
    async fn create(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), PhysicalTableError> {
        //Parse the query
        let logical_plan =
            Parser::parse_to_logical_plan(&session_ctx, self.table_generation_query.clone())
                .await
                .unwrap();

        let dataframe = DataFrame::new(session_ctx.state(), logical_plan);

        //Run the query against the engine
        self.engine
            .create(table_directory, dataframe)
            .await
            .unwrap();

        Ok(())
    }

    async fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, PhysicalTableError> {
        Ok(self
            .engine
            .table_provider(table_directory, session_ctx)
            .await
            .unwrap())
    }
}
