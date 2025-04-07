use std::{path::PathBuf, sync::Arc};

use beacon_sources::{
    formats_factory::Formats, parquet_format::SuperParquetFormatFactory, DataSource,
};
use datafusion::{
    catalog::TableProvider,
    dataframe::DataFrameWriteOptions,
    datasource::{
        file_format::{parquet::ParquetFormatFactory, FileFormatFactory},
        listing::ListingTableUrl,
    },
    prelude::{DataFrame, SessionContext},
};

use super::{error::TableEngineError, TableEngine};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ParquetTable {}

impl ParquetTable {}

#[typetag::serde]
#[async_trait::async_trait]
impl TableEngine for ParquetTable {
    async fn create(
        &self,
        table_directory: PathBuf,
        dataframe: DataFrame,
    ) -> Result<(), TableEngineError> {
        // Implement the logic to create a Parquet table

        // Directory name
        let directory_name = table_directory
            .file_name()
            .ok_or(TableEngineError::InvalidPath)?;

        //Path if relative to the object store filesystem root
        let path = format!(
            "{}/{}/{}",
            beacon_config::TABLES_DIR_PREFIX.to_string(),
            directory_name.to_string_lossy(),
            "table.parquet"
        );

        dataframe
            .write_parquet(&path, DataFrameWriteOptions::new(), None)
            .await
            .map_err(|e| TableEngineError::DatafusionError(e))?;

        Ok(())
    }

    async fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableEngineError> {
        //Table url
        let table_url = ListingTableUrl::parse(&format!(
            "{}/{}/{}",
            beacon_config::TABLES_DIR_PREFIX.to_string(),
            table_directory.file_name().unwrap().to_string_lossy(),
            "table.parquet"
        ))?;

        let session_state = session_ctx.state();

        let file_format = SuperParquetFormatFactory.default();

        Ok(Arc::new(
            DataSource::new(&session_state, file_format, vec![table_url])
                .await
                .map_err(|e| TableEngineError::TableProviderError(e))?,
        ))
    }
}
