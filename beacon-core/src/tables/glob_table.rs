use std::sync::Arc;

use beacon_sources::DataSource;
use datafusion::{datasource::listing::ListingTableUrl, execution::SessionState};

use super::table::BeaconTable;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlobTable {
    table_name: String,
    glob: String,
    file_format: FileFormat,
}

#[typetag::serde]
#[async_trait::async_trait]
impl BeaconTable for GlobTable {
    async fn as_table(
        &self,
        session_state: Arc<SessionState>,
    ) -> Arc<dyn datafusion::catalog::TableProvider> {
        let table_url = ListingTableUrl::parse(format!(
            "/{}/{}",
            beacon_config::DATASETS_DIR_PREFIX.to_string(),
            self.glob
        ))
        .unwrap();
        let source = DataSource::new(
            session_state.as_ref(),
            self.file_format.file_format(),
            vec![table_url],
        )
        .await
        .unwrap();

        Arc::new(source)
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum FileFormat {
    ArrowIpc,
    Parquet,
    CSV { delimiter: u8, infer_records: usize },
    NetCDF,
}

impl FileFormat {
    pub fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        match self {
            FileFormat::ArrowIpc => Arc::new(beacon_sources::arrow_format::SuperArrowFormat::new()),
            FileFormat::Parquet => {
                Arc::new(beacon_sources::parquet_format::SuperParquetFormat::new())
            }
            FileFormat::CSV {
                delimiter,
                infer_records,
            } => Arc::new(beacon_sources::csv_format::SuperCsvFormat::new(
                *delimiter,
                *infer_records,
            )),
            FileFormat::NetCDF => Arc::new(beacon_sources::netcdf_format::NetCDFFormat::new()),
        }
    }
}
