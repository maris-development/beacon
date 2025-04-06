use std::sync::Arc;

use beacon_sources::netcdf_format::NetCDFFormat;

#[async_trait::async_trait]
#[typetag::serde(tag = "file_format")]
pub trait FileFormat: std::fmt::Debug + Send + Sync {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat>;
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct NetCDFFileFormat;

#[typetag::serde(name = "netcdf")]
impl FileFormat for NetCDFFileFormat {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        Arc::new(NetCDFFormat::new())
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ArrowIpcFileFormat;

#[typetag::serde(name = "arrow_ipc")]
impl FileFormat for ArrowIpcFileFormat {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        Arc::new(beacon_sources::arrow_format::SuperArrowFormat::new())
    }
}
#[derive(Debug, serde::Deserialize, serde::Serialize)]

pub struct ParquetFileFormat;

#[typetag::serde(name = "parquet")]
impl FileFormat for ParquetFileFormat {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        Arc::new(beacon_sources::parquet_format::SuperParquetFormat::new())
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CSVFileFormat {
    pub delimiter: u8,
    pub infer_records: usize,
}
#[typetag::serde(name = "csv")]
impl FileFormat for CSVFileFormat {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        Arc::new(beacon_sources::csv_format::SuperCsvFormat::new(
            self.delimiter,
            self.infer_records,
        ))
    }
}
