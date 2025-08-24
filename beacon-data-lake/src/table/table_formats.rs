use std::sync::Arc;

use beacon_formats::{arrow::ArrowFormat, csv::CsvFormat, parquet::ParquetFormat};

#[typetag::serde(tag = "file_format")]
pub trait TableFileFormat: std::fmt::Debug + Send + Sync {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat>;
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ArrowIpcFileFormat;

#[typetag::serde(name = "arrow")]
impl TableFileFormat for ArrowIpcFileFormat {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        Arc::new(ArrowFormat::new())
    }
}
#[derive(Debug, serde::Deserialize, serde::Serialize)]

pub struct ParquetFileFormat;

#[typetag::serde(name = "parquet")]
impl TableFileFormat for ParquetFileFormat {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        Arc::new(ParquetFormat::new())
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CSVFileFormat {
    pub delimiter: u8,
    pub infer_records: usize,
}
#[typetag::serde(name = "csv")]
impl TableFileFormat for CSVFileFormat {
    fn file_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        Arc::new(CsvFormat::new(self.delimiter, self.infer_records))
    }
}
