use std::sync::Arc;

use beacon_formats::{arrow::ArrowFormat, csv::CsvFormat, parquet::ParquetFormat};

#[typetag::serde(tag = "file_format")]
pub trait TableFileFormat: std::fmt::Debug + Send + Sync {
    fn file_ext(&self) -> String;
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ArrowIpcFileFormat;

#[typetag::serde(name = "arrow")]
impl TableFileFormat for ArrowIpcFileFormat {
    fn file_ext(&self) -> String {
        "arrow".to_string()
    }
}
#[derive(Debug, serde::Deserialize, serde::Serialize)]

pub struct ParquetFileFormat;

#[typetag::serde(name = "parquet")]
impl TableFileFormat for ParquetFileFormat {
    fn file_ext(&self) -> String {
        "parquet".to_string()
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CSVFileFormat {
    pub delimiter: u8,
    pub infer_records: usize,
}
#[typetag::serde(name = "csv")]
impl TableFileFormat for CSVFileFormat {
    fn file_ext(&self) -> String {
        "csv".to_string()
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct NetCDFFileFormat;

#[typetag::serde(name = "netcdf")]
impl TableFileFormat for NetCDFFileFormat {
    fn file_ext(&self) -> String {
        "nc".to_string()
    }
}
