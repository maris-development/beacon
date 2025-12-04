use std::{collections::HashMap, sync::Arc};

use beacon_formats::zarr::{ZarrFormat, statistics::ZarrStatisticsSelection};
use datafusion::{
    datasource::{
        file_format::FileFormat,
        listing::{ListingTable, ListingTableUrl},
    },
    prelude::SessionContext,
};

#[typetag::serde(tag = "file_format")]
#[async_trait::async_trait]
pub trait TableFileFormat: std::fmt::Debug + Send + Sync {
    async fn apply_operation(
        &self,
        _op: serde_json::Value,
        _urls: Vec<ListingTableUrl>,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("No operations supported for this file format".into())
    }
    fn file_ext(&self) -> String;
    fn file_format(&self) -> Option<Arc<dyn FileFormat>> {
        None
    }
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

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ZarrFileFormat {
    #[serde(default)]
    pub statistics: Option<Arc<ZarrStatisticsSelection>>,
}

#[typetag::serde(name = "zarr")]
impl TableFileFormat for ZarrFileFormat {
    fn file_ext(&self) -> String {
        "zarr.json".to_string()
    }

    fn file_format(&self) -> Option<Arc<dyn FileFormat>> {
        Some(Arc::new(
            ZarrFormat::default().with_zarr_statistics(self.statistics.clone()),
        ))
    }
}
