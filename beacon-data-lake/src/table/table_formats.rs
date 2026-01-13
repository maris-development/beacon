use std::{collections::HashMap, sync::Arc};

use beacon_binary_format::object_store::ArrowBBFObjectWriter;
use beacon_formats::zarr::{ZarrFormat, statistics::ZarrStatisticsSelection};
use datafusion::{
    datasource::{
        file_format::FileFormat,
        listing::{ListingTable, ListingTableUrl},
    },
    prelude::SessionContext,
};
use futures::StreamExt;

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

// Support for BBF File Format
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct BBFFileFormat;

#[typetag::serde(name = "bbf")]
#[async_trait::async_trait]
impl TableFileFormat for BBFFileFormat {
    fn file_ext(&self) -> String {
        "bbf".to_string()
    }

    async fn apply_operation(
        &self,
        op: serde_json::Value,
        urls: Vec<ListingTableUrl>,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Parse operation from serde_json::Value
        let operation: BBFOperation = serde_json::from_value(op)?;

        for url in urls {
            let object_store = session_ctx.runtime_env().object_store(url.object_store())?;
            let state = session_ctx.state();
            let mut objects = url.list_all_files(&state, &object_store, "").await?;

            while let Some(object_meta) = objects.next().await {
                // For each file, create an ArrowBBFObjectWriter and perform the operation
                let object_meta = object_meta?;
                let writer =
                    ArrowBBFObjectWriter::new(object_meta.location.clone(), object_store.clone());

                match &operation {
                    BBFOperation::DeleteEntries { entries } => {
                        // Here you would implement the logic to delete entries from the BBF file.
                        // This is a placeholder for demonstration purposes.
                        beacon_binary_format::ops::async_delete_entries(
                            Arc::new(writer),
                            entries.clone(),
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
pub enum BBFOperation {
    #[serde(rename = "delete_entries")]
    DeleteEntries { entries: Vec<String> },
}
